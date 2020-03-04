/**
 * Copyright (C) 2019 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "transaction.hpp"


#include <cassert>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>

#include "gc/epoch_garbage_collector.hpp"
#include "global_context.hpp"
#include "thread_context.hpp"
#include "latch.hpp"
#include "utility.hpp"

using namespace std;

namespace teseo::internal::context {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[Transaction::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Init                                                                    *
 *                                                                           *
 *****************************************************************************/
Transaction::Transaction(uint64_t transaction_id) : m_transaction_id(transaction_id), m_num_undo_todo(0), m_state(State::PENDING), m_undo_last(nullptr){
    m_undo_last = &m_undo_buffer;
}

Transaction::~Transaction(){

    // release all undo buffers acquired
    UndoBuffer* buffer = m_undo_last;
    while(buffer != &m_undo_buffer){
        UndoBuffer* next = buffer->m_next;
        delete buffer;
        buffer = next;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

Transaction* transaction() {
    return thread_context()->transaction();
}

uint64_t Transaction::ts_read() const {
    return m_transaction_id;
}

uint64_t Transaction::ts_write() const {
    switch(m_state){
    case State::PENDING:
    case State::ERROR: // in error state, we consider the record still locked
        return m_transaction_id + (numeric_limits<uint64_t>::max()>>1);
    default:
        return m_transaction_id;
    }
}

Latch& Transaction::latch() {
    return m_latch;
}

bool Transaction::is_terminated() const {
    return m_state == State::COMMITTED || m_state == State::ABORTED;
}

bool Transaction::is_error() const {
    return m_state == State::ERROR;
}

bool Transaction::owns(Undo* undo) const{
    return undo != nullptr && undo->transaction() == this;
}

bool Transaction::can_write(Undo* undo) const {
    return undo == nullptr || owns(undo) || undo->transaction()->is_terminated();
}

/*****************************************************************************
 *                                                                           *
 *   Commit                                                                  *
 *                                                                           *
 *****************************************************************************/
void Transaction::commit(){
    WriteLatch xlock(m_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");
    if(is_error()) RAISE_EXCEPTION(LogicalError, "The transaction must be rolled back as it's in an error state");

    do_commit();
}

void Transaction::do_commit(){
    assert(m_latch.value() == -1 && "The tx latch should already have been acquired in exclusive mode");

    m_transaction_id = GlobalContext::global_context()->next_transaction_id();
    m_state = State::COMMITTED;
}

/*****************************************************************************
 *                                                                           *
 *   Undo                                                                    *
 *                                                                           *
 *****************************************************************************/

Undo* Transaction::add_undo(void* data_structure, Undo* next, UndoType type, uint32_t payload_length, void* payload) {
    uint64_t total_length = sizeof(Undo) + payload_length;
    assert(total_length <= UndoBuffer::BUFFER_SZ && "This entry won't fit any undo buffer");

    UndoBuffer* undo_buffer = m_undo_last;
    if(undo_buffer->m_space_left < total_length){
        undo_buffer = new UndoBuffer();
        undo_buffer->m_next = m_undo_last;
        m_undo_last = undo_buffer;
    }

    void* ptr = undo_buffer->m_buffer + undo_buffer->m_space_left - total_length;
    undo_buffer->m_space_left -= total_length;

    // Init the undo record
    Undo* undo = new (ptr) Undo(this, data_structure, next, type, payload_length);
    memcpy(undo + sizeof(Undo), payload, payload_length);

    m_num_undo_todo++;

    return undo;
}

void Transaction::tick_undo(){
    assert(m_latch.value() == -1 && "The write latch should have been acquired in exclusive mode");
    assert(m_num_undo_todo > 0 && "Underflow");

    m_num_undo_todo--;

    if(m_num_undo_todo == 0 && !m_user_reachable){
        global_context()->gc()->mark(this);
    }
}

void Transaction::rollback(){
    WriteLatch xlock(m_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");

    do_rollback();
}

void Transaction::do_rollback(){
    assert(m_latch.value() == -1 && "The tx latch should already have been acquired in exclusive mode");

    UndoBuffer* undo_buffer = m_undo_last;
    while(undo_buffer != nullptr){
        uint64_t* buffer = reinterpret_cast<uint64_t*>(undo_buffer->m_buffer) + undo_buffer->m_space_left / 8;
        uint64_t buffer_sz = (UndoBuffer::BUFFER_SZ - undo_buffer->m_space_left) / 8;
        uint64_t i = 0;
        while(i < buffer_sz){
            Undo* undo = reinterpret_cast<Undo*>(buffer + i);
            undo->rollback();

            i += undo->length(); // next undo entry
        }

        undo_buffer = undo_buffer->m_next;
    }

    m_state = State::ABORTED;
}

/*****************************************************************************
 *                                                                           *
 *   Garbage collection                                                      *
 *                                                                           *
 *****************************************************************************/

void Transaction::mark_user_unreachable(){
    WriteLatch xlock(m_latch);
    assert(m_user_reachable == true && "This method should be invoked only once");

    if(m_state == State::PENDING){
        do_commit(); // implicitly commit the transaction
    } else if (m_state == State::ERROR) {
        do_rollback(); // implicitly roll back the transaction
    }

    m_user_reachable = false;

    if(m_num_undo_todo <= 0){
        global_context()->gc()->mark(this);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void Transaction::dump() const {
    cout << "Transaction " << ts_read() << "/" << ts_write() << ", state: ";
    switch(m_state){
    case State::PENDING: cout << "RUNNING"; break;
    case State::ERROR: cout << "ERROR"; break;
    case State::COMMITTED: cout << "COMMITTED"; break;
    case State::ABORTED: cout << "ABORTED"; break;
    }
    cout << ", undo ref count: " << m_num_undo_todo << ", user reachable: " << boolalpha << m_user_reachable;

    UndoBuffer* undo_buffer = m_undo_last;
    while(undo_buffer != nullptr){
        uint64_t* buffer = reinterpret_cast<uint64_t*>(undo_buffer->m_buffer) + undo_buffer->m_space_left / 8;
        uint64_t buffer_sz = (UndoBuffer::BUFFER_SZ - undo_buffer->m_space_left) / 8;
        uint64_t i = 0;
        while(i < buffer_sz){
            cout << "\n";
            auto undo =  reinterpret_cast<Undo*>(buffer + i);
            undo->dump_chain();
        }

        undo_buffer = undo_buffer->m_next;
    }


}

} // namespace


