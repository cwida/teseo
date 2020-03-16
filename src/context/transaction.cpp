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

bool Transaction::is_terminated() const {
    bool done = false;
    State state;
    do {
        try {
            uint64_t version = m_undo_latch.read_version();
            state = m_state;
            m_undo_latch.validate_version(version);
            done = true;
        } catch (Abort) {
            if(m_undo_latch.is_invalid()){ return true; }
            // else, try again...
        }
    } while(!done);

    return state == State::COMMITTED || state == State::ABORTED;
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

bool Transaction::can_read(const Undo* head, void** out_payload) const {
    *out_payload = nullptr;
    if(head == nullptr) return true;

    const Transaction* owner = head->transaction();
    if(this == owner) return true;
    const uint64_t my_id = ts_read();

    do {
        try {
            uint64_t version0 = owner->m_undo_latch.read_version();
            uint64_t write_id = owner->ts_write();
            owner->m_undo_latch.validate_version(version0);
            if(write_id < my_id) return true; // read the item stored in the storage

            Undo* next = head->next();
            *out_payload = head->payload();
            owner->m_undo_latch.validate_version(version0); // check that next & out_payload are both valid

            while(next != nullptr){
                // lock coupling
                Transaction* tx_next = next->transaction();
                uint64_t version1 = tx_next->m_undo_latch.read_version();
                owner->m_undo_latch.validate_version(version0); // unlock current

                // move to the next undo in the chain
                Undo* current = next;
                owner = tx_next;
                version0 = version1;

                // check the transaction ID
                write_id = owner->ts_write();
                owner->m_undo_latch.validate_version(version0);
                if(write_id < my_id) return false; // read the payload from the previous item
                *out_payload = current->payload();
                next = current->next();
                owner->m_undo_latch.validate_version(version0); // check that next & out_payload are both valid
            }

            assert(*out_payload != nullptr);
            return false; // read the current value stored in out_payload
        } catch(Abort) { /* try again */ }
    } while(true);
}

/*****************************************************************************
 *                                                                           *
 *   Commit                                                                  *
 *                                                                           *
 *****************************************************************************/
void Transaction::commit(){
    WriteLatch xlock_user(m_transaction_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");
    if(is_error()) RAISE_EXCEPTION(LogicalError, "The transaction must be rolled back as it's in an error state");

    lock_guard<OptimisticLatch<0>> xlock_undo(m_undo_latch); // because the transaction ID and the state affect the undo records
    do_commit();
}

void Transaction::do_commit(){
    assert(m_undo_latch.is_locked() && "The undo latch should already have been acquired in exclusive mode");

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

    lock_guard<OptimisticLatch<0>> xlock(m_undo_latch);

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
    assert(m_undo_latch.is_locked() && "The write latch should have been acquired in exclusive mode");
    assert(m_num_undo_todo > 0 && "Underflow");

    m_num_undo_todo--;

    if(m_num_undo_todo == 0 && !m_user_reachable){
        global_context()->gc()->mark(this);
    }
}

void Transaction::rollback(){
    WriteLatch xlock_user(m_transaction_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");

    lock_guard<OptimisticLatch<0>> xlock_undo(m_undo_latch);
    do_rollback();
}

void Transaction::do_rollback(){
    assert(m_undo_latch.is_locked() && "The undo latch should already have been acquired in exclusive mode");

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
    m_undo_latch.lock();
    assert(m_user_reachable == true && "This method should be invoked only once");

    if(m_state == State::PENDING){
        do_commit(); // implicitly commit the transaction
    } else if (m_state == State::ERROR) {
        do_rollback(); // implicitly roll back the transaction
    }

    m_user_reachable = false;

    bool dealloc_txn = m_num_undo_todo <= 0;

    m_undo_latch.unlock();

    if(dealloc_txn){
        m_undo_latch.invalidate();
        m_transaction_latch.invalidate();
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


