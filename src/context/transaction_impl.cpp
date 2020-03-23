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

#include <cassert>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>


#include "util/miscellaneous.hpp"
#include "garbage_collector.hpp"
#include "global_context.hpp"
#include "thread_context.hpp"
#include "latch.hpp"
#include "transaction_impl.hpp"

using namespace std;
using namespace teseo::internal::util;

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
TransactionImpl::TransactionImpl(shared_ptr<ThreadContext> thread_context, uint64_t transaction_id) : m_thread_context(thread_context),
        m_start_time(transaction_id), m_commit_time(transaction_id + (numeric_limits<uint64_t>::max()>>1)), m_state(State::PENDING), m_undo_last(nullptr){
    m_undo_last = &m_undo_buffer;
    m_thread_context->register_transaction(this);
}

TransactionImpl::~TransactionImpl(){

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

uint64_t TransactionImpl::ts_read() const {
    if(((int)m_state) <= 1){
        return m_start_time;
    } else {
        return m_commit_time;
    }
}

uint64_t TransactionImpl::ts_write() const {
    return m_commit_time;
//    switch(m_state){
//    case State::PENDING:
//    case State::ERROR: // in error state, we consider the record still locked
//        return m_transaction_id + (numeric_limits<uint64_t>::max()>>1);
//    default:
//        return m_transaction_id;
//    }
}

bool TransactionImpl::is_terminated() const {
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

bool TransactionImpl::is_error() const {
    return m_state == State::ERROR;
}

bool TransactionImpl::owns(Undo* undo) const{
    return undo != nullptr && undo->transaction() == this;
}

bool TransactionImpl::can_write(Undo* undo) const {
    return undo == nullptr || owns(undo) || undo->transaction()->is_terminated();
}

bool TransactionImpl::can_read(const Undo* head, void** out_payload) const {
    *out_payload = nullptr;
    if(head == nullptr) return true;

    const TransactionImpl* owner = head->transaction();
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
                TransactionImpl* tx_next = next->transaction();
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
void TransactionImpl::commit(){
    WriteLatch xlock_user(m_transaction_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");
    if(is_error()) RAISE_EXCEPTION(LogicalError, "The transaction must be rolled back as it's in an error state");

    lock_guard<OptimisticLatch<0>> xlock_undo(m_undo_latch); // because the transaction ID and the state affect the undo records
    do_commit();
}

void TransactionImpl::do_commit(){
    assert(m_undo_latch.is_locked() && "The undo latch should already have been acquired in exclusive mode");

    m_commit_time = global_context()->next_transaction_id();
    m_state = State::COMMITTED;
}

/*****************************************************************************
 *                                                                           *
 *   Undo                                                                    *
 *                                                                           *
 *****************************************************************************/

Undo* TransactionImpl::add_undo(void* data_structure, Undo* next, UndoType type, uint32_t payload_length, void* payload) {
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

    incr_system_count();

    return undo;
}

void TransactionImpl::rollback(){
    WriteLatch xlock_user(m_transaction_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");

    lock_guard<OptimisticLatch<0>> xlock_undo(m_undo_latch);
    do_rollback();
}

void TransactionImpl::do_rollback(){
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

void TransactionImpl::mark_user_unreachable(){
    assert(m_ref_count_user == 0);
    m_thread_context->unregister_transaction(this);
}

void TransactionImpl::mark_system_unreachable(){
    m_thread_context->global_context()->gc()->mark(this);
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void TransactionImpl::dump() const {
    cout << "Transaction " << ts_read() << "/" << ts_write() << ", state: ";
    switch(m_state){
    case State::PENDING: cout << "RUNNING"; break;
    case State::ERROR: cout << "ERROR"; break;
    case State::COMMITTED: cout << "COMMITTED"; break;
    case State::ABORTED: cout << "ABORTED"; break;
    }
    cout << ", system ref count: " << m_ref_count_system << ", user ref count: " << m_ref_count_user;

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


/*****************************************************************************
 *                                                                           *
 *   TransactionList                                                         *
 *                                                                           *
 *****************************************************************************/

TransactionList::TransactionList() {
    memset(m_transactions, '\0', sizeof(m_transactions));
}

TransactionList::~TransactionList() {
    assert(m_transactions_sz == 0 && "There should not be any active transactions, otherwise they ref pointers will become dangling");
}

void TransactionList::insert(TransactionImpl* transaction) {
    m_latch.lock(); // xlock
    if(m_transactions_sz == m_transactions_capacity){
        m_latch.unlock();
        RAISE(LogicalError, "There are too many active transactions in this thread");
    } else {
        m_transactions[m_transactions_sz] = transaction;
        m_transactions_sz++;
        m_latch.unlock();
        transaction->incr_system_count();
    }
}

void TransactionList::remove(TransactionImpl* transaction){
    assert(transaction != nullptr && "Null pointer");

    m_latch.lock(); // xlock

    int64_t num_active_transactions = m_transactions_sz;
    int64_t i = 0;
    while(i < num_active_transactions && m_transactions[i] != transaction){ i ++; }

    if(i == num_active_transactions){
        m_latch.unlock();
        RAISE(InternalError, "Transaction not found in the active list: " << transaction);
    }

    // shift the remaining transactions back of one position
    num_active_transactions--;
    while(i < num_active_transactions){
        m_transactions[i] = m_transactions[i +1];
        i++;
    }

    assert(m_transactions_sz > 0 && "Underflow");
    m_transactions_sz--;

    m_latch.unlock();

    transaction->decr_system_count();
}

TransactionSequence TransactionList::snapshot() const {
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Need to be inside an epoch");

    do {
        try {
            uint64_t version = m_latch.read_version();
            int64_t num_active_transactions = m_transactions_sz;
            TransactionSequence seq ( num_active_transactions );
            for(int64_t i = 0; i < num_active_transactions; i++){
                TransactionImpl* tx = m_transactions[i];
                m_latch.validate_version(version);
                seq.m_transaction_ids[i] = tx->ts_read();
            }

            m_latch.validate_version(version);

            return seq;
        } catch (Abort){ /* retry */ }
    } while ( true );
}

/*****************************************************************************
 *                                                                           *
 *   TransactionSequence                                                     *
 *                                                                           *
 *****************************************************************************/
TransactionSequence::TransactionSequence(): m_num_transactions(0) { }

TransactionSequence::TransactionSequence(uint64_t num_transactions) : m_num_transactions(num_transactions){
    m_transaction_ids = new uint64_t[m_num_transactions]();
}

//TransactionSequence::TransactionSequence(const TransactionSequence& seq) : m_num_transactions(seq.size()) {
//    m_transactions = new TransactionImpl*[seq.size()]();
//    for(uint64_t i = 0; i < seq.size(); i++){
//        m_transactions[i] = seq.m_transactions[i];
//        if(m_transactions[i] != nullptr) m_transactions[i]->incr_system_count();
//    }
//}
//
//TransactionSequence& TransactionSequence::operator=(const TransactionSequence& seq) {
//    if(this != &seq){
//        clear();
//
//        m_num_transactions = seq.m_num_transactions;
//        m_transactions = new TransactionImpl*[m_num_transactions]();
//
//        for(uint64_t i = 0; i < seq.size(); i++){
//            m_transactions[i] = seq.m_transactions[i];
//            if(m_transactions[i] != nullptr) m_transactions[i]->incr_system_count();
//        }
//    }
//
//    return *this;
//}
//
//TransactionSequence::TransactionSequence(TransactionSequence&& seq) : m_transactions(seq.m_transactions), m_num_transactions(seq.m_num_transactions) {
//    seq.m_num_transactions = 0;
//    seq.m_transactions = nullptr;
//}
//
//TransactionSequence& TransactionSequence::operator=(TransactionSequence&& seq) {
//    if(this != &seq){
//        m_num_transactions = seq.m_num_transactions; seq.m_num_transactions = 0;
//        m_transactions = seq.m_transactions; seq.m_transactions = nullptr;
//    }
//
//    return *this;
//}

TransactionSequence::~TransactionSequence() {
    delete[] m_transaction_ids; m_transaction_ids = nullptr;
}

//void TransactionSequence::clear() {
//    for(uint64_t i = 0; i < m_num_transactions; i++){
//        if(m_transactions[i] != nullptr){
//            m_transactions[i]->decr_system_count();
//            m_transactions[i] = nullptr;
//        }
//    }
//
//    m_num_transactions = 0;
//    delete[] m_transactions; m_transactions = nullptr;
//}

uint64_t TransactionSequence::size() const {
    return m_num_transactions;
}

//TransactionImpl* TransactionSequence::operator[](uint64_t index){
//    assert(index < size());
//    return m_transactions[index];
//}
//
//const TransactionImpl* TransactionSequence::operator[](uint64_t index) const {
//    assert(index < size());
//    return m_transactions[index];
//}

uint64_t TransactionSequence::operator[](uint64_t index) const {
    assert(index < size());
    return m_transaction_ids[index];
}

} // namespace


