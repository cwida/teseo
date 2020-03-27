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
#include "latch.hpp"
#include "scoped_epoch.hpp"
#include "thread_context.hpp"
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
        m_transaction_id(transaction_id), m_state(State::PENDING), m_undo_last(nullptr){
    m_undo_last = &m_undo_buffer;
    m_thread_context->register_transaction(this);
}

TransactionImpl::~TransactionImpl(){
    // clean up its state
    if(!is_terminated()){
        COUT_DEBUG("Transaction not terminated => Roll back!");
        rollback();
    }

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
    return m_transaction_id;
}

uint64_t TransactionImpl::ts_write() const {
    switch(m_state){
    case State::PENDING:
    case State::ERROR: // in error state, we consider the record still locked
        return m_transaction_id + (numeric_limits<uint64_t>::max()>>1);
    default:
        return m_transaction_id;
    }
}

bool TransactionImpl::is_terminated() const {
    return m_state == State::COMMITTED || m_state == State::ABORTED;
}

bool TransactionImpl::is_error() const {
    return m_state == State::ERROR;
}

bool TransactionImpl::owns(Undo* undo) const{
    return undo != nullptr && undo->transaction() == this;
}

bool TransactionImpl::can_write(Undo* undo) const {
    return undo == nullptr || owns(undo) || (ts_read() > undo->transaction()->ts_write());
}

bool TransactionImpl::can_read(const Undo* head, void** out_payload) const {
    *out_payload = nullptr;
    if(head == nullptr) return true;

    const TransactionImpl* owner = head->transaction();
    uint64_t my_id = ts_read();
    if(this == owner || owner->ts_write() < my_id) return true; // read the item stored in the storage

    const Undo* u = head->next();

    while(true){
        if(u == nullptr){
            return false; // false && out_payload == nullptr => this tx cannot see these changes
        } else if(my_id >= u->transaction_id()){
            *out_payload = u->payload();
            return false; // read *out_payload
        } else { // go on
            u = u->next();
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *   Commit & rollback                                                       *
 *                                                                           *
 *****************************************************************************/
void TransactionImpl::commit(){

    TransactionWriteLatch xlock(m_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");
    if(is_error()) RAISE_EXCEPTION(LogicalError, "The transaction must be rolled back as it's in an error state");

    uint64_t transaction_id = m_thread_context->global_context()->next_transaction_id();

    // Save the local changes
    if(m_prop_local){
        ScopedEpoch epoch; // must be inside an epoch
        thread_context()->save_local_changes(m_prop_local, transaction_id);
    }

    m_transaction_id = transaction_id;
    m_state = State::COMMITTED;
}


void TransactionImpl::rollback(){
    TransactionWriteLatch xlock(m_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");

    do_rollback();
}

void TransactionImpl::do_rollback(){
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

TransactionRollbackImpl::~TransactionRollbackImpl(){ }

/*****************************************************************************
 *                                                                           *
 *   Undo                                                                    *
 *                                                                           *
 *****************************************************************************/

Undo* TransactionImpl::add_undo(TransactionRollbackImpl* data_structure, Undo* next, uint32_t payload_length, void* payload) {
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
    Undo* undo = new (ptr) Undo(this, data_structure, next, payload_length);
    memcpy(undo + sizeof(Undo), payload, payload_length);

    incr_system_count();

    return undo;
}


/*****************************************************************************
 *                                                                           *
 *   Graph properties                                                        *
 *                                                                           *
 *****************************************************************************/

GraphProperty TransactionImpl::graph_properties() const{
    // m_prop_global_sync = 0 => global properties not computed yet
    //    ``       ``     = 1 => someone else is computing the properties
    //    ``       ``     = 2 => global properties already computed
    if(m_prop_global_sync != 2){
        uint64_t expected = 0;
        if(m_prop_global_sync.compare_exchange_weak(/* expected */ expected, /* value to set */ 1ull,
            /* memory order in case of success */ std::memory_order_release,
            /* memory order in case of failure */ std::memory_order_relaxed)
        ){
            m_prop_global = global_context()->property_snapshot(ts_read());
            m_prop_global_sync = 2; // done
        } else {
            // expected != 0 => it can either be 1 or 2
            // if 1 => active wait, someone else is fetching the global property list
            // if 2 => we're done
            while(expected != 2){
                this_thread::yield();
                expected = m_prop_global_sync;
            }
        }
    }

    return m_prop_global + m_prop_local;
}

GraphProperty& TransactionImpl::local_graph_changes(){
    return m_prop_local;
}

const GraphProperty& TransactionImpl::local_graph_changes() const {
    return m_prop_local;
}

//// Retrieve the number of vertices in the graph
//uint64_t TransactionImpl::num_vertices() const {
//    ScopedEpoch epoch;
//
//    ReadLatch slock(m_latch);
//    return graph_properties().m_vertex_count;
//}
//
//// Retrieve the number of edges in the graph
//uint64_t TransactionImpl::num_edges() const {
//    ScopedEpoch epoch;
//
//    ReadLatch slock(m_latch);
//    return graph_properties().m_edge_count;
//}

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
    case State::PENDING: cout << "PENDING"; break;
    case State::ERROR: cout << "ERROR"; break;
    case State::COMMITTED: cout << "COMMITTED"; break;
    case State::ABORTED: cout << "ABORTED"; break;
    }
    cout << ", system ref count: " << m_ref_count_system << ", user ref count: " << m_ref_count_user << "\n";

    UndoBuffer* undo_buffer = m_undo_last;
    while(undo_buffer != nullptr){
        uint64_t* buffer = reinterpret_cast<uint64_t*>(undo_buffer->m_buffer) + undo_buffer->m_space_left / 8;
        uint64_t buffer_sz = (UndoBuffer::BUFFER_SZ - undo_buffer->m_space_left) / 8;
        uint64_t i = 0;
        while(i < buffer_sz){
            auto undo =  reinterpret_cast<Undo*>(buffer + i);
            undo->dump();

            i += undo->length();
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

uint64_t TransactionList::high_water_mark() const {
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Need to be inside an epoch");

    do {
        uint64_t minimum = numeric_limits<uint64_t>::max();

        try {
            uint64_t version = m_latch.read_version();
            int64_t num_active_transactions = m_transactions_sz;
            for(int64_t i = 0; i < num_active_transactions; i++){
                TransactionImpl* tx = m_transactions[i];
                m_latch.validate_version(version);
                minimum = std::min(tx->ts_read(), minimum);
            }

            m_latch.validate_version(version);

            return minimum;
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

TransactionSequence::~TransactionSequence() {
    delete[] m_transaction_ids; m_transaction_ids = nullptr;
}

uint64_t TransactionSequence::size() const {
    return m_num_transactions;
}

uint64_t TransactionSequence::operator[](uint64_t index) const {
    assert(index < size());
    return m_transaction_ids[index];
}

/*****************************************************************************
 *                                                                           *
 *   TransactionSequenceForwardIterator                                      *
 *                                                                           *
 *****************************************************************************/

TransactionSequenceForwardIterator::TransactionSequenceForwardIterator(const TransactionSequence* sequence) : m_sequence(sequence) {
    assert(sequence != nullptr);
}

bool TransactionSequenceForwardIterator::done() const {
    return m_position < m_sequence->size();
}

uint64_t TransactionSequenceForwardIterator::key() const {
    return done() ? numeric_limits<uint64_t>::max() : m_sequence->operator [](m_position);
}

void TransactionSequenceForwardIterator::next() {
    m_position++;
}

/*****************************************************************************
 *                                                                           *
 *   TransactionSequenceBackwardsIterator                                    *
 *                                                                           *
 *****************************************************************************/

TransactionSequenceBackwardsIterator::TransactionSequenceBackwardsIterator(const TransactionSequence* sequence) : m_sequence(sequence){
    if(sequence->size() > 0){
        m_position = sequence->size() -1;
    }
}

bool TransactionSequenceBackwardsIterator::done() const {
    return m_position >= 0;
}

uint64_t TransactionSequenceBackwardsIterator::key() const {
    return done() ? numeric_limits<uint64_t>::min() : m_sequence->operator [](m_position);
}

void TransactionSequenceBackwardsIterator::next() {
    m_position--;
}

} // namespace


