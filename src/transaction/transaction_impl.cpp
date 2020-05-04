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

#include "teseo/transaction/transaction_impl.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/memory_pool.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/transaction/undo_buffer.hpp"
#include "teseo/util/error.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::transaction {

/*****************************************************************************
 *                                                                           *
 *   Init                                                                    *
 *                                                                           *
 *****************************************************************************/
TransactionImpl::TransactionImpl(UndoBuffer* undo_buffer, context::GlobalContext* global_context, bool read_only) :
        m_global_context(global_context), // this can also be safely retrieved by context::global_context()
        m_transaction_id(-1), m_state(State::PENDING), m_undo_last(undo_buffer), m_read_only(read_only){

}

TransactionImpl::~TransactionImpl(){
    assert(m_undo_last->m_next == nullptr && "All additional buffers should have been already released by #release_undo_buffer()");
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

void TransactionImpl::set_transaction_id(uint64_t transaction_id){
    m_transaction_id = transaction_id;
}

uint64_t TransactionImpl::ts_read() const {
    return m_transaction_id;
}

uint64_t TransactionImpl::ts_write() const {
    switch(m_state){
    case State::PENDING:
        return m_transaction_id + (numeric_limits<uint64_t>::max()>>1);
    default:
        return m_transaction_id;
    }
}

bool TransactionImpl::is_terminated() const {
    return m_state == State::COMMITTED || m_state == State::ABORTED;
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
    if(this == owner || owner->ts_write() <= my_id) return true; // read the item stored in the storage

    const Undo* parent = head;
    const Undo* child = head->next();
    while(child != nullptr && my_id < child->transaction_id()){
        parent = child;
        child = child->next();
    }

    *out_payload = parent->payload();
    return false; // read the payload
}

template<typename OptimisticLatch>
bool TransactionImpl::can_read_optimistic(const Undo* head, void** out_payload, OptimisticLatch& latch, uint64_t version) const {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Must be inside an epoch");

    *out_payload = nullptr;
    if(head == nullptr) {
        latch.validate_version(version);
        return true;
    }

    const TransactionImpl* owner = head->transaction();
    latch.validate_version(version); // the pointer `owner' is safe

    uint64_t my_id = ts_read();
    if(this == owner || owner->ts_write() <= my_id){
        latch.validate_version(version);
        return true; // read the item stored in the storage
    }

    const Undo* parent = head;
    const Undo* child = head->next();
    latch.validate_version(version); // the pointer `child' is safe
    while(child != nullptr && my_id < child->transaction_id()){
        parent = child;
        child = child->next();
        latch.validate_version(version); // the pointer `child' is safe
    }

    *out_payload = parent->payload();
    latch.validate_version(version);
    return false; // read the payload
}

template
bool TransactionImpl::can_read_optimistic<util::OptimisticLatch<0>>(const Undo* undo, void** out_payload, util::OptimisticLatch<0>& latch, uint64_t version) const;
template
bool TransactionImpl::can_read_optimistic<util::OptimisticLatch<1>>(const Undo* undo, void** out_payload, util::OptimisticLatch<1>& latch, uint64_t version) const;
template
bool TransactionImpl::can_read_optimistic<util::OptimisticLatch<2>>(const Undo* undo, void** out_payload, util::OptimisticLatch<2>& latch, uint64_t version) const;
template
bool TransactionImpl::can_read_optimistic<util::OptimisticLatch<3>>(const Undo* undo, void** out_payload, util::OptimisticLatch<3>& latch, uint64_t version) const;
template
bool TransactionImpl::can_read_optimistic<util::OptimisticLatch<4>>(const Undo* undo, void** out_payload, util::OptimisticLatch<4>& latch, uint64_t version) const;

/*****************************************************************************
 *                                                                           *
 *   Commit & rollback                                                       *
 *                                                                           *
 *****************************************************************************/
void TransactionImpl::commit(){
    profiler::ScopedTimer profiler { profiler::TXN_COMMIT };

    TransactionWriteLatch xlock(m_latch);
    profiler::ScopedTimer prof_cs { profiler::TXN_COMMIT_CRITICAL_SECTION };
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");

    {
        profiler::ScopedTimer prof_unregister { profiler::TXN_COMMIT_UNREGISTER };
        unregister();
    }

    uint64_t transaction_id = m_global_context->next_transaction_id();

    // Save the local changes
    if(m_prop_local){
        context::ScopedEpoch epoch; // must be inside an epoch
        context::thread_context()->save_local_changes(m_prop_local, transaction_id);
    }

    m_transaction_id = transaction_id;
    m_state = State::COMMITTED;
}

void TransactionImpl::rollback(){
#if defined(HAVE_PROFILER) // the local thread context may have been released
    auto thread_context = context::thread_context_if_exists();
    profiler::ScopedTimer profiler { profiler::TXN_ROLLBACK, thread_context != nullptr ? thread_context->profiler_events() : nullptr };
#endif


    TransactionWriteLatch xlock(m_latch);
    if(is_terminated()) RAISE_EXCEPTION(LogicalError, "This transaction is already terminated");

    do_rollback();
    m_state = State::ABORTED;

    unregister();
}

void TransactionImpl::do_rollback(uint64_t N) {
    uint64_t i = 0;
    while(i < N){
        assert(m_undo_last->m_space_left <= m_undo_last->m_space_total);
        if(m_undo_last->m_space_left == m_undo_last->m_space_total){
            if(m_undo_last->m_next == nullptr) { break; } //  do not remove the embedded undo buffer

            UndoBuffer* temp = m_undo_last;
            m_undo_last = m_undo_last->m_next;
            gc_mark(temp, (void (*)(void*)) UndoBuffer::deallocate); // use the GC to support optimistic readers in the undo history
        } else {
            Undo* undo = reinterpret_cast<Undo*>(m_undo_last->buffer() + m_undo_last->m_space_left);
            if(undo->is_active()){ // ignore inactive undos
                undo->rollback();
                i++;
            }

            m_undo_last->m_space_left += undo->length();
        }
    }

    assert(N == numeric_limits<uint64_t>::max() || i == N); // ensure that N records have been restored
}


/*****************************************************************************
 *                                                                           *
 *   Undo                                                                    *
 *                                                                           *
 *****************************************************************************/

Undo* TransactionImpl::add_undo(RollbackInterface* data_structure, uint32_t payload_length, void* payload) {
    profiler::ScopedTimer profiler { profiler::TXN_ADD_UNDO };

    uint64_t total_length = sizeof(Undo) + payload_length;

    // first entry in the undo chain
    if(m_undo_last->m_space_left < total_length){
        UndoBuffer* temp = m_undo_last;
        m_undo_last = UndoBuffer::allocate(max<uint32_t>(total_length, context::StaticConfiguration::transaction_undo_buffer_size));
        m_undo_last->m_next = temp;
    }

    void* ptr = m_undo_last->buffer() + m_undo_last->m_space_left - total_length;
    m_undo_last->m_space_left -= total_length;

    // Init the undo record
    Undo* undo = new (ptr) Undo(this, data_structure, payload_length);
    memcpy((void*) (undo +1), payload, payload_length);

    return undo;
}

Undo* TransactionImpl::add_undo(RollbackInterface* data_structure, Undo* next, uint32_t payload_length, void* payload){
    Undo* undo = add_undo(data_structure, payload_length, payload);
    undo->set_active(next);
    return undo;
}

Undo* TransactionImpl::mark_last_undo(Undo* next) const {
    assert(m_undo_last->m_space_left < m_undo_last->m_space_total && "There is not a last undo");
    void* ptr = m_undo_last->buffer() + m_undo_last->m_space_left;
    Undo* undo = reinterpret_cast<Undo*>(ptr);
    undo->set_active(next);
    return undo;
}

/*****************************************************************************
 *                                                                           *
 *   Graph properties                                                        *
 *                                                                           *
 *****************************************************************************/

context::GraphProperty TransactionImpl::graph_properties() const{
    // m_prop_global_sync = 0 => global properties not computed yet
    //    ``       ``     = 1 => someone else is computing the properties
    //    ``       ``     = 2 => global properties already computed
    if(m_prop_global_sync != 2){
        uint64_t expected = 0;
        if(m_prop_global_sync.compare_exchange_weak(/* expected */ expected, /* value to set */ 1ull,
            /* memory order in case of success */ std::memory_order_release,
            /* memory order in case of failure */ std::memory_order_relaxed)
        ){
            m_prop_global = context::global_context()->property_snapshot(ts_read());
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

context::GraphProperty& TransactionImpl::local_graph_changes(){
    return m_prop_local;
}

const context::GraphProperty& TransactionImpl::local_graph_changes() const {
    return m_prop_local;
}

/*****************************************************************************
 *                                                                           *
 *   Garbage collection                                                      *
 *                                                                           *
 *****************************************************************************/

void TransactionImpl::mark_user_unreachable(){
    COUT_DEBUG("transaction: " << this);
    assert(m_shared == false || m_ref_count_user == 0);

    // clean up its state
    if(!is_terminated()){
        COUT_DEBUG("Transaction not terminated => Roll back!");
        rollback();
    }

    // the user count scores 1 point in the system count
    decr_system_count();
}

void TransactionImpl::mark_system_unreachable(){
    COUT_DEBUG("transaction: " << this);

    release_undo_buffers();

    // m_thread_context might or might not be deallocated, as it may have been implicitly deallocated by #mark_user_unreachable
    gc_mark(this, (void (*)(void*)) MemoryPool::destroy_transaction);
}

void TransactionImpl::gc_mark(void* pointer, void (*deleter)(void*)){
    // can we use the local thread context?
    context::ThreadContext* thread_context = context::thread_context_if_exists();
    if(thread_context != nullptr){
        thread_context->gc_mark(pointer, deleter);
    } else {
        /* fall back to the global context */
        m_global_context->gc()->mark(pointer, deleter);
    }
}

void TransactionImpl::release_undo_buffers(){
    assert(m_undo_last != nullptr && "The first buffer comes with the transaction pool and is always present");

    // release all undo buffers acquired, except the last one, as it belongs to the memory pool
    while(m_undo_last->m_next != nullptr){
        UndoBuffer* next = m_undo_last->m_next;
        // here always use the global GC
        gc_mark(m_undo_last, (void (*)(void*)) UndoBuffer::deallocate); // use the GC to support optimistic readers in the undo history
        m_undo_last = next;
    }
}

void TransactionImpl::unregister(){
    context::ThreadContext* tcntxt = context::thread_context_if_exists();
    bool success = tcntxt != nullptr && tcntxt->unregister_transaction(this);

    // if it's not in the local thread context, check all the other transaction lists
    if(!success){
        m_global_context->unregister_transaction(this);
    }
}

void TransactionImpl::incr_user_count(){
    m_shared = true;
    m_ref_count_user++;
}

void TransactionImpl::decr_user_count() {
    if(__builtin_expect(!m_shared, true) || --m_ref_count_user == 0){
        mark_user_unreachable();
    }
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
    case State::COMMITTED: cout << "COMMITTED"; break;
    case State::ABORTED: cout << "ABORTED"; break;
    }
    cout << ", system ref count: " << m_ref_count_system << ", user ref count: " << m_ref_count_user << ", shared: " << boolalpha << m_shared << "\n";

    UndoBuffer* undo_buffer = m_undo_last;
    while(undo_buffer != nullptr){
        uint8_t* buffer = undo_buffer->buffer() + undo_buffer->m_space_left;
        uint64_t buffer_sz = undo_buffer->m_space_total - undo_buffer->m_space_left;
        uint64_t i = 0;
        while(i < buffer_sz){
            auto undo =  reinterpret_cast<Undo*>(buffer + i);
            undo->dump();

            i += undo->length();
        }

        undo_buffer = undo_buffer->m_next;
    }
}


} // namespace
