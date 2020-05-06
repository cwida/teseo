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

#include "teseo/context/thread_context.hpp"

#include <iostream>
#include <memory>
#include <mutex>

#include "teseo/context/global_context.hpp"
#include "teseo/context/property_snapshot.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/profiler/event_thread.hpp"
#include "teseo/profiler/rebal_list.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/memory_pool.hpp"
#include "teseo/transaction/memory_pool_list.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/transaction_sequence.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::context {

/*****************************************************************************
 *                                                                           *
 *   Init                                                                    *
 *                                                                           *
 *****************************************************************************/


ThreadContext::ThreadContext(GlobalContext* global_context) : m_global_context(global_context), m_next(nullptr),
        m_ref_count(1), m_tx_seq(nullptr), m_tx_pool(nullptr), m_gc_queue(global_context->next_gc()),
        m_profiler_events(nullptr), m_profiler_rebalances{nullptr}
#if !defined(NDEBUG)
    , m_thread_id(util::Thread::get_thread_id())
#endif
{
    epoch_exit();

#if defined(HAVE_PROFILER)
    m_profiler_events = new profiler::EventThread();
    m_profiler_rebalances = new profiler::RebalanceList();
#endif

#if !defined(NDEBUG)
    COUT_DEBUG("thread_context: " << (void*) this << ", thread id: " << m_thread_id << ", started");
#endif
}

ThreadContext::~ThreadContext() {
    delete m_profiler_events; m_profiler_events = nullptr;
    delete m_profiler_rebalances; m_profiler_rebalances = nullptr;

#if !defined(NDEBUG)
    COUT_DEBUG("thread_context: " << (void*) this << ", thread id: " << m_thread_id << ", terminated");
#endif
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

const GlobalContext* ThreadContext::global_context() const noexcept {
    return m_global_context;
}

GlobalContext* ThreadContext::global_context() noexcept {
    return m_global_context;
}


/*****************************************************************************
 *                                                                           *
 *   Epoch                                                                   *
 *                                                                           *
 *****************************************************************************/

void ThreadContext::epoch_enter() {
    m_epoch = util::rdtscp();
}

void ThreadContext::epoch_exit() {
    m_epoch = numeric_limits<uint64_t>::max();
}

uint64_t ThreadContext::epoch() const {
    return m_epoch;
}

/*****************************************************************************
 *                                                                           *
 *   Transactions                                                            *
 *                                                                           *
 *****************************************************************************/
transaction::TransactionSequence* ThreadContext::all_active_transactions(){
    assert(epoch() != numeric_limits<uint64_t>::max() && "Must be inside an epoch");

    transaction::TransactionSequence* seq = m_tx_seq;

    // regenerate the list of the active transactions
    if(seq == nullptr){
        m_tx_seq = seq = global_context()->active_transactions();

        // automatically clear this cache in a bit of time
        global_context()->runtime()->schedule_reset_active_transactions();
    }

    return seq;
}

transaction::TransactionSequence* ThreadContext::reset_cache_active_transactions(){
    transaction::TransactionSequence* seq = m_tx_seq;
    m_tx_seq = nullptr;

    // we cannot use gc_mark here, because this method may be invoked by the TcTimer thread and not the
    // actual thread owning this thread context. Simply return the object to the caller and let it delete
    // properly.
    return seq;
}

transaction::TransactionImpl* ThreadContext::create_transaction(bool read_only){
    if(m_tx_pool == nullptr){ // first invocation
        m_tx_pool = m_global_context->transaction_pool()->exchange(nullptr);
    }

    transaction::TransactionImpl* tx = m_tx_pool->create_transaction(m_global_context, read_only);
    if(tx == nullptr){ // the thread pool is full
//        m_tx_pool->rebuild_free_list();
//        if(m_tx_pool->fill_factor() <= context::StaticConfiguration::transaction_memory_pool_ffreuse){
//            tx = m_tx_pool->create_transaction(m_global_context, read_only);
//        } else {
            m_tx_pool = m_global_context->transaction_pool()->exchange(m_tx_pool); // give away the old tx pool
            tx = m_tx_pool->create_transaction(m_global_context, read_only);
            assert(tx != nullptr && "We should have received from the global context a new memory pool with plenty of space");
//        }
    }

    uint64_t transaction_id = m_tx_list.insert(m_global_context, tx);
    tx->set_transaction_id(transaction_id);

    return tx;
}

/*****************************************************************************
 *                                                                           *
 *   Graph properties                                                        *
 *                                                                           *
 *****************************************************************************/
void ThreadContext::save_local_changes(GraphProperty& changes, uint64_t transaction_id){
    assert(epoch() != numeric_limits<uint64_t>::max() && "Must be inside an epoch");

    PropertySnapshot p;
    p.m_property = changes;
    p.m_transaction_id = transaction_id;
    m_prop_list.insert(p, all_active_transactions());
}

/*****************************************************************************
 *                                                                           *
 *   Garbage collector                                                       *
 *                                                                           *
 *****************************************************************************/

void ThreadContext::delete_transaction_sequence(void* pointer){
    delete reinterpret_cast<transaction::TransactionSequence*>(pointer);
}

void ThreadContext::incr_ref_count(){
    m_ref_count ++;
}

void ThreadContext::decr_ref_count(){
    if(--m_ref_count == 0){
        m_global_context->delete_thread_context(this);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void ThreadContext::dump() const {
#if !defined(NDEBUG)
    cout << "thread_id: " << m_thread_id << ", ";
#endif
    cout << "epoch: " << epoch();

    cout << "\n";
}

} // namespace


