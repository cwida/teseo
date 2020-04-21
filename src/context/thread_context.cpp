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

#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/property_snapshot.hpp"
#include "teseo/context/tctimer.hpp"
#include "teseo/profiler/event_thread.hpp"
#include "teseo/profiler/rebal_list.hpp"
#include "teseo/transaction/memory_pool.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/transaction_sequence.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/debug.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/thread.hpp"

using namespace std;

namespace teseo::context {

/*****************************************************************************
 *                                                                           *
 *   Init                                                                    *
 *                                                                           *
 *****************************************************************************/


ThreadContext::ThreadContext(GlobalContext* global_context) : m_global_context(global_context), m_next(nullptr),
        m_tx_seq(nullptr), m_tx_pool(nullptr), m_profiler(nullptr), m_rebalances{nullptr}
#if !defined(NDEBUG)
    , m_thread_id(util::Thread::get_thread_id())
#endif
{
    epoch_exit();

#if defined(HAVE_PROFILER)
    m_profiler = new profiler::EventThread();
    m_rebalances = new profiler::RebalanceList();
#endif

#if !defined(NDEBUG)
    COUT_DEBUG("thread_context: " << (void*) this << ", thread id: " << m_thread_id << ", started");
#endif
}

ThreadContext::~ThreadContext() {
    delete m_profiler; m_profiler = nullptr;
    delete m_rebalances; m_rebalances = nullptr;


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
        shared_ptr<ThreadContext> shptr = shptr_thread_context();
        assert(shptr.get() == this && "This method should only be invoked by active thread contexts");
        global_context()->tctimer()->register_thread_context(shptr);
    }

    return seq;
}

void ThreadContext::reset_cache_active_transactions(){
    transaction::TransactionSequence* seq = m_tx_seq;
    m_tx_seq = nullptr;
    global_context()->gc()->mark(seq);
}

transaction::TransactionImpl* ThreadContext::create_transaction(bool read_only){
    auto tcptr = shptr_thread_context();
    auto instance = tcptr.get();
    if(instance == nullptr) { RAISE(LogicalError, "No thread context registered"); }
    return instance->create_transaction(tcptr, read_only);
}

transaction::TransactionImpl* ThreadContext::create_transaction(std::shared_ptr<ThreadContext> tctxt, bool read_only){
    transaction::TransactionImpl* tx = m_tx_pool->create_transaction(tctxt, read_only);
    if(tx == nullptr){ // the thread pool is full
        m_tx_pool = m_global_context->new_transaction_pool(m_tx_pool); // give away the old tx pool
        tx = m_tx_pool->create_transaction(tctxt, read_only);
        assert(tx != nullptr && "We should have received from the global context a new memory pool with plenty of space");
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
    m_prop_list.insert(p, /* it might be null */ m_tx_seq);
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


