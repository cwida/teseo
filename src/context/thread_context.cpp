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

#include "thread_context.hpp"

#include <iostream>
#include <memory>
#include <mutex>

#include "util/miscellaneous.hpp"
#include "garbage_collector.hpp"
#include "global_context.hpp"
#include "property_snapshot.hpp"
#include "tctimer.hpp"
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
#define COUT_DEBUG_FORCE(msg) { std::lock_guard<mutex> lock(g_debugging_mutex); std::cout << "[ThreadContext::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
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


ThreadContext::ThreadContext(GlobalContext* global_context) : m_global_context(global_context), m_next(nullptr), m_tx_seq(nullptr)
#if !defined(NDEBUG)
    , m_thread_id(get_thread_id())
#endif
{
    epoch_exit();
}

ThreadContext::~ThreadContext() {
#if !defined(NDEBUG)
    COUT_DEBUG("thread id: " << m_thread_id << ", terminated");
#endif
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                                   *
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
    m_epoch = rdtscp();
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
TransactionSequence* ThreadContext::all_active_transactions(){
    assert(epoch() != numeric_limits<uint64_t>::max() && "Must be inside an epoch");

    TransactionSequence* seq = m_tx_seq;

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
    TransactionSequence* seq = m_tx_seq;
    m_tx_seq = nullptr;
    global_context()->gc()->mark(seq);
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


