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

#include "global_context.hpp"
#include "transaction.hpp"
#include "utility.hpp"

using namespace std;

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


ThreadContext::ThreadContext(GlobalContext* global_context) : m_global_context(global_context), m_next(nullptr)
#if !defined(NDEBUG)
    , m_thread_id(get_thread_id())
#endif
{
    epoch_exit();
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
 *   Transaction                                                             *
 *                                                                           *
 *****************************************************************************/
//Transaction* ThreadContext::transaction(){
//    auto ptr = m_transaction.get();
//    if(ptr == nullptr){ RAISE_EXCEPTION(LogicalError, "There is no active transaction in the current thread"); }
//    return ptr;
//}
//
//const Transaction* ThreadContext::transaction() const{
//    auto ptr = m_transaction.get();
//    if(ptr == nullptr){ RAISE_EXCEPTION(LogicalError, "There is no active transaction in the current thread"); }
//    return ptr;
//}
//
//Transaction* ThreadContext::txn_start(){
//    if(m_transaction.get() != nullptr && !m_transaction->is_terminated()){
//        RAISE_EXCEPTION(LogicalError, "There is already a pending transaction registered to the current thread");
//    }
//
//    auto deleter = [](Transaction* txn) { txn->mark_user_unreachable(); };
//    m_transaction.reset( new Transaction( global_context()->next_transaction_id() ), deleter );
//    return m_transaction.get();
//}
//
//void ThreadContext::txn_commit(){
//    m_transaction->commit();
//    m_transaction.reset();
//}
//
//void ThreadContext::txn_rollback(){
//    m_transaction->rollback();
//    m_transaction.reset();
//}


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
}

} // namespace


