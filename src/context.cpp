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

#include "context.hpp"

#include <cinttypes>
#include <mutex>

#include "utility.hpp"

using namespace std;

namespace teseo::internal {

static thread_local ThreadContext* g_thread_context {nullptr};
std::mutex g_debugging_mutex; // to sync output messages to the stdout, for debugging purposes


/*****************************************************************************
 *                                                                           *
 *  GlobalContext                                                            *
 *                                                                           *
 *****************************************************************************/

GlobalContext::GlobalContext() : m_garbage_collector( this ){
    register_thread();
}


GlobalContext::~GlobalContext(){
    unregister_thread(); // if still alive

    // stop the garbage collector
    delete m_garbage_collector; m_garbage_collector = nullptr;
}

void GlobalContext::register_thread(){
    // well, this should be a warning, if already registered
    if(g_thread_context != nullptr){ unregister_thread(); }
    ThreadContext* local_context = g_thread_context = new ThreadContext(this);

    // append the new context to the chain of existing contexts
    lock_guard<OptimisticLatch<0>> xlock(m_tc_latch);
    g_thread_context->m_next = m_tc_head;
    m_tc_head = g_thread_context;
}


void GlobalContext::unregister_thread(){
    if(g_thread_context == nullptr) return; // nop

    // remove the current context in the chain of contexts
    bool done = false;
    ThreadContext* parent { nullptr };
    ThreadContext* current { nullptr };
    uint64_t version_parent {0}, version_current {0};
    do {

        try {
            parent = current = nullptr; // reinit
            g_thread_context->epoch_enter();

            OptimisticLatch<0>* latch = &m_tc_latch;
            version_current = latch->read_version();

            current = m_tc_head;

            // find m_thread_context in the list of contexts
            while(current != g_thread_context){
                parent = current;
                version_parent = version_current;

                current = current->m_next;
                assert(current != nullptr && "It cannot be nullptr, because all existing contexts must be present in the chain");
                OptimisticLatch<0>* latch_tmp = current->m_latch;

                latch->validate_version(version_parent); // so far what we read from parent is good
                latch = latch_tmp;
                version_current = latch->read_version();
            }

            // acquire the xlock on the parent and the current node
            OptimisticLatch<0>& latch_parent = (parent == nullptr) ? m_tc_head : parent->m_latch;
            lock_guard<OptimisticLatch<0>> xlock1 ( latch_parent );
            lock_guard<OptimisticLatch<0>> xlock2 ( current->m_latch );
            latch_parent.validate_version(version_parent);
            current->m_latch.validate_version(version_current);

            if(parent == nullptr){
                m_tc_head = g_thread_context->m_next;
            } else {
                parent->m_next = g_thread_context->m_next;
            }

            done = true;
        } catch (Abort) { /* retry again */ }
    } while (!done);

    g_thread_context->epoch_exit();

    delete g_thread_context; g_thread_context = nullptr;
}


uint64_t GlobalContext::min_epoch() const {
    uint64_t epoch = 0;
    bool done = false;

    do {
        try {
            epoch = numeric_limits<uint64_t>::max(); // reinit

            OptimisticLatch<0>* latch = m_tc_latch;
            uint64_t version1 = latch->read_version();
            ThreadContext* child = m_tc_head;
            latch->validate_version(version1);
            if(child == nullptr) return epoch; // there are no registered contexts
            uint64_t version2 = child->m_latch.read_version();
            latch->validate_version(version1);
            version1 = version2;

            while(child != nullptr){
                ThreadContext* parent = child;
                epoch = std::min(epoch, parent->epoch());
                child = child->m_next;
                if(child != nullptr)
                    version2 = child->m_latch.read_version();
                parent->m_latch.validate_version(version1);

                version1 = version2;
            }

            done = true;

        } catch(Abort) { } /* retry */
    } while (!done);


    return epoch;
}

GlobalContext* GlobalContext::context(){
    return ThreadContext::context()->global_context();
}

GarbageCollector* GlobalContext::gc() const noexcept {
    return m_garbage_collector;
}

/*****************************************************************************
 *                                                                           *
 *  ThreadContext                                                            *
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

GlobalContext* ThreadContext::global_context() const noexcept {
    return m_global_context;
}

ThreadContext* ThreadContext::context(){
    if(g_thread_context == nullptr)
        RAISE(LogicalError, "No context for this thread. Use the function Database::register_thread() to associate the thread to a given Database");
    return g_thread_context;
}

} // namespace
