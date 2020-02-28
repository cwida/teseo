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

#include "global_context.hpp"

#include "gc/epoch_garbage_collector.hpp"
#include "error.hpp"
#include "thread_context.hpp"
#include "utility.hpp"

using namespace teseo::internal::gc;
using namespace std;

namespace teseo::internal::context {

static thread_local ThreadContext* g_thread_context {nullptr};
std::mutex g_debugging_mutex; // to sync output messages to the stdout, for debugging purposes

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::lock_guard<mutex> lock(g_debugging_mutex); std::cout << "[GlobalContext::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *  Init                                                                     *
 *                                                                           *
 *****************************************************************************/
GlobalContext::GlobalContext() : m_garbage_collector( new EpochGarbageCollector(this) ){
    register_thread();
}

GlobalContext::~GlobalContext(){
    unregister_thread(); // if still alive

    // stop the garbage collector
    delete m_garbage_collector; m_garbage_collector = nullptr;
}


/*****************************************************************************
 *                                                                           *
 *  Properties                                                               *
 *                                                                           *
 *****************************************************************************/

GlobalContext* global_context() {
    return GlobalContext::thread_context()->global_context();
}

ThreadContext* GlobalContext::thread_context(){
    if(g_thread_context == nullptr)
        RAISE(LogicalError, "No context for this thread. Use the function Database::register_thread() to associate the thread to a given Database");
    return g_thread_context;
}

teseo::internal::gc::EpochGarbageCollector* GlobalContext::gc() const noexcept {
    return m_garbage_collector;
}

uint64_t GlobalContext::next_transaction_id() {
    return m_txn_global_counter++;
}

/*****************************************************************************
 *                                                                           *
 *  Register/unregister thread                                               *
 *                                                                           *
 *****************************************************************************/

void GlobalContext::register_thread(){
    // well, this should be a warning, if already registered
    if(g_thread_context != nullptr){ unregister_thread(); }
    g_thread_context = new ThreadContext(this);
    COUT_DEBUG("context: " << g_thread_context);

    // append the new context to the chain of existing contexts
    lock_guard<OptimisticLatch<0>> xlock(m_tc_latch);
    g_thread_context->m_next = m_tc_head;
    m_tc_head = g_thread_context;
}

void GlobalContext::unregister_thread(){
    if(g_thread_context == nullptr) return; // nop
    COUT_DEBUG("context: " << g_thread_context);

    // remove the current context in the chain of contexts
    bool done = false;
    ThreadContext* parent { nullptr };
    ThreadContext* current { nullptr };
    uint64_t version_parent {0}, version_current {0};
    do {

        try {
            parent = current = nullptr; // reinit
            g_thread_context->epoch_enter();

            assert(m_tc_head != nullptr && "At least the current thread_context must be in the linked list");
//            OptimisticLatch<0>* latch = &m_tc_latch;
            version_parent = m_tc_latch.read_version();
            current = m_tc_head;
            m_tc_latch.validate_version(version_parent); // current is still valid as a ptr
            version_current = current->m_latch.read_version();
            m_tc_latch.validate_version(version_parent); // current was still the first item in m_tc_head when the version was read

            // find m_thread_context in the list of contexts
            while(current != g_thread_context){
                parent = current;
                version_parent = version_current;

                current = current->m_next;
                assert(current != nullptr && "It cannot be a nullptr, because we haven't reached the current thread_context yet and it must be still present in the chain");

                parent->m_latch.validate_version(version_parent); // so far what we read from parent is good
                version_current = current->m_latch.read_version();
                parent->m_latch.validate_version(version_parent); // current is still the next node in the chain after parent
            }

            // acquire the xlock on the parent and the current node
            OptimisticLatch<0>& latch_parent = (parent == nullptr) ? m_tc_latch : parent->m_latch;
            latch_parent.update(version_parent);

            OptimisticLatch<0>& latch_current = current->m_latch;
            try {
                latch_current.update(version_current);
            } catch (Abort) {
                latch_parent.unlock();
                throw; // restart again
            }

            if(parent == nullptr){
                m_tc_head = g_thread_context->m_next;
            } else {
                parent->m_next = g_thread_context->m_next;
            }

            latch_parent.unlock();
            latch_current.invalidate(); // invalidate the current node

            done = true;
        } catch (Abort) { /* retry again */ }
    } while (!done);

    g_thread_context->epoch_exit();

    gc()->mark(g_thread_context);
    g_thread_context = nullptr;
}


/*****************************************************************************
 *                                                                           *
 *  Epochs                                                                   *
 *                                                                           *
 *****************************************************************************/
uint64_t GlobalContext::min_epoch() const {
    uint64_t epoch = 0;
    bool done = false;

    do {
        try {
            epoch = numeric_limits<uint64_t>::max(); // reinit

            OptimisticLatch<0>* latch = &m_tc_latch;
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


/*****************************************************************************
 *                                                                           *
 *  Dump                                                                     *
 *                                                                           *
 *****************************************************************************/
void GlobalContext::dump() const {
    cout << "[Local contexts]\n";
    ThreadContext* local = m_tc_head;
    cout << "0. (head): " << local << " => "; local->dump();
    int i = 0;
    while(local->m_next != nullptr){
        local = local->m_next;
        cout << i << ". : " << local << "=> "; local->dump();

        i++;
    }
    cout << "\n";

    gc()->dump();
}

} // namespace


