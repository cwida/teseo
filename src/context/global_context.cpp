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

#include "teseo/context/global_context.hpp"

#include <chrono>
#include <emmintrin.h>
#include <fstream>
#include <queue>

#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/event_global.hpp"
#include "teseo/profiler/rebal_global_list.hpp"
#include "teseo/profiler/save_to_disk.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/memory_pool_list.hpp"
#include "teseo/transaction/transaction_sequence.hpp"
#include "teseo/util/debug.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/thread.hpp"
#include "teseo/util/tournament_tree.hpp"

using namespace std;

namespace teseo::context {

static thread_local ThreadContext* g_thread_context {nullptr};

/*****************************************************************************
 *                                                                           *
 *  Init                                                                     *
 *                                                                           *
 *****************************************************************************/
GlobalContext::GlobalContext() {
#if defined(HAVE_PROFILER)
    m_profiler_events = new profiler::EventGlobal();
    m_profiler_rebalances = new profiler::GlobalRebalanceList();
#endif
    // start the background threads
    m_runtime = new runtime::Runtime(this);

    // keep track of the global edge count / vertex count
    m_prop_list = new PropertySnapshotList();

    m_runtime->register_thread_contexts();

    // because the storage appends a default key to the index, we first need to have
    // a thread context alive before initialising it
    register_thread();

    // memstore instance
    m_memstore = new memstore::Memstore(this, /* directed ? */ false);
}

GlobalContext::~GlobalContext(){
    m_runtime->unregister_thread_contexts();

    // temporary register a new thread context for cleaning purposes. Don't add it to the
    // list of active threads, as we are not going to perform any transaction
    register_thread(); // even if it's already present!
    m_memstore->clear();
    unregister_thread(); // done

    COUT_DEBUG("Waiting for all thread contexts to terminate ...");
    while(m_tc_head != nullptr){
        this_thread::sleep_for(100ms); // check every 100 milliseconds
    }

    // Stop the event loop for the asynchronous events
    m_runtime->stop_timer();

    // remove the storage
    delete m_memstore; m_memstore = nullptr; // must be done inside a thread context

    // remove the `global' property list
    delete m_prop_list; m_prop_list = nullptr;

    // remove the runtime
    delete m_runtime; m_runtime = nullptr;

    // profiler data
#if defined(HAVE_PROFILER)
    profiler::save_to_disk(m_profiler_events, m_profiler_rebalances);
    delete m_profiler_events; m_profiler_events = nullptr;
    delete m_profiler_rebalances; m_profiler_rebalances = nullptr;;
#endif

}


/*****************************************************************************
 *                                                                           *
 *  Properties                                                               *
 *                                                                           *
 *****************************************************************************/

GlobalContext* global_context() {
    return thread_context()->global_context();
}

ThreadContext* thread_context() {
    if(g_thread_context == nullptr)
        RAISE(LogicalError, "No context for this thread. Use the function Database::register_thread() to associate the thread to a given Database");
    return g_thread_context;
}

ThreadContext* thread_context_if_exists(){
    return g_thread_context;
}

gc::GarbageCollector* GlobalContext::gc() const noexcept {
    return runtime()->gc();
}

runtime::Runtime* GlobalContext::runtime() const noexcept {
    return m_runtime;
}

profiler::EventGlobal* GlobalContext::profiler_events() {
    return m_profiler_events;
}

uint64_t GlobalContext::next_transaction_id() {
    return m_txn_global_counter++;
}

memstore::Memstore* GlobalContext::memstore() {
    return m_memstore;
}

const memstore::Memstore* GlobalContext::memstore() const {
    return m_memstore;
}


transaction::MemoryPoolList* GlobalContext::transaction_pool(){
    return m_runtime->transaction_pool();
}

/*****************************************************************************
 *                                                                           *
 *  Register/unregister thread                                               *
 *                                                                           *
 *****************************************************************************/

void GlobalContext::register_thread(){
    unregister_thread(); // in case there is one already registered

    g_thread_context = new ThreadContext(this);
    COUT_DEBUG("context: " << g_thread_context);

    // append the new context to the chain of existing contexts
    lock_guard<util::OptimisticLatch<0>> xlock(m_tc_latch);
    g_thread_context->m_next = m_tc_head;
    m_tc_head = g_thread_context;
}

void GlobalContext::unregister_thread() {
    if(g_thread_context != nullptr){
        g_thread_context->decr_ref_count();
        g_thread_context = nullptr;
    }
}

void gc_delete_thread_context(void* pointer){
    delete reinterpret_cast<ThreadContext*>(pointer);
}

void GlobalContext::delete_thread_context(ThreadContext* tcntxt){
    assert(tcntxt != nullptr && "Null pointer");
    COUT_DEBUG("thread context: " << tcntxt);
    tcntxt->epoch_enter(); // protect from the GC

    // remove the current context in the chain of contexts
    bool done = false;
    ThreadContext* parent { nullptr };
    ThreadContext* current { nullptr };
    uint64_t version_parent {0}, version_current {0};
    do {
        try {
            parent = current = nullptr; // reinit

            assert(m_tc_head != nullptr && "At least the current thread_context must be in the linked list");
            version_parent = m_tc_latch.read_version();
            current = m_tc_head;
            m_tc_latch.validate_version(version_parent); // current is still valid as a ptr
            version_current = current->m_latch.read_version();
            m_tc_latch.validate_version(version_parent); // current was still the first item in m_tc_head when the version was read

            // find m_thread_context in the list of contexts
            while(current != tcntxt){
                parent = current;
                version_parent = version_current;

                current = current->m_next;
                assert(current != nullptr && "It cannot be a nullptr, because we haven't reached the current tcntxt yet and it must be still present in the chain");

                parent->m_latch.validate_version(version_parent); // so far what we read from parent is good
                version_current = current->m_latch.read_version();
                parent->m_latch.validate_version(version_parent); // current is still the next node in the chain after parent
            }

            // acquire the xlock on the parent and the current node
            util::OptimisticLatch<0>& latch_parent = (parent == nullptr) ? this->m_tc_latch : parent->m_latch;
            latch_parent.update(version_parent);

            util::OptimisticLatch<0>& latch_current = current->m_latch;
            try {
                latch_current.update(version_current);
            } catch (Abort) {
                latch_parent.unlock();
                throw; // restart again
            }

            if(parent == nullptr){
                m_tc_head = tcntxt->m_next;
            } else {
                parent->m_next = tcntxt->m_next;
            }

            // save the local changes
            m_prop_list->acquire(this, tcntxt->m_prop_list);

            // remove the transaction pool
            transaction_pool()->release(tcntxt->m_tx_pool);
            tcntxt->m_tx_pool = nullptr;

            // save the data from the profiler
#if defined(HAVE_PROFILER)
            m_profiler_events->acquire(tcntxt->m_profiler_events);
            tcntxt->m_profiler_events = nullptr;
            m_profiler_rebalances->insert(tcntxt->profiler_rebalances());
#endif

            tcntxt->m_gc_queue.release();

            latch_parent.unlock();
            latch_current.invalidate(); // invalidate the current node

            done = true;
        } catch (Abort) { /* retry again */ }
    } while (!done);

    tcntxt->epoch_exit(); // not really necessary, just for symmetry
    gc()->mark(tcntxt, gc_delete_thread_context);
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

            util::OptimisticLatch<0>* latch = &m_tc_latch;
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
 *  Active transactions                                                      *
 *                                                                           *
 *****************************************************************************/

transaction::TransactionSequence* GlobalContext::active_transactions(){
    using namespace teseo::transaction;

    // the thread must be inside an epoch to use this method
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max());
    uint64_t max_transaction_id; // the id of the next active transaction
    bool include_max_transaction_id; // shall we include the tx id of the next upcoming transaction in the list?

    // first, we need to retrieve the list of all thread contexts
    struct Queue {
        TransactionSequence m_sequence;
        uint64_t m_position = 0;
    };
    vector<Queue> queues;
    uint64_t num_transactions;

    bool done = false;
    do {
        queues.clear();
        num_transactions = 0;
        max_transaction_id = m_txn_global_counter;
        include_max_transaction_id = true;

        try {

            util::OptimisticLatch<0>* latch = &m_tc_latch;
            uint64_t version1 = latch->read_version();
            ThreadContext* child = m_tc_head;
            latch->validate_version(version1);
            if(child != nullptr) { // otherwise there are no registered contexts
                uint64_t version2 = child->m_latch.read_version();
                latch->validate_version(version1);
                version1 = version2;

                while(child != nullptr){
                    ThreadContext* parent = child;
                    TransactionSequence seq = parent->my_active_transactions();
                    if(seq.size() > 0){
                        include_max_transaction_id &= ( max_transaction_id > seq[0] );

                        num_transactions += seq.size();
                        queues.emplace_back( Queue{} );
                        queues.back().m_sequence = move(seq);
                    }

                    child = child->m_next;
                    if(child != nullptr)
                        version2 = child->m_latch.read_version();
                    parent->m_latch.validate_version(version1);

                    version1 = version2;
                }
            }

            done = true;

        } catch(Abort) { /* retry */ }
    } while (!done);

    // add to the list the transaction ID of the next upcoming transaction
    if(include_max_transaction_id){
        TransactionSequence seq { 1 };
        seq.m_transaction_ids[0] = max_transaction_id;
        queues.emplace_back( Queue{} );
        queues.back().m_sequence = move(seq);
        num_transactions++;
    }

    // second, merge the transaction lists together
    TransactionSequence* result = new TransactionSequence{ num_transactions };
    util::TournamentTree</* Transaction ID */ uint64_t, /* Queue ID */ uint64_t, /* Op */ greater<uint64_t>> tree { queues.size() };
    for(uint64_t i = 0; i < queues.size(); i++){
        if(queues[i].m_sequence.size() > 0){
            tree.set(i, queues[i].m_sequence[0], i);
            queues[i].m_position = 1;
        }
    }
    tree.rebuild();
    uint64_t position = 0;
    while(!tree.done()){
        auto item = tree.top();
        result->m_transaction_ids[position++] = item.first;
        auto& Q = queues[item.second];
        if(Q.m_position < Q.m_sequence.size()){
            tree.pop_and_replace(Q.m_sequence[Q.m_position]);
            Q.m_position++;
        } else {
            tree.pop_and_unset();
        }
    }

    return result;
}

uint64_t GlobalContext::high_water_mark() const {
    // the thread must be inside an epoch to use this method
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max());


    do {
        uint64_t minimum = numeric_limits<uint64_t>::max(); // reinit
        uint64_t next_transaction_id = m_txn_global_counter; // the id of the next active transaction

        try {
            util::OptimisticLatch<0>* latch = &m_tc_latch;
            uint64_t version1 = latch->read_version();
            ThreadContext* child = m_tc_head;
            latch->validate_version(version1);
            if(child == nullptr) return minimum; // there are no registered contexts
            uint64_t version2 = child->m_latch.read_version();
            latch->validate_version(version1);
            version1 = version2;

            while(child != nullptr){
                ThreadContext* parent = child;
                minimum = std::min(minimum, parent->my_high_water_mark());
                child = child->m_next;
                if(child != nullptr)
                    version2 = child->m_latch.read_version();
                parent->m_latch.validate_version(version1);

                version1 = version2;
            }

            // have we set a minimum ?
            if(minimum < numeric_limits<uint64_t>::max()){
                return minimum;
            } else {
                return next_transaction_id;
            }

        } catch(Abort) { } /* retry */
    } while (true);
}


void GlobalContext::unregister_transaction(transaction::TransactionImpl* transaction){
    COUT_DEBUG("transaction: " << transaction);
    bool success = false;

    util::OptimisticLatch<0>* latch_parent = &m_tc_latch;
    latch_parent->lock();
    ThreadContext* tcntxt = m_tc_head;
    while(tcntxt != nullptr && !success){
        util::OptimisticLatch<0>* latch_child = &(tcntxt->m_latch);
        latch_child->lock();
        latch_parent->unlock();

        success = tcntxt->unregister_transaction(transaction);

        // next iteration
        latch_parent = latch_child;
        tcntxt = tcntxt->m_next;
    }

    latch_parent->unlock();

    // it's fine if we were not able to remove the transaction; it means the thread context
    // is gone for good and the transaction does not appear anyway.
}

/*****************************************************************************
 *                                                                           *
 *  Graph properties                                                         *
 *                                                                           *
 *****************************************************************************/
GraphProperty GlobalContext::property_snapshot(uint64_t transaction_id) const {
    // the thread must be inside an epoch to use this method
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max());

    if(m_prop_list->size() >= /* magic number */ 8){
        m_prop_list->prune(high_water_mark());
    }

    do {
        try {
            // we use version_start and version_end to detect changes performed by a thread
            // context TC concurrently invoking #delete_thread_context:
            // if we see TC in the list, then there are two possible situation:
            // - either TC updates the global prop_list before we terminate => version_start != version_end => retry
            // - or TC updates the global prop_list after we terminate => that's okay, because we already accounted the changes of TC
            // if we don't see TC in the list, then:
            // - either TC terminated before we read version_start => that's okay, no conflict
            // - or TC terminated after we read version_start => then TC must have updated the global prop list before we terminate
            //   because it held the latch to its parent when it did so => version_start != version_end => retry

            uint64_t version_start = m_prop_list->version();
            GraphProperty result = m_prop_list->snapshot(transaction_id);

            // traverse the list of thread contexts
            util::OptimisticLatch<0>* latch = &m_tc_latch;
            uint64_t version1 = latch->read_version();
            ThreadContext* child = m_tc_head;
            latch->validate_version(version1);
            if(child != nullptr) { // otherwise there are no registered contexts
                uint64_t version2 = child->m_latch.read_version();
                latch->validate_version(version1);
                version1 = version2;

                while(child != nullptr){
                    ThreadContext* parent = child;
                    result += parent->my_local_changes(transaction_id);
                    child = child->m_next;
                    if(child != nullptr)
                        version2 = child->m_latch.read_version();
                    parent->m_latch.validate_version(version1);

                    version1 = version2;
                }
            }

            uint64_t version_end = m_prop_list->version();

            if ( version_start == version_end ) { // check the global property list did not change in the meanwhile
                return result;
            }
        } catch(Abort){ /* retry */ }

    } while(true);
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
    int i = 1;
    while(local->m_next != nullptr){
        local = local->m_next;
        cout << i << ". : " << local << "=> "; local->dump();

        i++;
    }
}

} // namespace


