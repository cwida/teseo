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
#include <fstream>
#include <queue>

#include "memstore/sparse_array.hpp"
#include "teseo/context/tctimer.hpp"
#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/profiler/event_global.hpp"
#include "teseo/profiler/rebal_global_list.hpp"
#include "teseo/profiler/save_to_disk.hpp"
#include "teseo/transaction/memory_pool.hpp"
#include "teseo/transaction/memory_pool_list.hpp"
#include "teseo/transaction/transaction_sequence.hpp"
#include "teseo/util/debug.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/thread.hpp"
#include "teseo/util/tournament_tree.hpp"

using namespace std;

namespace teseo::context {

static thread_local shared_ptr<ThreadContext> g_thread_context {nullptr};

/*****************************************************************************
 *                                                                           *
 *  Init                                                                     *
 *                                                                           *
 *****************************************************************************/
GlobalContext::GlobalContext() : m_garbage_collector( new GarbageCollector(this) ){
#if defined(HAVE_PROFILER)
    m_profiler = new profiler::EventGlobal();
    m_rebalances = new profiler::GlobalRebalanceList();
#endif

    // keep track of the global edge count / vertex count
    m_prop_list = new PropertySnapshotList();

    // start the TcTimer's service
    m_tctimer = new TcTimer();

    // init the transaction pool
    m_txn_pool_list = new transaction::MemoryPoolList();

    // because the storage appends a default key to the index, we first need to have
    // a thread context alive before initialising it
    register_thread();

    // instance to the storage
    if( g_debugging_test ){ // create a smaller sparse array, for testing purposes
        m_storage = new SparseArray(this, /* directed ? */ false, /* qwords per segment */ 32,  /* segments per gate */ 4, /* memory budget */ 4096);
    } else { // create a sparse array with the default parameters
        m_storage = new SparseArray(this, /* directed ? */ false);
    }
}

GlobalContext::~GlobalContext(){
    // temporary register a new thread context for cleaning purposes. Don't add it to the
    // list of active threads, as we are not going to perform any transaction
    register_thread(); // even if it's already present!
    m_storage->clear();
    unregister_thread(); // done

    COUT_DEBUG("Waiting for all thread contexts to terminate ...");
    while(m_tc_head != nullptr){
        this_thread::sleep_for(100ms); // check every 100 milliseconds
    }

    // stop the ThreadContext timer service
    delete m_tctimer; m_tctimer = nullptr;

    // stop the garbage collector
    delete m_garbage_collector; m_garbage_collector = nullptr;

    // from now on, the following structures DO NOT use the garbage collector in the dtor...

    // remove the storage
    delete m_storage; m_storage = nullptr; // must be done inside a thread context

    // clear the transaction pools
    delete m_txn_pool_list;

    // remove the `global' property list
    delete m_prop_list; m_prop_list = nullptr;

    // profiler data
#if defined(HAVE_PROFILER)
    profiler::save_to_disk(m_profiler, m_rebalances);
    delete m_profiler; m_profiler = nullptr;
    delete m_rebalances; m_rebalances = nullptr;;
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
    if(!g_thread_context)
        RAISE(LogicalError, "No context for this thread. Use the function Database::register_thread() to associate the thread to a given Database");
    return g_thread_context.get();
}

shared_ptr<ThreadContext> shptr_thread_context(){
    return g_thread_context;
}

GarbageCollector* GlobalContext::gc() const noexcept {
    return m_garbage_collector;
}

TcTimer* GlobalContext::tctimer() const noexcept {
    return m_tctimer;
}

profiler::EventGlobal* GlobalContext::profiler() {
    return m_profiler;
}

uint64_t GlobalContext::next_transaction_id() {
    return m_txn_global_counter++;
}

SparseArray* GlobalContext::storage() {
    return m_storage;
}

const SparseArray* GlobalContext::storage() const {
    return m_storage;
}


/*****************************************************************************
 *                                                                           *
 *  Register/unregister thread                                               *
 *                                                                           *
 *****************************************************************************/

void GlobalContext::register_thread(){
    g_thread_context.reset( new ThreadContext(this), GlobalContext::delete_thread_context );
    COUT_DEBUG("context: " << g_thread_context);

    // append the new context to the chain of existing contexts
    lock_guard<util::OptimisticLatch<0>> xlock(m_tc_latch);
    g_thread_context->m_next = m_tc_head;
    m_tc_head = g_thread_context.get();
}

void GlobalContext::unregister_thread() {
    g_thread_context.reset();
}

void GlobalContext::delete_thread_context(ThreadContext* tcntxt){
    assert(tcntxt != nullptr && "Null pointer");
    COUT_DEBUG("thread context: " << tcntxt);
    GlobalContext* gcntxt = tcntxt->global_context();

    // remove the current context in the chain of contexts
    bool done = false;
    ThreadContext* parent { nullptr };
    ThreadContext* current { nullptr };
    uint64_t version_parent {0}, version_current {0};
    do {

        try {
            parent = current = nullptr; // reinit

            assert(gcntxt->m_tc_head != nullptr && "At least the current thread_context must be in the linked list");
            version_parent = gcntxt->m_tc_latch.read_version();
            current = gcntxt->m_tc_head;
            gcntxt->m_tc_latch.validate_version(version_parent); // current is still valid as a ptr
            version_current = current->m_latch.read_version();
            gcntxt->m_tc_latch.validate_version(version_parent); // current was still the first item in m_tc_head when the version was read

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
            util::OptimisticLatch<0>& latch_parent = (parent == nullptr) ? gcntxt->m_tc_latch : parent->m_latch;
            latch_parent.update(version_parent);

            util::OptimisticLatch<0>& latch_current = current->m_latch;
            try {
                latch_current.update(version_current);
            } catch (util::Abort) {
                latch_parent.unlock();
                throw; // restart again
            }

            if(parent == nullptr){
                gcntxt->m_tc_head = tcntxt->m_next;
            } else {
                parent->m_next = tcntxt->m_next;
            }

            // save the local changes
            gcntxt->m_prop_list->acquire(gcntxt, tcntxt->m_prop_list);

            // remove the transaction pool
            gcntxt->m_txn_pool_list->release(tcntxt->m_tx_pool);
            tcntxt->m_tx_pool = nullptr;

            // save the data from the profiler
#if defined(HAVE_PROFILER)
            gcntxt->m_profiler->acquire(tcntxt->m_profiler);
            tcntxt->m_profiler = nullptr;
            gcntxt->m_rebalances->insert(tcntxt->rebalances());
#endif

            latch_parent.unlock();
            latch_current.invalidate(); // invalidate the current node

            done = true;
        } catch (util::Abort) { /* retry again */ }
    } while (!done);

    gcntxt->gc()->mark(tcntxt);
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

        } catch(util::Abort) { } /* retry */
    } while (!done);

    return epoch;
}


/*****************************************************************************
 *                                                                           *
 *  Active transactions                                                      *
 *                                                                           *
 *****************************************************************************/

TransactionSequence* GlobalContext::active_transactions(){
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

        } catch(util::Abort) { /* retry */ }
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

        } catch(util::Abort) { } /* retry */
    } while (true);
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
        } catch(util::Abort){ /* retry */ }

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

    gc()->dump();
}

} // namespace


