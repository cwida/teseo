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

#include "teseo/aux/cache.hpp"
#include "teseo/aux/dynamic_view.hpp"
#include "teseo/aux/static_view.hpp"
#include "teseo/bp/buffer_pool.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/event_global.hpp"
#include "teseo/profiler/rebal_global_list.hpp"
#include "teseo/profiler/save_to_disk.hpp"
#include "teseo/rebalance/merger_service.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/memory_pool_list.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/transaction_sequence.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/numa.hpp"
#include "teseo/util/thread.hpp"
#include "teseo/util/tournament_tree.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::context {

static thread_local ThreadContext* g_thread_context {nullptr};

/*****************************************************************************
 *                                                                           *
 *  Init                                                                     *
 *                                                                           *
 *****************************************************************************/
GlobalContext::GlobalContext() : m_tc_list(this), m_aux_degree_enabled(StaticConfiguration::aux_degree_enabled) {
#if defined(HAVE_PROFILER)
    m_profiler_events = new profiler::EventGlobal();
    m_profiler_rebalances = new profiler::GlobalRebalanceList();
#endif

    // validate the settings for NUMA at runtime
    util::NUMA::check_numa_support();

    // start the background threads
    m_runtime = new runtime::Runtime(this);

    // support for huge pages
    if(context::StaticConfiguration::huge_pages){
        m_bufferpool = new bp::BufferPool();
        m_runtime->schedule_bp_pass();
    }

    // keep track of the global edge count / vertex count
    m_prop_list = new PropertySnapshotList();

    m_runtime->register_thread_contexts();

    // aux cache
    m_aux_cache = StaticConfiguration::aux_cache_enabled ? new aux::Cache() : nullptr;

    // because the storage appends a default key to the index, we first need to have
    // a thread context alive before initialising it
    register_thread();

    // memstore instance
    m_memstore = new memstore::Memstore(this, /* directed ? */ false);
}

GlobalContext::~GlobalContext(){
    m_memstore->merger()->stop(); // unsafe to run the merger as the GCs won't see its epoch anymore

    m_runtime->unregister_thread_contexts();
    // ... GC is at full throttle here (it doesn't respect the epochs) ...

    // temporary register a new thread context for cleaning purposes. Don't add it to the
    // list of active threads, as we are not going to perform any transaction
    register_thread(); // even if it's already present!
    m_memstore->clear();
    unregister_thread(); // done

    // wait for all thread contexts to terminate ZzZ....
    COUT_DEBUG("Waiting for all thread contexts to terminate ...");
    while(!m_tc_list.empty()){ // wait for all thread contexts to be removed by the GC & the async timer
        this_thread::sleep_for(context::StaticConfiguration::runtime_gc_frequency);
    }

    // Stop the event loop for the asynchronous events
    m_runtime->stop_timer();

    // remove the storage
    delete m_memstore; m_memstore = nullptr; // must be done inside a thread context

    // remove the `global' property list
    delete m_prop_list; m_prop_list = nullptr;

    // remove the auxiliary view cache
    delete m_aux_cache; m_aux_cache = nullptr;

    // remove the runtime
    delete m_runtime; m_runtime = nullptr;

    // remove the buffer pool
    delete m_bufferpool; m_bufferpool = nullptr;

    // profiler data
#if defined(HAVE_PROFILER)
    profiler::save_to_disk(m_profiler_events, m_profiler_rebalances);
    delete m_profiler_events; m_profiler_events = nullptr;
    delete m_profiler_rebalances; m_profiler_rebalances = nullptr;;
#endif

    COUT_DEBUG("done");
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

gc::GarbageCollector* GlobalContext::next_gc() const noexcept {
    return runtime()->next_gc();
}

runtime::Runtime* GlobalContext::runtime() const noexcept {
    return m_runtime;
}

bp::BufferPool* GlobalContext::bp() const noexcept {
    return m_bufferpool;
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
    m_tc_list.insert(g_thread_context);
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
    {
        scoped_lock<util::OptimisticLatch<0>> xlock(m_tc_list.m_latch);
        m_tc_list.remove(tcntxt);

        // ... do not release the latch yet ...
        // to guarantee that all properties are transferred to the global context before other threads
        // can access them.

        // save the local changes
        m_prop_list->acquire(this, tcntxt->m_prop_list);

        m_txn_highest_rw_id = max(m_txn_highest_rw_id, tcntxt->m_tx_list.highest_txn_rw_id_unsafe());
    } // terminate the scope of the latch

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

    gc()->mark(tcntxt, gc_delete_thread_context);
}

void gc_delete_thread_contexts_list(void* pointer){
    delete[] reinterpret_cast<ThreadContext**>(pointer);
}

/*****************************************************************************
 *                                                                           *
 *  Epochs                                                                   *
 *                                                                           *
 *****************************************************************************/
uint64_t GlobalContext::min_epoch() const {
    ThreadContext* my_thread_context = thread_context_if_exists();
    // if the GC doesn't have a thread context, then the runtime must have unregistered them, it means there are no thread contexts around
    if(my_thread_context == nullptr) return numeric_limits<uint64_t>::max();

    uint64_t epoch = 0;
    bool done = false;

    do {
        epoch = numeric_limits<uint64_t>::max(); // reinit

        // protect from the other GCs, that may clear a thread context while we're reading it
        my_thread_context->epoch_enter();

        try {

            uint64_t version = m_tc_list.read_version();
            ThreadContext** __restrict list = m_tc_list.list();
            ThreadContext* tcntxt = list[0]; int i = 0;
            while(tcntxt != nullptr){
                // we cannot use list[i] here anymore as the pointer may change, it could have become null
                epoch = std::min(epoch, tcntxt->epoch());

                // next iteration
                tcntxt = list[++i];
            }
            m_tc_list.validate_version(version);

            done = true;
        } catch(Abort) { /* retry */ }

    } while (!done);

    my_thread_context->epoch_exit();

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
        num_transactions = 1; // assume max_transaction_id is present
        max_transaction_id = m_txn_global_counter;

        try {
            uint64_t version = m_tc_list.read_version();
            ThreadContext** __restrict list = m_tc_list.list();
            ThreadContext* tcntxt = list[0]; int i = 0;
            while(tcntxt != nullptr){
                TransactionSequence seq = tcntxt->my_active_transactions(max_transaction_id);
                if(seq.size() > 0){
                    num_transactions += seq.size();
                    queues.emplace_back( Queue{} );
                    queues.back().m_sequence = move(seq);
                }

                // next iteration
                tcntxt = list[++i];
            }

            m_tc_list.validate_version(version);
            done = true;

        } catch(Abort) { /* retry */ }
    } while (!done);

    // final result
    TransactionSequence* result = new TransactionSequence{ num_transactions };
    result->m_transaction_ids[0] = max_transaction_id;
    if(num_transactions == 1) return result; // we're done

    // second, merge the transaction lists together
    util::TournamentTree</* Transaction ID */ uint64_t, /* Queue ID */ uint64_t, /* Op */ greater<uint64_t>> tree { queues.size() };
    for(uint64_t i = 0; i < queues.size(); i++){
        assert(queues[i].m_sequence.size() > 0 && "Empty queues are discarded when fetching the active transactions");
        tree.set(i, queues[i].m_sequence[0], i);
        queues[i].m_position = 1;
    }

    tree.rebuild();
    uint64_t position = 1; // as m_transaction_ids[0] = max_transaction_id
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

    assert(result->size() == num_transactions && "Not all transaction IDs have been extracted from the tournament tree");
    return result;
}

uint64_t GlobalContext::high_water_mark() const {
    // the thread must be inside an epoch to use this method
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max());


    do {
        uint64_t minimum = numeric_limits<uint64_t>::max(); // reinit
        uint64_t next_transaction_id = m_txn_global_counter; // the id of the next active transaction

        try {
            uint64_t version = m_tc_list.read_version();
            ThreadContext** __restrict list = m_tc_list.list();
            ThreadContext* tcntxt = list[0]; int i = 0;
            while(tcntxt != nullptr){
                minimum = std::min(minimum, tcntxt->my_high_water_mark());

                // next iteration
                tcntxt = list[++i];
            }
            m_tc_list.validate_version(version);

            // have we set a minimum ?
            if(minimum < numeric_limits<uint64_t>::max()){
                return minimum;
            } else {
                return next_transaction_id;
            }

        } catch(Abort) { } /* retry */
    } while (true);
}


uint64_t GlobalContext::highest_txn_rw_id() const {
    // the thread must be inside an epoch to use this method
    assert(thread_context()->epoch() != numeric_limits<uint64_t>::max());

    do {
        try {
            uint64_t version = m_tc_list.read_version();
            uint64_t highest_txn_id = m_txn_highest_rw_id;

            ThreadContext** __restrict list = m_tc_list.list();
            ThreadContext* tcntxt = list[0]; int i = 0;
            while(tcntxt != nullptr){
                highest_txn_id = std::max(highest_txn_id, tcntxt->my_highest_txn_rw_id());

                // next iteration
                tcntxt = list[++i];
            }
            m_tc_list.validate_version(version);

            return highest_txn_id;
        } catch(Abort) { } /* retry */
    } while (true);
}

void GlobalContext::unregister_transaction(transaction::TransactionImpl* transaction){
    COUT_DEBUG("transaction: " << transaction);
    bool success = false;

    lock_guard<util::OptimisticLatch<0>> xlock(m_tc_list.m_latch);
    uint32_t i = 0, sz = m_tc_list.m_size;
    ThreadContext** __restrict list = m_tc_list.list();
    while(i < sz && !success){
        ThreadContext* tcntxt = list[i]; // list[i] is fine here as we are in xlock
        success = tcntxt->unregister_transaction(transaction);
        i++;
    }

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
        // we use property_{version_start, version_end} to detect changes performed by a thread context TC that is
        // concurrently invoking #delete_thread_context.
        // If we see TC in the list, then there are two possible situation:
        // - either TC updates the global prop_list before we terminate => version_start != version_end => retry
        // - or TC updates the global prop_list after we terminate => that's okay, because we already accounted the changes of TC
        // If we don't see TC in the list, then:
        // - either TC terminated before we read version_start => that's okay, no conflict
        // - or TC terminated after we read version_start => then TC must have updated the global prop list before we terminate
        //   because it held the latch to its parent when it did so => version_start != version_end => retry
        //
        // 05/04/2020: the tc linked list has been replaced with a vector, but the logic above still holds.

        uint64_t property_version_start = m_prop_list->version();
        GraphProperty result = m_prop_list->snapshot(transaction_id);

        uint64_t tclist_version_start = m_tc_list.read_version();
        ThreadContext** __restrict list = m_tc_list.list();
        ThreadContext* tcntxt = list[0]; int i = 0;
        while(tcntxt != nullptr){
            result += tcntxt->my_local_changes(transaction_id);

            // next iteration
            tcntxt = list[++i];
        }

        uint64_t tclist_version_end = m_tc_list.read_version();
        uint64_t property_version_end = m_prop_list->version();

        // check the global property list did not change in the meanwhile
        if ( property_version_start == property_version_end && tclist_version_start == tclist_version_end ) {
            return result;
        }

    } while(true);
}


/*****************************************************************************
 *                                                                           *
 *  Auxiliary view                                                           *
 *                                                                           *
 *****************************************************************************/

void GlobalContext::aux_view(transaction::TransactionImpl* transaction, aux::View** out_views) {
    constexpr uint64_t NUM_NODES = StaticConfiguration::numa_num_nodes;
    if(transaction->is_terminated()){
        RAISE(InternalError, "The transaction is already terminated");
    }

    if(!transaction->is_read_only()){ // read-write transaction -> DynamicView
        out_views[0] = aux::DynamicView::create_undirected(memstore(), transaction);

    } else { // read-only transactions -> StaticView
        aux::StaticView** out_static_views = reinterpret_cast<aux::StaticView**>(out_views);

        if(is_aux_cache_enabled()){ // Check to cache

            uint64_t max_writer_txn_id = highest_txn_rw_id();
            bool cache_hit = m_aux_cache->get(transaction->ts_read(), max_writer_txn_id, out_static_views);

            if(!cache_hit){ // we need to compute it
                aux::StaticView::create_undirected(memstore(), transaction, out_static_views, NUM_NODES);
                // update the cache ?
                m_aux_cache->set(out_static_views, transaction->ts_read());
            }

        } else { // compute it anyway
            aux::StaticView::create_undirected(memstore(), transaction, out_static_views, NUM_NODES);
        }

    }
}

void GlobalContext::enable_aux_degree() noexcept {
    m_aux_degree_enabled = true;
}

void GlobalContext::disable_aux_degree() noexcept {
    m_aux_degree_enabled = false;
}

bool GlobalContext::is_aux_degree_enabled() const noexcept {
    return m_aux_degree_enabled;
}

void GlobalContext::enable_aux_cache() noexcept {
    if(m_aux_cache == nullptr){
        m_aux_cache = new aux::Cache();
    }
}

void GlobalContext::disable_aux_cache() noexcept {
    delete m_aux_cache; m_aux_cache = nullptr;
}

bool GlobalContext::is_aux_cache_enabled() const noexcept {
    return m_aux_cache != nullptr;
}

void GlobalContext::set_break_into_debugger(bool value) {
#if defined(MAYBE_BREAK_INTO_DEBUGGER_ENABLED)
      util::maybe_break_into_debugger_enabled = value;
#else
      ERROR("The macro MAYBE_BREAK_INTO_DEBUGGER_ENABLED must be statically defined first");
#endif
}


/*****************************************************************************
 *                                                                           *
 *  Dump                                                                     *
 *                                                                           *
 *****************************************************************************/
void GlobalContext::dump() const {
    cout << "[Local contexts]\n";
    ThreadContext** list = m_tc_list.list();
    for(uint32_t i = 0; list[i] != nullptr; i++){
        ThreadContext* tcntxt = list[i];
        cout << "[" << i << "] " <<  tcntxt << " => "; tcntxt->dump();
    }
}

} // namespace


