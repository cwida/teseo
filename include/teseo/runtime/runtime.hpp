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

#pragma once

#include <memory>

#include "teseo/runtime/queue.hpp"
#include "teseo/runtime/task.hpp"
#include "teseo/runtime/timer_service.hpp"

namespace teseo::aux { class PartialResult; }
namespace teseo::context { class GlobalContext; }
namespace teseo::context { class ThreadContext; }
namespace teseo::gc { class GarbageCollector; }
namespace teseo::memstore { class Context; }
namespace teseo::memstore { class Key; }
namespace teseo::memstore { class Leaf; }
namespace teseo::memstore { class Segment; }
namespace teseo::transaction { class MemoryPoolList; }

namespace teseo::runtime {

/**
 * Execute tasks asynchronously in the background.
 *
 * This class is thread-safe.
 */
class Runtime {
    context::GlobalContext* m_global_context; // pointer to the owner of this instance
    Queue m_queue; // workers' queues
    TimerService m_timer_service; // schedule tasks in the future
    std::atomic<uint64_t> m_gc_next_counter = 0; // counter to return the next GC
    std::atomic<uint64_t> m_rr_next_counter = 0; // counter to return the next worker in a round robin fashion

    // Submit a synchronous task to all workers, wait for its completion before resuming
    void execute_sync(TaskType task_type);

public:
    // Constructor
    Runtime(context::GlobalContext* global_context);

    // Destructor
    ~Runtime();

    // Retrieve the garbage collector
    gc::GarbageCollector* gc();

    // Retrieve the next GC instance from the list, in round robin fashion
    gc::GarbageCollector* next_gc();

    // Retrieve the next worker ID to process a task, in round robin fashion
    int next_worker_id();

    // Retrieve a random transaction pool
    transaction::MemoryPoolList* transaction_pool();

    // Retrieve the transaction pool associated to the given worker ID
    transaction::MemoryPoolList* transaction_pool(int worker_id);

    // Rebalance the given segment `synchronously'. Used for testing purposes.
    void rebalance_first_leaf();
    void rebalance_first_leaf(memstore::Memstore* memstore, uint64_t segment_id);
    void rebalance_segment_sync(memstore::Memstore* memstore, memstore::Leaf* leaf, memstore::Segment* segment);

    // Compute a partial result for the auxiliary vector
    void aux_partial_result(const memstore::Context& context, aux::PartialResult* partial_result);

    // Schedule a rebalance
    void schedule_rebalance(const memstore::Context& context, const memstore::Key& key);

    // Schedule a pass of the GC
    void schedule_gc_pass(int worker_id);

    // Schedule a maintenance pass of the buffer pool
    void schedule_bp_pass();

    // Schedule a maintenance pass of the cached memory pools, to rebuild their free lists
    void schedule_txnpool_pass(int worker_id);

    // Schedule a reset of the cache of active transaction
    void schedule_reset_active_transactions();

    // Send the given task to the worker
    // @param worker_id the id, in [0, num_workers), of the worker. If < 0, then pick a random worker.
    void execute(Task task, int worker_id);

    // Register a thread contexts in the workers
    void register_thread_contexts();

    // Remove the thread contexts in the workers
    void unregister_thread_contexts();

    // Enable the asynchronous rebalances
    void enable_rebalance();

    // Disable the asynchronous rebalances
    void disable_rebalance();

    // Pointer to the global context
    context::GlobalContext* global_context();

    // Deallocate the payload associated to the given task
    static void delete_task(Task task);

    // Stop the timer service
    void stop_timer();
};

} // namespace
