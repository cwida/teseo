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

namespace teseo::context { class GlobalContext; }
namespace teseo::context { class ThreadContext; }
namespace teseo::gc { class GarbageCollector; }
namespace teseo::memstore { class Context; }
namespace teseo::memstore { class Key; }

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

public:
    // Constructor
    Runtime(context::GlobalContext* global_context);

    // Destructor
    ~Runtime();

    // Retrieve the garbage collector
    gc::GarbageCollector* gc();

    // Schedule a rebalance
    void schedule_rebalance(const memstore::Context& context, const memstore::Key& key);

    // Schedule a pass of the GC
    void schedule_gc_pass(int worker_id);

    // Schedule a reset of the cache of active transaction
    void schedule_reset_active_transactions(std::shared_ptr<context::ThreadContext> thread_context);

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
};

} // namespace
