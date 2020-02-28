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

#include <mutex>
#include "latch.hpp"

namespace teseo::internal::gc { class EpochGarbageCollector; } // forward decl.
namespace teseo::internal::context {
class ThreadContext; // forward declaration

// sync messages to stdout, for debugging purposes only
extern std::mutex g_debugging_mutex;

/**
 * A database instance
 */
class GlobalContext {
    GlobalContext(const GlobalContext&) = delete;
    GlobalContext& operator=(const GlobalContext& ) = delete;

    ThreadContext* m_tc_head {nullptr}; // linked list of the registered contexts
    mutable OptimisticLatch<0> m_tc_latch; // latch for the head of registered contexts
    std::atomic<uint64_t> m_txn_global_counter = 0; // global counter, where the startTime and commitTime for transactions are drawn
    teseo::internal::gc::EpochGarbageCollector* m_garbage_collector {nullptr}; // pointer to the epoch-based garbage collector

public:
    /**
     * Constructor
     */
    GlobalContext();

    /**
     * Destructor
     */
    ~GlobalContext();

    /**
     * Register the current thread with a thread context
     */
    void register_thread();

    /**
     * Unregister the current thread from a thread context
     */
    void unregister_thread();

    /**
     * Generate a new transaction id from the global counter, to be used for the startTime & commitTime
     */
    uint64_t next_transaction_id();

    /**
     * Retrieve the min epoch among all registered threads
     */
    uint64_t min_epoch() const;

    /**
     * Retrieve the current global instance
     */
    static GlobalContext* global_context();

    /**
     * Retrieve the current local context
     */
    static ThreadContext* thread_context();

    /**
     * Instance to the epoch-based garbage collector
     */
    teseo::internal::gc::EpochGarbageCollector* gc() const noexcept;

    /**
     * Dump the content of the global context, for debugging purposes
     */
    void dump() const;
};

/**
 * Retrieve the DBMS associated to the current thread
 */
GlobalContext* global_context();

} // namesapce
