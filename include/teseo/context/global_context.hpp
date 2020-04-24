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

#include <atomic>
#include <memory>
#include <mutex>

#include "teseo/context/property_snapshot.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::memstore { class Memstore; } // forward declaration
namespace teseo::profiler { class EventGlobal; } // forward declaration
namespace teseo::profiler { class GlobalRebalanceList; } // forward declaration
namespace teseo::rebalance { class AsyncService; } // forward declaration
namespace teseo::transaction{ class MemoryPool; } // forward declaration
namespace teseo::transaction{ class MemoryPoolList; } // forward declaration
namespace teseo::transaction{ class TransactionSequence; } // forward declaration


namespace teseo::context {
class GarbageCollector; // forward declaration
class ThreadContext; // forward declaration
class TcTimer; // forward declaration

/**
 * A database instance
 */
class GlobalContext {
    GlobalContext(const GlobalContext&) = delete;
    GlobalContext& operator=(const GlobalContext& ) = delete;

    ThreadContext* m_tc_head {nullptr}; // linked list of registered contexts
    mutable util::OptimisticLatch<0> m_tc_latch; // latch for the head of registered contexts
    std::atomic<uint64_t> m_txn_global_counter = 0; // global counter, where the startTime and commitTime for transactions are drawn
    PropertySnapshotList* m_prop_list { nullptr }; // global list of properties
    GarbageCollector* m_garbage_collector {nullptr}; // pointer to the epoch-based garbage collector
    TcTimer* m_tctimer {nullptr}; // the service to flush the active transactions caches
    memstore::Memstore* m_memstore {nullptr}; // storage for the nodes/edges
    rebalance::AsyncService* m_async {nullptr}; // asynchronous rebalancers
    transaction::MemoryPoolList* m_txn_pool_list { nullptr }; // cache of transaction pools
    profiler::EventGlobal* m_profiler {nullptr}; // profiler events
    profiler::GlobalRebalanceList* m_rebalances {nullptr}; // record of all rebalances performed

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
     * Unregister the thread context associated to the current thread
     */
    void unregister_thread();

    /**
     * Retrieve the list of all active transactions, up to this moment
     */
    transaction::TransactionSequence* active_transactions();

    /**
     * Retreive the minimum transaction ID among the active transactions
     */
    uint64_t high_water_mark() const;

    /**
     * Remove the given thread from the list of contest
     */
    static void delete_thread_context(ThreadContext* tcntxt);

    /**
     * Generate a new transaction id from the global counter, to be used for the startTime & commitTime
     */
    uint64_t next_transaction_id();

    /**
     * Retrieve the min epoch among all registered threads
     */
    uint64_t min_epoch() const;

    /**
     * Instance to the epoch-based garbage collector
     */
    GarbageCollector* gc() const noexcept;

    /**
     * Retrieve current snapshot for the global properties of the given transaction
     */
    GraphProperty property_snapshot(uint64_t transaction_id) const;

    /**
     * Instance to the ThreadContext timer service
     */
    TcTimer* tctimer() const noexcept;

    /**
     * Instance to the storage
     */
    memstore::Memstore* memstore();
    const memstore::Memstore* memstore() const;

    /**
     * Asynchronous rebalancers
     */
    rebalance::AsyncService* async();

    /**
     * Retrieve a new transaction pool
     */
    transaction::MemoryPool* new_transaction_pool(transaction::MemoryPool* old_txn_pool = nullptr);

    /**
     * List of events recorder in the profiler
     */
    profiler::EventGlobal* profiler();

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
