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
#include "teseo/context/tc_list.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::gc { class GarbageCollector; } // forward declaration
namespace teseo::memstore { class Memstore; } // forward declaration
namespace teseo::profiler { class EventGlobal; } // forward declaration
namespace teseo::profiler { class GlobalRebalanceList; } // forward declaration
namespace teseo::runtime { class Runtime; } // forward declaration
namespace teseo::transaction{ class MemoryPoolList; } // forward declaration
namespace teseo::transaction{ class TransactionImpl; } // forward declaration
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

    TcList m_tc_list; // list of all registered thread contexts
    std::atomic<uint64_t> m_txn_global_counter = 0; // global counter, where the startTime and commitTime for transactions are drawn
    PropertySnapshotList* m_prop_list { nullptr }; // global list of properties
    memstore::Memstore* m_memstore {nullptr}; // storage for the nodes/edges
    runtime::Runtime* m_runtime { nullptr }; // background threads performing maintenance tasks
    profiler::EventGlobal* m_profiler_events {nullptr}; // all internal timers used for profiling
    profiler::GlobalRebalanceList* m_profiler_rebalances {nullptr}; // record of all rebalances performed

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
    void delete_thread_context(ThreadContext* tcntxt);

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
    gc::GarbageCollector* gc() const noexcept;

    /**
     * Return the next GC in a round robin fashion
     */
    gc::GarbageCollector* next_gc() const noexcept;

    /**
     * Retrieve current snapshot for the global properties of the given transaction
     */
    GraphProperty property_snapshot(uint64_t transaction_id) const;

    /**
     * Instance to the runtime
     */
    runtime::Runtime* runtime() const noexcept;

    /**
     * Instance to the storage
     */
    memstore::Memstore* memstore();
    const memstore::Memstore* memstore() const;

    /**
     * Remove the given transaction from the transaction list.
     * This is a fall back approach. A transaction should remove itself from its thread own context. Only
     * when a thread context is not available  this method should be invoked. This situation typically
     * arises when a transaction is in roll back after the thread has been explicitly removed by the user.
     */
    void unregister_transaction(transaction::TransactionImpl* transaction);

    /**
     * Retrieve the cache of transaction pools
     */
    transaction::MemoryPoolList* transaction_pool();

    /**
     * Remove empty memory pools from the transaction pool
     */
    void refresh_transaction_pool();

    /**
     * List of events recorder in the profiler
     */
    profiler::EventGlobal* profiler_events();

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
