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

#include <cinttypes>
#include <memory>
#include "latch.hpp"
#include "property_snapshot.hpp"
#include "transaction_impl.hpp"

namespace teseo::internal::context {

class GlobalContext; // forward decl.
class ThreadContext {
    friend class GlobalContext;

    GlobalContext* m_global_context; // pointer to the instance of the database
    uint64_t m_epoch; // current epoch of the thread
    OptimisticLatch<0> m_latch; // latch, used to manage the linked list of thread contexts
    ThreadContext* m_next; // next thread context in the chain
    TransactionList m_tx_list; // sorted list of active transactions
    TransactionSequence* m_tx_seq; // the sequence of all active transactions
    PropertySnapshotList m_prop_list; // list of the global alterations performed to the graph (vertex count/edge count)

#if !defined(NDEBUG) // thread contexts are always associated to a single logical thread, keep thrack of its ID for debugging purposes
    const int64_t m_thread_id;
#endif


public:
    /**
     * Create a new thread context, associated to the given database instance
     */
    ThreadContext(GlobalContext* global_context);

    /**
     * Destructor
     */
    ~ThreadContext();

    /**
     * Enter a new epoch in the current context
     */
    void epoch_enter();

    /**
     * Exit the epoch in the current context
     */
    void epoch_exit();

    /**
     * Retrieve the current epoch for this context
     */
    uint64_t epoch() const;

    /**
     * Register the given transaction in this context
     */
    void register_transaction(TransactionImpl* tx);

    /**
     * Unregister the given transaction in this context
     */
    void unregister_transaction(TransactionImpl* tx);

    /**
     * Retrieve the list of active transactions in this context
     */
    TransactionSequence my_active_transactions() const;

    /**
     * Retrieve the mimimum transaction ID among the active transactions in this context
     */
    uint64_t my_high_water_mark() const;

    /**
     * Retrieve the list of all active transactions in the global context
     */
    TransactionSequence* all_active_transactions();

    /**
     * Clear the cache of all active transactions in the global context
     */
    void reset_cache_active_transactions();

    /**
     * Save the local property alteration to the property list
     */
    void save_local_changes(GraphProperty& changes, uint64_t transaction_id);

    /**
     * Retrieve the local changes of this thread context
     */
    GraphProperty my_local_changes(uint64_t transaction_id) const;

    /**
     * Retrieve the global context associated to the given local context
     */
    GlobalContext* global_context() noexcept;
    const GlobalContext* global_context() const noexcept;

    /**
     * Dump the content of this context to stdout, for debugging purposes
     */
    void dump() const;
};

/**
 * Retrieve the current thread context
 */
ThreadContext* thread_context();
std::shared_ptr<ThreadContext> shptr_thread_context();


/**
 * Implementation details
 */
inline
void ThreadContext::register_transaction(TransactionImpl* tx) {
    m_tx_list.insert(tx);
}

inline
void ThreadContext::unregister_transaction(TransactionImpl* tx) {
    m_tx_list.remove(tx);
}

inline
TransactionSequence ThreadContext::my_active_transactions() const{
    return m_tx_list.snapshot();
}

inline
uint64_t ThreadContext::my_high_water_mark() const {
    return m_tx_list.high_water_mark();
}

inline
GraphProperty ThreadContext::my_local_changes(uint64_t transaction_id) const {
    return m_prop_list.snapshot(transaction_id);
}

} // namespace
