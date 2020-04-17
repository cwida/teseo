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
#include <cinttypes>
#include <memory>
#include <mutex>

#include "teseo/context/property_snapshot.hpp"
#include "teseo/util/latch.hpp"

/*****************************************************************************
 *                                                                           *
 *   Forward declarations & aliases                                          *
 *                                                                           *
 *****************************************************************************/
namespace teseo::context {
    class GlobalContext;
    class ThreadContext;
}
namespace teseo::transaction {
    class RollbackInterface;
    using TransactionWriteLatch = std::lock_guard<util::OptimisticLatch<0>>;
    class UndoBuffer;
    class Undo;
}

/*****************************************************************************
 *                                                                           *
 *   Transaction                                                             *
 *                                                                           *
 *****************************************************************************/

namespace teseo::transaction {

/**
 * The actual implementation of a user transaction
 */
class TransactionImpl {
    friend class Undo;

    enum State {
        PENDING = 0,
        COMMITTED = 2,
        ABORTED = 3
    };

    std::shared_ptr<context::ThreadContext> m_thread_context; // the thread context owning this transaction
    context::GlobalContext* const m_global_context; // pointer to the global context
    mutable util::OptimisticLatch<0> m_latch; // used to sync by multiple threads operating on the same transaction
    uint64_t m_transaction_id; // the transaction ID, depending on the state, this is either the startTime or commitTime
    State m_state;
    UndoBuffer* m_undo_last = nullptr; // pointer to the last undo log in the chain
    std::atomic<int64_t> m_ref_count_user = 0; // number of entry pointers from the user
    std::atomic<int64_t> m_ref_count_system = 0; // number of entry pointers from the implementations
    mutable context::GraphProperty m_prop_global; // global changes to the graph
    mutable std::atomic<uint64_t> m_prop_global_sync = 0; // latch to compute the global properties
    context::GraphProperty m_prop_local; // local changes
    const bool m_read_only; // true if the transaction has flagged as read only upon creation

    // Commit the transaction (assume the write latch has already been acquired)
    void do_commit();

    // Mark the transaction as unreachable from the user.
    void mark_user_unreachable();

    // Mark the transaction as unreachable from the system
    void mark_system_unreachable();

public:
    TransactionImpl(UndoBuffer* undo_buffer, std::shared_ptr<context::ThreadContext> thread_context, bool read_only);

    // Destructor
    ~TransactionImpl();

    // Get the startTime of this transaction
    uint64_t ts_start() const;

    // Get the startTime or commitTime of the transaction
    uint64_t ts_read() const;

    // Get the transactionID or commitTime of the transaction
    uint64_t ts_write() const;

    // Check whether the current transaction terminated
    bool is_terminated() const;

    // Check whether the transaction locked the given undo record
    bool owns(Undo* undo) const;

    // Check whether the given item can be written by the transaction according to the state of the undo entry
    bool can_write(Undo* undo) const;

    // Check whether the current transaction can read the given change
    // @return true if the content to read is the image in the storage, false if the tx needs to read out_payload
    bool can_read(const Undo* undo, void** out_payload) const;

    // Add an undo record
    Undo* add_undo(RollbackInterface* data_structure, uint32_t payload_length, void* payload);
    template<typename T> Undo* add_undo(RollbackInterface* data_structure, const T* payload); // shortcut
    template<typename T> Undo* add_undo(RollbackInterface* data_structure, const T& payload); // shortcut

    // Commit the transaction
    void commit();

    // Rollback and undo all changes in this transaction
    void rollback();

    // Rollback N changes in this transaction (assume the write latch has already been acquired)
    void do_rollback(uint64_t N = std::numeric_limits<uint64_t>::max());

    // Retrieve the transaction latch
    util::OptimisticLatch<0>& latch() const;

    // Check whether the transaction has been flagged read only
    bool is_read_only() const;

    // Manage the reference counters
    void incr_system_count();
    void incr_user_count();
    void decr_system_count();
    void decr_user_count();

    // Retrieve/update the graph counters for the local changes
    context::GraphProperty& local_graph_changes();
    const context::GraphProperty& local_graph_changes() const;

    // Retrieve the vertex/edge count of the graph
    context::GraphProperty graph_properties() const;

    // Dump the content of this transaction to stdout, for debugging purposes
    void dump() const;
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
template<typename T>
Undo* TransactionImpl::add_undo(RollbackInterface* data_structure, const T* payload){
    return add_undo(data_structure, sizeof(T), (void*) payload);
}

template<typename T>
Undo* TransactionImpl::add_undo(RollbackInterface* data_structure, const T& payload){
    return add_undo(data_structure, sizeof(T), (void*) &payload);
}

inline
void TransactionImpl::incr_system_count(){
    m_ref_count_system++;
}

inline
void TransactionImpl::incr_user_count(){
    m_ref_count_user++;
}

inline
void TransactionImpl::decr_system_count(){
    if(--m_ref_count_system == 0){ mark_system_unreachable(); }
}

inline
void TransactionImpl::decr_user_count(){
    if(--m_ref_count_user == 0){ mark_user_unreachable(); }
}

inline
util::OptimisticLatch<0>& TransactionImpl::latch() const {
    return m_latch;
}

inline
bool TransactionImpl::is_read_only() const {
    return m_read_only;
}

} // namespace
