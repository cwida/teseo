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
#include <vector>

#include "latch.hpp"
#include "property_snapshot.hpp"
#include "undo.hpp"

namespace teseo::internal::context {

class GlobalContext; // forward declaration
class ThreadContext; // forward declaration
class TransactionImpl; // forward declaration
class TransactionList; // forward declaration
class TransactionRollbackImpl; // forward declaration
class TransactionSequence; // forward declaration
using TransactionWriteLatch = std::lock_guard<OptimisticLatch<0>>;

class TransactionImpl {
    friend class Undo;

    struct UndoBuffer {
        constexpr static uint64_t BUFFER_SZ = 264192; // 256kb
        char m_buffer[BUFFER_SZ];
        uint64_t m_space_left = BUFFER_SZ;
        UndoBuffer* m_next {nullptr}; // pointer to the next undo log in the chain
    };

    enum State {
        PENDING = 0,
        ERROR = 1,
        COMMITTED = 2,
        ABORTED = 3
    };


    std::shared_ptr<ThreadContext> m_thread_context; // the thread context owning this transaction
    GlobalContext* const m_global_context; // pointer to the global context
    mutable OptimisticLatch<0> m_latch; // used to sync by multiple threads operating on the same transaction
    uint64_t m_transaction_id; // the transaction ID, depending on the state, this is either the startTime or commitTime
    State m_state;
    UndoBuffer* m_undo_last; // pointer to the last undo log in the chain
    UndoBuffer m_undo_buffer; // first undo log in the chain
    std::atomic<int64_t> m_ref_count_user = 0; // number of entry pointers from the user
    std::atomic<int64_t> m_ref_count_system = 0; // number of entry pointers from the implementations
    mutable GraphProperty m_prop_global; // global changes to the graph
    mutable std::atomic<uint64_t> m_prop_global_sync = 0; // latch to compute the global properties
    GraphProperty m_prop_local; // local changes
    const bool m_read_only; // true if the transaction has flagged as read only upon creation

    // Commit the transaction (assume the write latch has already been acquired)
    void do_commit();

    // Rollback the transaction (assume the write latch has already been acquired)
    void do_rollback();

    // Mark the transaction as unreachable from the user.
    void mark_user_unreachable();

    // Mark the transaction as unreachable from the system
    void mark_system_unreachable();

public:
    TransactionImpl(std::shared_ptr<ThreadContext> thread_context, bool read_only = false);

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

    // Check whether the current transaction is in an erroneous state
    bool is_error() const;

    // Check whether the transaction locked the given undo record
    bool owns(Undo* undo) const;

    // Check whether the given item can be written by the transaction according to the state of the undo entry
    bool can_write(Undo* undo) const;

    // Check whether the current transaction can read the given change
    // @return true if the content to read is the image in the storage, false if the tx needs to read out_payload
    bool can_read(const Undo* undo, void** out_payload) const;

    // Add an undo record
    Undo* add_undo(TransactionRollbackImpl* data_structure, Undo* next, uint32_t payload_length, void* payload);
    template<typename T> Undo* add_undo(TransactionRollbackImpl* data_structure, Undo* next, const T* payload); // shortcut
    template<typename T> Undo* add_undo(TransactionRollbackImpl* data_structure, Undo* next, const T& payload); // shortcut

    // Commit the transaction
    void commit();

    // Rollback and undo all changes in this transaction
    void rollback();

    // Retrieve the transaction latch
    OptimisticLatch<0>& latch() const;

    // Check whether the transaction has been flagged read only
    bool is_read_only() const;

    // Manage the reference counters
    void incr_system_count();
    void incr_user_count();
    void decr_system_count();
    void decr_user_count();

    // Retrieve/update the graph counters for the local changes
    GraphProperty& local_graph_changes();
    const GraphProperty& local_graph_changes() const;

    // Retrieve the vertex/edge count of the graph
    GraphProperty graph_properties() const;

    // Dump the content of this transaction to stdout, for debugging purposes
    void dump() const;
};

/**
 * This is just an interface, whose only method is invoked when we need to revert a change (insert/
 * update/remove) previously performed by a transaction
 */
class TransactionRollbackImpl {
public:
    // Destructor placeholder
    virtual ~TransactionRollbackImpl();

    // Rollback a previously performed object
    // @param object the opaque item stored in the undo object, to reconstruct the change to revert
    // @param next the next item in the undo chain list, if anyone
    virtual void do_rollback(void* object, Undo* next) = 0;
};

/**
 * A sorted immutable sequence of transaction IDs, ordered in decreasing order by their start time
 */
class TransactionSequence {
    TransactionSequence(const TransactionSequence&) = delete;
    TransactionSequence& operator=(const TransactionSequence&) = delete;

    friend class GlobalContext;
    friend class TransactionList;
    uint64_t* m_transaction_ids = nullptr; // the actual sequence of transactions
    /*const*/ uint64_t m_num_transactions = 0; // the total number of transactions contained

    // Invoked by GlobalContext#active_transactions() and TransactionList#snapshot()
    TransactionSequence(uint64_t num_transactions);
public:
    /**
     * Create an empty sequence
     */
    TransactionSequence();

    /**
     * Move constructor
     */
    TransactionSequence(TransactionSequence&& move);
    TransactionSequence& operator=(TransactionSequence&& move);

    /**
     * Destructors
     */
    ~TransactionSequence();

    /**
     * Total number of transactions contained in the sequence. Some entries may be null
     */
    uint64_t size() const;

    /**
     * Retrieve the transaction at the given position in the sequence. Some entries may be null.
     */
    uint64_t operator[](uint64_t index) const;
};


/**
 * A forward iterator over a transaction sequence
 */
class TransactionSequenceForwardIterator {
    const TransactionSequence* m_sequence;
    uint64_t m_position = 0;

public:
    TransactionSequenceForwardIterator(const TransactionSequence* sequence);

    /**
     * Whether the iterator has been depleted
     */
    bool done() const;

    /**
     * Retrieve the current key in the sequence, or INT_MAX if the iterator has been depleted
     */
    uint64_t key() const;

    /**
     * Fetch the next key in the sequence
     */
    void next();
};


/**
 * A backward iterator over a transaction sequence
 */
class TransactionSequenceBackwardsIterator {
    const TransactionSequence* m_sequence;
    int64_t m_position = -1;

public:
    TransactionSequenceBackwardsIterator(const TransactionSequence* sequence);

    /**
     * Whether the iterator has been depleted
     */
    bool done() const;

    /**
     * Retrieve the current key in the sequence, or INT_MIN if the iterator has been depleted
     */
    uint64_t key() const;

    /**
     * Fetch the previous key in the sequence
     */
    void next();
};

/**
 * An ordered list of the active transactions. Each thread context owns an instance of a list
 * for the transactions that were created inside that context.
 *
 * This class is thread-safe.
 */
class TransactionList {
    // This class is non copyable.
    TransactionList(const TransactionList&) = delete;
    TransactionList& operator=(const TransactionList&);

    mutable OptimisticLatch<0> m_latch; // To ensure thread safety
    constexpr static uint64_t m_transactions_capacity = 32; // Max number of transactions that can be active inside a thread
    uint64_t m_transactions_sz = 0; // Number of transactions present in the list so far
    TransactionImpl* m_transactions[m_transactions_capacity]; // The actual list of active transactions

public:
    /**
     * Initialise an empty transaction list
     */
    TransactionList();

    /**
     * Destructor
     */
    ~TransactionList();

    /**
     * Insert the given transaction in the list & increment its system ref count
     * @return the transaction id assigned to the transaction
     */
    uint64_t insert(GlobalContext* gcntxt, TransactionImpl* transaction);

    /**
     * Remove the given transaction from the list & decrement its sytem ref count by 1
     */
    void remove(TransactionImpl* transaction);

    /**
     * Retrieve a `snapshot' of all active transactions up to this moment, sorted in
     * decreasing order by the transaction startTime.
     */
    TransactionSequence snapshot() const;

    /**
     * Retrieve the minimum transaction ID stored in the list
     */
    uint64_t high_water_mark() const;
};

/**
 * Implementation details
 */
template<typename T>
Undo* TransactionImpl::add_undo(TransactionRollbackImpl* data_structure, Undo* next, const T* payload){
    return add_undo(data_structure, next, sizeof(T), (void*) payload);
}

template<typename T>
Undo* TransactionImpl::add_undo(TransactionRollbackImpl* data_structure, Undo* next, const T& payload){
    return add_undo(data_structure, next, sizeof(T), (void*) &payload);
}

//inline
//void TransactionImpl::incr_system_count(){
//    m_ref_count_system++;
//}
//
//inline
//void TransactionImpl::incr_user_count(){
//    m_ref_count_user++;
//}
//
//inline
//void TransactionImpl::decr_system_count(){
//    if(--m_ref_count_system == 0){ mark_system_unreachable(); }
//}
//
//inline
//void TransactionImpl::decr_user_count(){
//    if(--m_ref_count_user == 0){ mark_user_unreachable(); }
//}

inline
OptimisticLatch<0>& TransactionImpl::latch() const {
    return m_latch;
}

inline
bool TransactionImpl::is_read_only() const {
    return m_read_only;
}

}
