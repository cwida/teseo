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
#include "undo.hpp"

namespace teseo::internal::context {

class GlobalContext; // forward declaration
class ThreadContext; // forward declaration
class TransactionImpl; // forward declaration
class TransactionList; // forward declaration
class TransactionSequence; // forward declaration

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
    mutable Latch m_transaction_latch; // used to sync by multiple threads operating on the same transaction
    const uint64_t m_start_time; // the startTime of this transaction
    uint64_t m_commit_time = 0; // the commitTime of this transaction
    State m_state;
    mutable OptimisticLatch<0> m_undo_latch; // sync the access to the undo records
    UndoBuffer* m_undo_last; // pointer to the last undo log in the chain
    UndoBuffer m_undo_buffer; // first undo log in the chain
    std::atomic<int64_t> m_ref_count_user = 0; // number of entry pointers from the user
    std::atomic<int64_t> m_ref_count_system = 0; // number of entry pointers from the implementations

    // Commit the transaction (assume the write latch has already been acquired)
    void do_commit();

    // Rollback the transaction (assume the write latch has already been acquired)
    void do_rollback();

    // Mark the transaction as unreachable from the user.
    void mark_user_unreachable();

    // Mark the transaction as unreachable from the system
    void mark_system_unreachable();

public:
    TransactionImpl(std::shared_ptr<ThreadContext> thread_context, uint64_t transaction_id);

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
    bool can_read(const Undo* undo, void** out_payload) const;

    // Add an undo record
    Undo* add_undo(void* data_structure, Undo* next, UndoType type, uint32_t payload_length, void* payload);
    template<typename T> Undo* add_undo(void* data_structure, Undo* next, UndoType type, const T* payload); // shortcut
    template<typename T> Undo* add_undo(void* data_structure, Undo* next, UndoType type, const T& payload); // shortcut

    // Commit the transaction
    void commit();

    // Rollback and undo all changes in this transaction
    void rollback();

    // Manage the reference counters
    void incr_system_count(){ m_ref_count_system++; }
    void incr_user_count(){ m_ref_count_user++; }
    void decr_system_count(){ if(--m_ref_count_system == 0){ mark_system_unreachable(); } }
    void decr_user_count(){ if(--m_ref_count_user == 0){ mark_user_unreachable(); } }

    // Dump the content of this transaction to stdout, for debugging purposes
    void dump() const;
};


///**
// * A sorted immutable sequence of Transactions, ordered in decreasing order by their start time
// */
//class TransactionSequence {
//    friend class GlobalContext;
//    friend class TransactionList;
//    TransactionImpl** m_transactions; // the actual sequence of transactions
//    /*const*/ uint64_t m_num_transactions; // the total number of transactions contained
//
//    // Invoked by GlobalContext#active_transactions() TransactionList#snapshot()
//    TransactionSequence(uint64_t num_transactions);
//
//    // Remove all elements in the sequence
//    void clear();
//public:
//    /**
//     * Create an empty sequence
//     */
//    TransactionSequence();
//
//    /**
//     * Copy & move ctors
//     */
//    TransactionSequence(const TransactionSequence&);
//    TransactionSequence& operator=(const TransactionSequence&);
//    TransactionSequence(TransactionSequence&&);
//    TransactionSequence& operator=(TransactionSequence&&);
//
//    /**
//     * Destructors
//     */
//    ~TransactionSequence();
//
//    /**
//     * Total number of transactions contained in the sequence. Some entries may be null
//     */
//    uint64_t size() const;
//
//    /**
//     * Retrieve the transaction at the given position in the sequence. Some entries may be null.
//     */
//    TransactionImpl* operator[](uint64_t index);
//    const TransactionImpl* operator[](uint64_t index) const;
//};


/**
 * A sorted immutable sequence of transaction IDs, ordered in decreasing order by their start time
 */
class TransactionSequence {
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
     */
    void insert(TransactionImpl* transaction);

    /**
     * Remove the given transaction from the list & decrement its sytem ref count by 1
     */
    void remove(TransactionImpl* transaction);

    /**
     * Retrieve a `snapshot' of all active transactions up to this moment, sorted in
     * decreasing order by the transaction startTime.
     */
    TransactionSequence snapshot() const;
};

/**
 * Implementation details
 */
template<typename T>
Undo* TransactionImpl::add_undo(void* data_structure, Undo* next, UndoType type, const T* payload){
    return add_undo(data_structure, next, type, sizeof(T), (void*) payload);
}
template<typename T>
Undo* TransactionImpl::add_undo(void* data_structure, Undo* next, UndoType type, const T& payload){
    return add_undo(data_structure, next, type, sizeof(T), (void*) &payload);
}

}
