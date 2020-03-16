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

#include "latch.hpp"
#include "undo.hpp"

namespace teseo::internal::context {

class Transaction {
    friend class Undo;

    struct UndoBuffer {
        constexpr static uint64_t BUFFER_SZ = 264192; // 256kb
        char m_buffer[BUFFER_SZ];
        uint64_t m_space_left = BUFFER_SZ;
        UndoBuffer* m_next {nullptr}; // pointer to the next undo log in the chain
    };

    enum class State {
        PENDING,
        ERROR,
        COMMITTED,
        ABORTED
    };

    mutable Latch m_transaction_latch; // used to sync by multiple threads operating on the same transaction
    uint64_t m_transaction_id; // either the startTime or commitTime of the transaction, depending on m_state
    int m_num_undo_todo; // ref count on the number of undo entries that can be reached by some pointer
    State m_state;
    mutable OptimisticLatch<0> m_undo_latch; // sync the access to the undo records
    UndoBuffer* m_undo_last; // pointer to the last undo log in the chain
    UndoBuffer m_undo_buffer; // first undo log in the chain
    bool m_user_reachable = true; // whether there are still user threads referring to this TX

    // Commit the transaction (assume the write latch has already been acquired)
    void do_commit();

    // Rollback the transaction (assume the write latch has already been acquired)
    void do_rollback();

    // Tick the number of undos processed/obsolete
    void tick_undo();

public:
    Transaction(uint64_t transaction_id);

    // Destructor
    ~Transaction();

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

    // Mark the transaction as unreachable from the user, ready to be garbage collected by the epoch based GC
    // This method is implicitly invoked by the thread local shared_ptr when the last user thread removes
    // the last reference to the transaction
    void mark_user_unreachable();

    // Dump the content of this transaction to stdout, for debugging purposes
    void dump() const;
};

/**
 * Retrieve the current transaction
 */
Transaction* transaction();

/**
 * Implementation details
 */
template<typename T>
Undo* Transaction::add_undo(void* data_structure, Undo* next, UndoType type, const T* payload){
    return add_undo(data_structure, next, type, sizeof(T), (void*) payload);
}
template<typename T>
Undo* Transaction::add_undo(void* data_structure, Undo* next, UndoType type, const T& payload){
    return add_undo(data_structure, next, type, sizeof(T), (void*) &payload);
}

}
