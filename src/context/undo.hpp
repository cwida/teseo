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
#include <ostream>

namespace teseo::internal::context {

class TransactionImpl; // forward decl.

enum class UndoType : uint16_t {
    SparseArrayUpdate // the undo represents an update to the sparse array
};

class Undo {
    TransactionImpl* m_transaction; // transaction that performed the update
    union {
        void* m_data_structure; // pointer to the data structure when the update has been performed
        Undo* m_previous; // pointer to the newest entry in the chain, or
    };
    Undo* m_next; // pointer to the older entry in the chain
    UndoType m_type; // the type of record stored in the undo
    uint16_t m_flags; // internal flags
    const uint32_t m_length_payload; // the length, in bytes, of the payload associated to this undo record

    enum UndoFlag {
        UNDO_FIRST = 0x1, // this is the first m_previous is actually a pointer to the sparse array
        UNDO_REVERTED = 0x2, // this undo entry is orphan or already processed
    };

    // Set & check whether a given flag is set
    void set_flag(UndoFlag flag, bool value = true);
    bool has_flag(UndoFlag flag) const;

    // Ignore the changes from this undo record. Assume that the undo latch has already been acquired
    void do_ignore();

public:
    /**
     * Create a new Undo entry
     * @param tx: the transaction that is performing the update
     * @param data_structure: the data structure, e.g. the sparse array, where the update has been performed
     * @param next: pointer to the next update (undo) in the chain, possible nullptr
     * @param type: the type of the record stored in the update
     * @param length: the size of the record stored in the update
     */
    Undo(TransactionImpl* tx, void* data_structure, Undo* next, UndoType type, uint32_t length);

    // The transaction the performed the update
    const TransactionImpl* transaction() const;
    TransactionImpl* transaction();

    // The id associated to this update
    uint64_t transaction_id() const;

    // Pointer to the payload associated to this undo entry
    void* payload() const;

    // Length of the undo record & its associated payload, in bytes
    uint64_t length() const;

    // Read the next undo record in the chain
    Undo* next() const;

    // Revert the changes of this entry
    void rollback();

    // Ignore the changes from this undo record
    void ignore();

    // Mark the whole chain of records in the undo list obsolete
    static void mark_chain_obsolete(Undo* head);

    // Mark this undo entry as the first in the chain
    void mark_first(void* data_structure);

    // Dump to stdout the content of this undo, for debugging purposes
    void dump() const;

    // Dump the whole chains of undos
    void dump_chain(int prefix_blank_spaces = 4) const;
};

} // namespace
