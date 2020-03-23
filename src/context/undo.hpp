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
class TransactionRollbackImpl; // forward decl.
class TransactionSequence; // forward decl.

class Undo {
    TransactionImpl* m_transaction; // transaction that performed the update
    TransactionRollbackImpl* m_data_structure; // pointer to the data structure when the update has been performed
    Undo* m_next; // pointer to the older entry in the chain
    const uint32_t m_length_payload; // the length, in bytes, of the payload associated to this undo record


public:
    /**
     * Create a new Undo entry
     * @param tx: the transaction that is performing the update
     * @param data_structure: the data structure, e.g. the sparse array, where the update has been performed
     * @param next: pointer to the next update (undo) in the chain, possible nullptr
     * @param type: the type of the record stored in the update
     * @param length: the size of the record stored in the update
     */
    Undo(TransactionImpl* tx, TransactionRollbackImpl* data_structure, Undo* next, uint32_t length);

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

    // Prune the undo chain according to the given (sorted) sequence of active transactions
    // Returns a pair with the new head of the chain (or nullptr) and the total length of the chain
    static std::pair<Undo*, uint64_t> prune(Undo* head, const TransactionSequence* sequence);

    // Prune the undo chain according to the given high watermark
    // Returns a pair with the new head of the chain (or nullptr) and the total length of the chain
    static std::pair<Undo*, uint64_t> prune(Undo* head, uint64_t high_water_mark);

    // Completely clear the chain of undos. Invoked only by the destructor of a data structure to
    // completely release the memory used
    static void clear(Undo* head);

    // Revert the changes of this entry
    void rollback();

    // Get a string representation of this undo record, for debugging purposes
    std::string to_string() const;

    // Dump to stdout the content of this undo, for debugging purposes
    void dump() const;

    // Dump the whole chains of undos
    static void dump_chain(Undo* head, int prefix_blank_spaces = 2);
};

} // namespace
