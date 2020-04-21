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
#include <string>

namespace teseo::context { class GlobalContext; } // forward declaration

namespace teseo::transaction {

class TransactionList; // forward declaration

/**
 * A sorted immutable sequence of transaction IDs, ordered in decreasing order by their start time
 */
class TransactionSequence {
    TransactionSequence(const TransactionSequence&) = delete;
    TransactionSequence& operator=(const TransactionSequence&) = delete;
    friend class TransactionList;
    friend class context::GlobalContext;

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

    /**
     * Retrieve a string representation of the sequence, for debugging purposes
     */
    std::string to_string() const;

    /**
     * Dump the content of the sequence to stdout, for debugging purposes
     */
    void dump() const;
};

/**
 * Print to the output stream the content of the sequence
 */
std::ostream& operator<<(std::ostream& out, const TransactionSequence& sequence);

} // namespace
