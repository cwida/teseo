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
#include "teseo/transaction/transaction_sequence.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::context { class GlobalContext; } // forward declaration

namespace teseo::transaction {

class TransactionImpl; // forward decl.

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

    mutable util::OptimisticLatch<0> m_latch; // To ensure thread safety
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
    uint64_t insert(context::GlobalContext* gcntxt, TransactionImpl* transaction);

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


} // namespace
