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
#include "teseo/transaction/transaction_sequence.hpp"

namespace teseo::context { class GlobalContext; } // forward declaration

namespace teseo::transaction {

class TransactionImpl; // forward decl.

/**
 * An ordered list of the active transactions. Each thread context owns an instance of a list
 * for the transactions that were created inside that context.
 *
 * To guarantee thread-safety we have a bit of protocol to follow:
 * - Only the local thread can invoke #insert, so there is only one writer
 * - The field `m_version' acts as a latch, if mod 2 == 0 => free, mod 2 = 1 => busy
 * - Only #insert can modify the field m_transaction_sz
 * - The method #remove can be invoked by any thread, it sets the related cell to nullptr
 * - snapshot & high_water_mark can be invoked by any thread, the field m_version will operate like an
 *   optimistic latch on the field m_version
 */
class TransactionList {
    // This class is non copyable.
    TransactionList(const TransactionList&) = delete;
    TransactionList& operator=(const TransactionList&);

    std::atomic<uint64_t> m_version = 0; // To ensure thread safety
    constexpr static uint64_t m_transactions_capacity = 32; // Max number of transactions that can be active inside a thread
    uint64_t m_transactions_sz = 0; // Number of transactions present in the list so far
    TransactionImpl* m_transactions[m_transactions_capacity]; // The actual list of active transactions
    volatile uint64_t m_highest_writer_id = 0; // The max transaction ID among the writers registered in this list

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
     * Insert the given transaction in the list
     * @return the transaction id and the slot ID assigned to the transaction
     */
    uint64_t insert(context::GlobalContext* gcntxt, TransactionImpl* transaction);

    /**
     * Remove the given transaction from the list (if present)
     */
    bool remove(TransactionImpl* transaction);

    /**
     * Retrieve a `snapshot' of all active transactions up to this moment, sorted in
     * decreasing order by the transaction startTime.
     */
    TransactionSequence snapshot(uint64_t max_transaction_id) const;

    /**
     * Retrieve the minimum transaction ID stored in the list
     */
    uint64_t high_water_mark() const;

    /**
     * Retrieve the highest transaction ID of the read-write transactions registered in this list
     */
    uint64_t highest_txn_rw_id() const;
};


} // namespace
