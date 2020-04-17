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

#include "teseo/transaction/transaction_list.hpp"

#include <cassert>
#include <cstring>

#include "teseo/context/global_context.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/error.hpp"

using namespace std;

namespace teseo::transaction {

TransactionList::TransactionList() {
    memset(m_transactions, '\0', sizeof(m_transactions));
}

TransactionList::~TransactionList() {
    assert(m_transactions_sz == 0 && "There should not be any active transactions, otherwise they ref pointers will become dangling");
}

uint64_t TransactionList::insert(context::GlobalContext* gcntxt, TransactionImpl* transaction) {
    m_latch.lock(); // xlock
    if(m_transactions_sz == m_transactions_capacity){
        m_latch.unlock();
        RAISE(LogicalError, "There are too many active transactions in this thread");
    } else {
        m_transactions[m_transactions_sz] = transaction;
        m_transactions_sz++;

        // we have to assign here the transaction ID, to avoid a potential data race, if done before:
        // Thread #1 starts a new transaction and creates a new transaction ID, e.g. 7
        // Thread #2 executes #active_transactions() and read the next transaction ID: 8
        // Thread #2 if completes the invocation before Thread #1 invokes this method, it thinks that the next minimum transaction ID is 7, rather than 8
        uint64_t transaction_id = gcntxt->next_transaction_id();

        m_latch.unlock();
        transaction->incr_system_count();

        return transaction_id;
    }
}

void TransactionList::remove(TransactionImpl* transaction){
    assert(transaction != nullptr && "Null pointer");

    m_latch.lock(); // xlock

    int64_t num_active_transactions = m_transactions_sz;
    int64_t i = 0;
    while(i < num_active_transactions && m_transactions[i] != transaction){ i ++; }

    if(i == num_active_transactions){
        m_latch.unlock();
        RAISE(InternalError, "Transaction not found in the active list: " << transaction);
    }

    // shift the remaining transactions back of one position
    num_active_transactions--;
    while(i < num_active_transactions){
        m_transactions[i] = m_transactions[i +1];
        i++;
    }

    assert(m_transactions_sz > 0 && "Underflow");
    m_transactions_sz--;

    m_latch.unlock();

    transaction->decr_system_count();
}

TransactionSequence TransactionList::snapshot() const {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Need to be inside an epoch");

    do {
        try {
            uint64_t version = m_latch.read_version();
            int64_t num_active_transactions = m_transactions_sz;
            TransactionSequence seq ( num_active_transactions );
            for(int64_t i = 0, j = num_active_transactions -1; i < num_active_transactions; i++, j--){
                TransactionImpl* tx = m_transactions[j];
                m_latch.validate_version(version);
                seq.m_transaction_ids[i] = tx->ts_read();
            }

            m_latch.validate_version(version);

            return seq;
        } catch (util::Abort){ /* retry */ }
    } while ( true );
}

uint64_t TransactionList::high_water_mark() const {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Need to be inside an epoch");

    do {
        uint64_t minimum = numeric_limits<uint64_t>::max();

        try {
            uint64_t version = m_latch.read_version();
            int64_t num_active_transactions = m_transactions_sz;
            if(num_active_transactions > 0){
                minimum = m_transactions[0]->ts_read();
            }
            m_latch.validate_version(version);

            return minimum;
        } catch (util::Abort){ /* retry */ }
    } while ( true );
}

} // namespace


