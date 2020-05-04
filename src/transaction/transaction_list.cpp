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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <emmintrin.h>
#include <iostream>

#include "teseo/context/global_context.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/error.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::transaction {

TransactionList::TransactionList() {
    memset(m_transactions, '\0', sizeof(m_transactions));
}

TransactionList::~TransactionList() {

}

uint64_t TransactionList::insert(context::GlobalContext* gcntxt, TransactionImpl* transaction) {
    bool success = false;
    uint64_t transaction_id { 0 };

    uint64_t value = m_version ++ ;// lock
    assert(m_version % 2 == 1 && "Odd value => locked");
    assert(value % 2 == 0 && "Expected the even value before the lock was taken");

//    // from time to time we rebuild the value of m_transaction_sz
//    if((value % /* arbitrary value */ (1ull<<10)) == 0){
//        while(m_transactions_sz > 0 && m_transactions[m_transactions_sz -1] == nullptr) m_transactions_sz--;
//    }


    uint64_t slot_id = 0;
    while(slot_id < m_transactions_sz && !success){
        success = (m_transactions[slot_id] == nullptr);
        slot_id += (!success);
    }

    if(!success && m_transactions_sz < m_transactions_capacity){
        slot_id = m_transactions_sz;
        m_transactions_sz++;
        success = true;
    }

    if(success){
        // we have to assign here the transaction ID, to avoid a potential data race, if done before:
        // Thread #1 starts a new transaction and creates a new transaction ID, e.g. 7
        // Thread #2 executes #active_transactions() and read the next transaction ID: 8
        // If Thread #2 completes the invocation before Thread #1 it will think that the high water mark is 8, rather than 7
        m_transactions[slot_id] = transaction;
        transaction_id = gcntxt->next_transaction_id();
    }

    m_version = value +2; // unlock

    if(!success){ RAISE(LogicalError, "There are too many active transactions in this thread"); }

    return transaction_id;
}

bool TransactionList::remove(TransactionImpl* transaction){
    assert(transaction != nullptr && "Null pointer");

    int64_t num_active_transactions = m_transactions_sz;
    int64_t i = 0;
    while(i < num_active_transactions && m_transactions[i] != transaction){ i ++; }
    bool found = !(i == num_active_transactions);

    if(found){
        m_transactions[i] = nullptr;
    }

    return found;
}

TransactionSequence TransactionList::snapshot(uint64_t max_transaction_id) const {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() &&
            "Need to be inside an epoch, the transaction read could have been released to the GC");

    TransactionSequence seq ( m_transactions_capacity );
    uint64_t version0 {0}, version1 {0};

    do {
        // lock (reader)
        version0 = m_version;
        while(version0 % 2 == 1){ _mm_pause(); version0 = m_version; }

        int64_t num_active_transactions = m_transactions_sz;
        int64_t size = 0;
        for(int64_t i = 0; i < num_active_transactions; i++){
            TransactionImpl* tx = m_transactions[i];
            if(tx != nullptr && tx->ts_read() < max_transaction_id){
                seq.m_transaction_ids[size++] = tx->ts_read();
            }
        }
        seq.m_num_transactions = size;

        // unlock (reader)
        version1 = m_version;
    } while(version0 != version1);

    std::sort(seq.m_transaction_ids, seq.m_transaction_ids + seq.m_num_transactions, std::greater<uint64_t>());

    return seq;
}

uint64_t TransactionList::high_water_mark() const {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() &&
            "Need to be inside an epoch, the transaction read could have been released to the GC");

    uint64_t version0 {0}, version1 {0}, minimum {0};

    do {
        minimum = numeric_limits<uint64_t>::max();

        // lock (reader)
        version0 = m_version;
        while(version0 % 2 == 1){ _mm_pause(); version0 = m_version; }

        int num_active_transactions = m_transactions_sz;
        for(int i = 0; i < num_active_transactions; i++){
            if(m_transactions[i] != nullptr && m_transactions[i]->ts_read() < minimum){
                minimum = m_transactions[i]->ts_read();
            }
        }

        // unlock (reader)
        version1 = m_version;
    } while(version0 != version1);

    return minimum;
}

} // namespace


