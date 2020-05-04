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

#include "teseo/transaction/transaction_impl.hpp"

namespace teseo::transaction {

/**
 * This class acts as RAII object and scoped lock to the latch associated to a transaction.
 * It acts as an optimisation as the latch is only acquired if we know there can exist more
 * than one entry pointer to the transaction.
 */
class TransactionWriteLatch {
    util::OptimisticLatch<0>* m_latch;
public:

    /**
     * Acquire a lock to the given transaction
     */
    TransactionWriteLatch(TransactionImpl* transaction);

    /**
     * Release the acquired lock
     */
    ~TransactionWriteLatch();
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
TransactionWriteLatch::TransactionWriteLatch(TransactionImpl* transaction) : m_latch(nullptr) {
    if(transaction->m_shared){
        m_latch = &(transaction->m_latch);
        m_latch->lock();
    }
}

TransactionWriteLatch::~TransactionWriteLatch(){
    if(m_latch != nullptr)
        m_latch->unlock();
}


} // namespace
