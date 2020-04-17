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

#include <cassert>
#include <cinttypes>
#include <limits>
#include "transaction_sequence.hpp"

namespace teseo::transaction {

/**
 * A forward iterator over a transaction sequence
 */
class TransactionSequenceForwardIterator {
    const TransactionSequence* m_sequence;
    uint64_t m_position = 0;

public:
    /**
     * Init the iterator
     */
    TransactionSequenceForwardIterator(const TransactionSequence* sequence);

    /**
     * Whether the iterator has been depleted
     */
    bool done() const;

    /**
     * Retrieve the current key in the sequence, or INT_MAX if the iterator has been depleted
     */
    uint64_t key() const;

    /**
     * Fetch the next key in the sequence
     */
    void next();
};


/**
 * A backward iterator over a transaction sequence
 */
class TransactionSequenceBackwardsIterator {
    const TransactionSequence* m_sequence;
    int64_t m_position = -1;

public:
    /**
     * Init the iterator
     */
    TransactionSequenceBackwardsIterator(const TransactionSequence* sequence);

    /**
     * Whether the iterator has been depleted
     */
    bool done() const;

    /**
     * Retrieve the current key in the sequence, or INT_MIN if the iterator has been depleted
     */
    uint64_t key() const;

    /**
     * Fetch the previous key in the sequence
     */
    void next();
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
TransactionSequenceForwardIterator::TransactionSequenceForwardIterator(const TransactionSequence* sequence) : m_sequence(sequence) {
    assert(sequence != nullptr);
}

inline
bool TransactionSequenceForwardIterator::done() const {
    return m_position >= m_sequence->size();
}

inline
uint64_t TransactionSequenceForwardIterator::key() const {
    return done() ? std::numeric_limits<uint64_t>::max() : m_sequence->operator [](m_position);
}

inline
void TransactionSequenceForwardIterator::next() {
    m_position++;
}

inline
TransactionSequenceBackwardsIterator::TransactionSequenceBackwardsIterator(const TransactionSequence* sequence) : m_sequence(sequence){
    if(sequence->size() > 0){
        m_position = sequence->size() -1;
    }
}

inline
bool TransactionSequenceBackwardsIterator::done() const {
    return m_position < 0;
}

inline
uint64_t TransactionSequenceBackwardsIterator::key() const {
    return done() ? std::numeric_limits<uint64_t>::min() : m_sequence->operator [](m_position);
}

inline
void TransactionSequenceBackwardsIterator::next() {
    m_position--;
}

} // namespace
