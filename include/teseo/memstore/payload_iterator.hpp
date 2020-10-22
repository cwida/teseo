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

namespace teseo::memstore {

class PayloadFile; // forward declaration

/**
 * An iterator to fetch, one by one, all elements stored in a payload file.
 *
 * The class is not thread-safe.
 */
class PayloadIterator {
    const PayloadFile* m_block; // current block
    const double* m_start; // current section of the block, lhs or rhs, being retrieved
    uint32_t m_position; // relative position of the cursor, in [0, m_length)
    uint32_t m_length; // number of elements in the current section

public:
    // Initialise a new iterator for the given file
    PayloadIterator(const PayloadFile* file);

    // Return the current element from the cursor & move the cursor ahead of one position
    double next();

    // Check whether there is an element next to the current position that can be fetched
    bool has_next() const;

    // Move the cursor forward of N elements
    void skip(uint64_t num_elements);
};

} // namespace
