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

class PayloadIterator; // Forward declaration

/**
 * A payload file consists of a linked list of blocks reserved to store the payload or the weight
 * attached to the edges.  We store the weights separately from the edges to reduce the working set
 * when fetching only the edges, without the weights, in a scan for a graph algorithms.
 *
 * A payload file is associated to a single sparse file containing the related edges.
 * The sorted order in the sparse file is the same in the payload file, and therefore, the weight
 * related to an edge can be identified by its position in the file.
 *
 * The first block of the payload file is always physically stored in the fat tree. Subsequent blocks are
 * created when a block becomes full and it is split in a procedure similar to B+ trees. All blocks, expect
 * the first, are allocated in the heap.
 *
 * The layout of a block is similar to a sparse file. The weights are stored in both the left and the
 * right side of the block, with empty space left in the middle.
 *
 * This class is not thread-safe. Writers should acquire an xlock in the associated segment/sparse
 * file before making any alteration to the payload file.
 */
class PayloadFile {
    PayloadFile(const PayloadFile&) = delete;
    PayloadFile& operator=(const PayloadFile&) = delete;
    friend class PayloadIterator;

    const uint16_t m_capacity; // total capacity of the block, as multiple of 8 bytes
    uint16_t m_cardinality; // total number of elements of weights in the block
    uint16_t m_empty_lhs; // the left hand side border, where the empty section starts
    uint16_t m_empty_rhs; // the right hand side border, where the empty section starts
    PayloadFile* m_next; // next block, if present, of the linked list

    // Get the start position of the data
    double* data();
    const double* data() const;

    // Split this block in two new blocks, distributing half of the existing elements in the new block
    void split();

    // Merge this block with the next block
    void merge();

    // Check whether the block is full
    bool is_full() const;

    // Get the capacity of this block
    uint64_t capacity() const;

    // Get the cardinality of this block
    uint64_t cardinality() const;

    // Insert a new element, in the left or right hand side of the block
    // The parameter relative_position refers to the start of the LHS or the RHS
    void do_insert_lhs(uint64_t relative_position, double element);
    void do_insert_rhs(uint64_t relative_position, double element);

    // Remove the element at the given position, in the left or right hand side of the block
    // The parameter relative_position refers to the start of the LHS or the RHS
    void do_remove_lhs(uint64_t relative_position);
    void do_remove_rhs(uint64_t relative_position);

public:
    // Initialise the capacity of this block
    PayloadFile(uint64_t capacity);

    // Destructor
    ~PayloadFile();

    // Insert an element in the file
    void insert(uint64_t position, double payload);

    // Remove an element from the file
    void remove(uint64_t position);

    // Remove all elements from the payload file.
    void clear();

    // Get the element at the given position
    double get(uint64_t position) const;

    // An iterator to fetch the elements from the file one by one. The iterator is not thread-safe
    // and the content of the file should not be modified while the iterator is active.
    PayloadIterator iterator() const;

    // Dump to stdout the content of the file, for debugging purposes
    void dump() const;
};

// Create a new payload block with the given capacity, as a multiple of the weight (8 bytes)
PayloadFile* create_payload_block(uint64_t capacity);

// Create a new payload block with the capacity set in the static configuration.
PayloadFile* create_payload_block();

// Deallocate a payload block
void destroy_payload_block(PayloadFile* block);

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
uint64_t PayloadFile::capacity() const {
    return m_capacity;
}

inline
uint64_t PayloadFile::cardinality() const {
    return m_cardinality;
}

inline
double* PayloadFile::data() {
    return reinterpret_cast<double*>(this +1);
}

inline
const double* PayloadFile::data() const {
    return reinterpret_cast<const double*>(this +1);
}

} // namespace
