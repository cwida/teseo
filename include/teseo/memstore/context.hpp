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
#include <limits>
#include <ostream>

#include "key.hpp"
#include "segment.hpp"

namespace teseo::transaction {
class TransactionImpl; // forward declaration
} // namespace

namespace teseo::memstore {

class DenseFile; // forward declaration
class Memstore; // forward declaration
class Leaf; // forward declaration
class Segment; // forward declaration
class SparseFile; // forward declaration

/**
 * A context is bookkeeping structure to visit the of the fat tree, knowing at each point which
 * tree instance, leaf and segment is part of the visitor path.
 */
class Context {
private:
    // Shared implementation for #reader_enter and #reader_next
    void reader_enter_impl(Key search_key, Leaf* leaf, int64_t segment_id);

    // Shared implementation for #optimistic_enter and #optimistic_next
    void optimistic_enter_impl(Key search_key, Leaf* leaf, int64_t segment_id);

public:
    transaction::TransactionImpl* m_transaction; // pointer to the current user transaction
    Memstore* m_tree; // pointer to the instance of the fat tree
    Leaf* m_leaf; // pointer to the current visited leaf
    Segment* m_segment; // pointer to the current visited segment
    uint64_t m_version; // the version of the segment accessed by an ``optimistic'' reader

    // Create a new memstore context
    Context(Memstore* tree, transaction::TransactionImpl* transaction = nullptr);

    // Copy & assigment operators
    Context(const Context&) = default;
    Context& operator=(const Context&) = default;

    /**
     * Retrieve the current segment id
     */
    uint64_t segment_id() const;

    /**
     * Retrieve the current sparse file
     */
    SparseFile* sparse_file() const;

    /**
     * Retrieve the current dense file
     */
    DenseFile* dense_file() const;

    /**
     * Retrieve the sparse file for the given leaf & segment id
     */
    static SparseFile* sparse_file(const Leaf* leaf, uint64_t segment_id);

    /**
     * Retrieve the dense file for the given leaf & segment id
     */
    static DenseFile* dense_file(const Leaf* leaf, uint64_t segment_id);

    /**
     * Access the related segment for the given search key as a writer
     */
    void writer_enter(Key search_key);

    /**
     * Release the lock for the associated segment
     */
    void writer_exit();

    /**
     * Access the related segment for the given search key as a reader
     */
    void reader_enter(Key search_key);

    /**
     * Move to the next segment
     */
    void reader_next(Key search_key);

    /**
     * Release the lock for the associated segment
     */
    void reader_exit();

    /**
     * Access the related segment as an optimistic reader
     */
    void optimistic_enter(Key search_key);

    /**
     * Move to the next segment
     */
    void optimistic_next(Key search_key);

    /**
     * Release the related segment as an optimistic reader
     */
    void optimistic_exit();

    /**
     * Reset the content of the context after an optimistic exit
     */
    void optimistic_reset();

    /**
     * Validate the current latch version
     */
    void validate_version();

    /**
     * Validate the current latch version only iff we are using an optimistic reader
     */
    void validate_version_if_present();

    /**
     * Check whether a version has been set
     */
    bool has_version() const;
};

// Print to stdout the content of a Context
std::ostream& operator<<(std::ostream& out, const Context& context);


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
bool Context::has_version() const {
    return m_version != std::numeric_limits<uint64_t>::max();
}

inline
void Context::validate_version(){
    assert(m_version != std::numeric_limits<uint64_t>::max() && "No version set");
    m_segment->m_latch.validate_version(m_version);
}

inline
void Context::validate_version_if_present(){
    if(has_version())
        validate_version();
}



} // namespace
