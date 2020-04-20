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

#include <ostream>

#include "key.hpp"

namespace teseo::transaction {
class TransactionImpl; // forward declaration
} // namespace

namespace teseo::memstore {

class Memstore; // forward declaration
class Leaf; // forward declaration
class Segment; // forward declaration
class SparseFile; // forward declaration

/**
 * A context is bookkeeping structure to visit the of the fat tree, knowing at each point which
 * tree instance, leaf and segment is part of the visitor path.
 */
class Context {
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;

public:
    transaction::TransactionImpl* m_transaction; // pointer to the current user transaction
    Memstore* m_tree; // pointer to the instance of the fat tree
    Leaf* m_leaf; // pointer to the current visited leaf
    Segment* m_segment; // pointer to the current visited segment

    // Create a new memstore context
    Context(Memstore* tree, transaction::TransactionImpl* transaction = nullptr);

    /**
     * Retrieve the current segment id
     */
    uint64_t segment_id() const;

    /**
     * Retrieve the current sparse file
     */
    SparseFile* sparse_file() const;

    /**
     * Retrieve the sparse file for the given leaf & segment id
     */
    static SparseFile* sparse_file(const Leaf* leaf, uint64_t segment_id);

    /**
     * Access the related segment for the given search key as a writer
     */
    void writer_enter(Key search_key);

    /**
     * Release the lock for the associated segment
     */
    void writer_exit();


};

// Print to stdout the content of a Context
std::ostream& operator<<(std::ostream& out, const Context& context);


} // namespace
