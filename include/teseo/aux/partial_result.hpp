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
#include <utility>

#include "teseo/memstore/key.hpp"

namespace teseo::aux {

class Builder; // forward declaration

/**
 * An ordered chunk of pairs <vertex_id, degree>.
 * Used by the workers to collect the partial degrees of each vertex, before aggregating
 * the final result, the aux vector, in the builder.
 *
 * This class is not thread-safe.
 */
class PartialResult {
    PartialResult(const PartialResult&) = delete;
    PartialResult& operator=(const PartialResult&) = delete;

    Builder* const m_builder; // the final builder to process these partial results
    const uint64_t m_id; // ordered sequence of IDs, e.g. 0, 1, 2,... used by the builder to reorder the sequence of partial results
    const memstore::Key m_from; // the first vertex to insert in the sequence (inclusive), used by the workers to make the partial result
    const memstore::Key m_to; // the last vertex to insert in the sequence (exclusive), used by the workers to make the partial result
    using item_t = std::pair<uint64_t, uint64_t>; // the pair vertex_id, degree
    item_t* m_array; // the actual container for the items
    uint64_t m_size; // the number of items in the container `m_array'
    uint64_t m_capacity; // the max number of items that can be stored in the container `m_array'
    uint64_t m_last; // the index of the last vertex inserted

    // Reset the capacity of the container `m_array'
    void resize(uint64_t new_capacity);

public:
    // Initialise the class
    PartialResult(Builder* builder, uint64_t id, const memstore::Key& from /* incl */, const memstore::Key& to /* excl*/);

    // Destructor
    ~PartialResult();

    // Increment the degree of the given vertex_id
    void incr_degree(uint64_t vertex_id, uint64_t increment);

    // Signal to the builder that this partial result is ready to be consumed
    void done();

    // Observer, get the logical ID of this instance
    uint64_t id() const noexcept;

    // Observer, get the start key for the range of this instance (inclusive)
    const memstore::Key& key_from() const noexcept;

    // Observer, get teh last key for the range of this instance (exclusive)
    const memstore::Key& key_to() const noexcept;

    // Check the current capacity of the array
    uint64_t capacity() const noexcept;

    // Check the current size of the container
    uint64_t size() const noexcept;

    // Retrieve the pair <vertex_id, degree> at the given position
    std::pair<uint64_t, uint64_t> get(uint64_t index) const;
    std::pair<uint64_t, uint64_t> at(uint64_t index) const; // alias
};

} // namespace
