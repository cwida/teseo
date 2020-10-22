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
#include <cstdint>
#include <future>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/direct_pointer.hpp"
#include "teseo/util/circular_array.hpp"

namespace teseo::memstore {

/**
 * A secondary index that maps each (real) vertex ID to its leaf/segment ID/segment's position.
 * The index is updated asynchronously by rebalancers and the information contained may be outdated.
 * Users of the index need to validate the information fetched by checking whether the retrieved
 * leaf still exists, the key is in the range of the fence keys of the accessed segment, and the
 * version of the segment still matches the version retrieved from the index.
 */
class VertexTable {
    VertexTable(const VertexTable& ) = delete;
    VertexTable& operator=(const VertexTable&) = delete;

    // A single element in the hash table
    struct Element {
        uint64_t m_key; // vertex id
        DirectPointer m_value;
    };
    Element* m_hashtables[context::StaticConfiguration::numa_num_nodes]; // one per NUMA node
    int64_t m_capacity; // current capacity of each hash table
    int64_t m_num_elts; // number of elements inserted by the merger
    std::atomic<int64_t> m_num_tombstones; // number of elements removed since the last migration (grow)
    uint64_t m_latch; // mutual protection on grow/migration
    util::CircularArray<std::promise<void>*> m_queue; // waiting list

    // latch flags
    static constexpr uint64_t MASK_VERSION = (1ull << 56) -1;
    static constexpr uint64_t MASK_XLOCK = 1ull << 56;

    // An empty slot in the table
    static constexpr uint64_t EMPTY = 0;

    // A tombstone, an element removed from the hash table
    static constexpr uint64_t TOMBSTONE = 1;

    // Retrieve the current fill factor of the hash table
    double fill_factor() const;

    // Resize the hash table to a proper capacity
    void resize();
    void do_resize();

    // Acquire an xlock to the latch
    void xlock();

    // Release the xlock to the latch
    void xunlock();

    // Wait for a resize to finish
    void wait();

    // Compute the hash function for the given vertex
    static int64_t hashf(uint64_t vertex_id, uint64_t capacity);

public:
    /**
     * Initialise the data structure
     */
    VertexTable();

    /**
     * Destructor
     */
    ~VertexTable();

    /**
     * Insert (or update) the given entry in the hash table. This method can only be
     * invoked by the Merger thread
     */
    void upsert(uint64_t vertex_id, const DirectPointer& pointer) noexcept;

    /**
     * Update a given entry in the hash table. If the key is not present in the hash
     * table, do nothing.
     * This method should only be invoked by service thread asynchronously.
     *
     * @return true if the update has been performed, that is the vertex_id already exists
     *         in the hash table, false otherwise
     */
    bool update(uint64_t vertex_id, const DirectPointer& pointer) noexcept;

    /**
     * Set a tombstone for the given entry in the table.
     */
    void remove(uint64_t vertex_id) noexcept;

    /**
     * Retrieve the pointer for the given vertex. It may be a nullptr if the vertex is
     * not registered in the hash table
     */
    DirectPointer get(uint64_t vertex_id, uint64_t numa_node) const noexcept;

    /**
     * Explicitly remove all elements of the vertex table
     */
    void clear();

    /**
     * Dump the content of the hash tables to stdout, for debugging purposes
     */
    void dump() const;
};

} // namespace
