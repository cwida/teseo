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

    // 24/Dec/2020: An entry in the hash table can contain two elements. We need to align the values to 16 bytes boundaries
    // for the CAS instructions. To achieve this we store 2 elements together in the following layout:
    // - key 1: 8 bytes
    // - key 2: 8 bytes
    // - value 1: 16 bytes
    // - value 2: 16 bytes
    // This allows to avoid losing 8 bytes on each entry due to padding and decreases the memory footprint of the hash table by 25%.
    struct Entry {
        uint64_t m_key1; // vertex id for the first element
        uint64_t m_key2; // vertex id for the second element
        CompressedDirectPointer m_value1; // payload attached to the first element
        CompressedDirectPointer m_value2; // payload attached to the second element
    };
    Entry* m_hashtables[context::StaticConfiguration::numa_num_nodes]; // one per NUMA node, aligned to 16 bytes
    uint64_t m_num_entries; // total number of entries in the hash table.
    int64_t m_num_elts; // number of elements inserted by the merger
    std::atomic<int64_t> m_num_tombstones; // number of elements removed since the last migration (grow)
    uint64_t m_latch; // mutual protection on grow/migration
    util::CircularArray<std::promise<void>*> m_queue; // waiting list
    void* m_allocations[context::StaticConfiguration::numa_num_nodes]; // one per NUMA node, aligned to whatever numa::alloc() returns
    mutable CompressedDirectPointer m_vertex1 alignas(16); // special entry to store the vertex with ID == 1. We use the same ID for the tombstones

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

    // Allocate a hash table with the given capacity, aligned to a 16 byte boundary.
    // @param total number of entries in the hash table
    // @param numa_node the ID of the socket to allocate the hash table
    // @return the first pointer is the pointer for the allocation, the second is the start of the hash table
    static std::pair<void*, Entry*> allocate_hash_table(uint64_t num_entries, int numa_node);

    // Atomically get the value of a CDPTR
    static CompressedDirectPointer load(CompressedDirectPointer& variable);

    // Atomically set the value of a CDPTR
    static void store(CompressedDirectPointer& variable, CompressedDirectPointer value);

    // Compute the hash function for the given vertex
    static uint64_t hashf(uint64_t vertex_id, uint64_t capacity);

    // Get the pointer for the vertex with ID == 1
    DirectPointer get_vertex1() const noexcept;

    // Insert or update the entry for the vertex with ID 1
    void upsert_vertex1(const DirectPointer& pointer) noexcept;

    // Update the entry (if already set) for the vertex with ID 1
    bool update_vertex1(const DirectPointer& pointer) noexcept;

    // Check whether the entry for vertex with ID 1 is set
    bool has_vertex1() const noexcept;

    // Remove the entry for the vertex with ID 1
    void remove_vertex1() noexcept;

    // Helper method for #clear, reset a given element in the hash table
    static void reset_elt(uint64_t& /* in/out */ key, CompressedDirectPointer& /* in/out */ value);

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
