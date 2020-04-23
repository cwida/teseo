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

#include "teseo/memstore/data_item.hpp"


namespace teseo::rebalance {


/**
 * An fixed size array, used to temporarily load the content of the section of the Memstore to be rebalanced.
 */
class ScratchPad {
    ScratchPad(const ScratchPad&) = delete;
    ScratchPad& operator=(const ScratchPad&) = delete;

    uint64_t m_capacity; // total number of elements that can be stored in the scratchpad
    uint64_t m_size = 0; // current size
    uint64_t m_last_vertex_loaded; // the index of the last vertex loaded

    union {
        memstore::Vertex m_vertex;
        memstore::Edge m_edge;
    }* m_elements = nullptr;
    memstore::Version* m_versions = nullptr; // array with the versions loaded


public:

    /**
     * Create a new instance with the given initial capacity
     */
    ScratchPad(uint64_t capacity);

    /**
     * Destructor
     */
    ~ScratchPad();

    /**
     * Retrieve the current number of elements loaded in the scratch pad
     */
    uint64_t size() const;

    /**
     * Retrieve the capacity of the scratch pad
     */
    uint64_t capacity() const;

    /**
     * Ensure the capacity of the scratch pad is larger than N. In case it is not,
     * resize the underlying buffers.
     */
    void ensure_capacity(uint64_t cardinality);

    /**
     * Reset the size of the scratch pad
     */
    void clear();

    /**
     * Retrieve the element at the given position
     */
    memstore::Vertex* get_vertex(uint64_t position) const;
    memstore::Edge* get_edge(uint64_t position) const;
    memstore::Version* get_version(uint64_t position) const;

    /**
     * Retrieve & unset the version for the element at the given position.
     */
    memstore::Version move_version(uint64_t position);

    /**
     * Check whether the element at the given position has attached a version
     */
    bool has_version(uint64_t position) const;

    /**
     * Retrieve the last vertex loaded
     */
    memstore::Vertex* get_last_vertex() const;

    /**
     * Check whether the index for the last vertex loaded is set
     */
    bool has_last_vertex() const;

    /**
     * Load a vertex into the scratchpad
     */
    void load_vertex(memstore::Vertex* vertex, memstore::Version* version);

    /**
     * Load an edge into the scratchpad
     */
    void load_edge(memstore::Edge* edge, memstore::Version* version);

    /**
     * Unload the last vertex
     */
    void unload_last_vertex();

    /**
     * Shift an element back of the given number of positions
     */
    void shift_back(uint64_t position, uint64_t shift);

    /**
     * Reset the size of the scratchpads
     */
    void set_size(uint64_t new_size);

    /**
     * Check the slot at the given position contains a valid element
     */
    bool has_element(uint64_t position) const;

    /**
     * Unset the element at the given position
     */
    void unset_element(uint64_t position);

    /**
     * Set the version for the record at the given position
     */
    void set_version(uint64_t position, memstore::Version* version);

    /**
     * Delete (remove) the version for the record at the given position
     */
    void unset_version(uint64_t position);
};


}

