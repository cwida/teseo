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
#include <vector>

#include "../memstore-old/key.hpp"
#include "../memstore-old/sparse_array.hpp"

namespace teseo::internal::memstore {

class Gate; // forward declaration

/**
 * Wrapper class to remove a vertex and its attached edges from a sparse array
 */
class RemoveVertex {
    SparseArray* const m_instance; // the attached Sparse Array instance
    SparseArray::Transaction* const m_transaction; // user transaction
    const uint64_t m_vertex_id; // the vertex to remove
    std::vector<uint64_t>* m_outgoing_edges; // the list of outgoing edges removed
    bool m_owns_outgoing_edges; // whether the memory of `m_outgoing_edges' has been allocated by this instance

    // Descending iterator
    SparseArray::Chunk* m_chunk = nullptr;
    Gate* m_gate = nullptr;
    SparseArray::SegmentMetadata* m_segment = nullptr;
    bool m_is_lhs = false;
    Key m_key;

    // Number of items removed so far
    uint64_t m_num_items_removed = 0;

    // Scrachpad
    uint64_t* m_scratchpad = nullptr;
    uint64_t m_scratchpad_pos;

    bool m_rebalance = false; // Signal above whether to rebalance
    bool m_unlock_required = false; // Whether we need a further step to unlock the vertices

    // Locking phase
    void lock();
    void lock_gate();
    void lock_segment();
    void record_edge_removed(SparseArray::SegmentEdge* edge);
    void copy_scratchpad(int64_t bookmark);

    // Unlocking step
    void unlock();
    void unlock_gate();
    void unlock_segment();

public:
    /**
     * Initialise the object
     */
    RemoveVertex(SparseArray* instance, SparseArray::Transaction* transaction, uint64_t vertex_id, std::vector<uint64_t>* out_outgoing_edges);

    /**
     * Destructor
     */
    ~RemoveVertex();

    /**
     * Execute the operation, that is remove the `vertex_id' and its attached edges.
     * Return the number of edges removed.
     */
    uint64_t operator()();
};

} // namespace
