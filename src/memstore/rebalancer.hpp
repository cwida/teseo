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

#include "sparse_array.hpp"

namespace teseo::internal::context {
class TransactionSequence; // forward decl.
class Undo; // forward decl.
}

namespace teseo::internal::memstore {

class Rebalancer {
    Rebalancer(const Rebalancer&) = delete;
    Rebalancer& operator=(const Rebalancer&) = delete;
    using Undo = teseo::internal::context::Undo;

    union Element {
        SparseArray::SegmentVertex m_vertex;
        SparseArray::SegmentEdge m_edge;
    };

    SparseArray* m_instance;
    Element* m_elements; // store all elements (nodes/edges) loaded
    SparseArray::SegmentVersion* m_versions; // store all versions loaded
    uint64_t m_capacity; // max capacity of the arrays m_elements and m_versions

    // Input
    int64_t m_load_previous_vertex = -1; // last vertex loaded
    const int64_t m_num_segments_total; // total number of segments loaded
    uint64_t m_size = 0; // total number of elements loaded
    uint64_t m_space_required = 0; // total amount of space required

    // Write cursor
    uint64_t m_write_next_vertex = 0; // index to the last loaded vertex in the array entries
    uint64_t m_write_cursor = 0; // current position in the array elements within the serialiser
    int64_t m_save_space_used = 0;
    int64_t m_num_segments_saved = 0; // number of segments written so far

    // Ensure that the array entries contains enough space to store the elements
    void resize_if_needed();

    void do_load(uint64_t* __restrict c_start, uint64_t* __restrict c_end, uint64_t* __restrict v_start, uint64_t* __restrict v_end);
    void do_save(SparseArray::Chunk* chunk, uint64_t segment_id);

    template<bool is_lhs> void write(int64_t target_len, SparseArray::SegmentMetadata* segment, int64_t* out_space_consumed);
    void write_content(uint64_t* dest_raw, uint64_t src_first_vertex, uint64_t src_start, uint64_t src_end);
    void write_versions(uint64_t* dest_raw, uint64_t src_start, uint64_t src_end, uint64_t backptr);

    template<bool is_lhs> void write_dump(SparseArray::SegmentMetadata* segment);

public:
    // Constructor
    // @param instance sparse array instance
    // @param capacity expected elements to load
    // @param active_transactions current snapshot of active transactions
    Rebalancer(SparseArray* instance, uint64_t num_total_segments, uint64_t capacity = 4096);

    // Destructor
    ~Rebalancer();

    // Load the content from the sparse arrays into the scratchpad
    void load(SparseArray::Chunk* chunk);
    void load(SparseArray::Chunk* chunk, uint64_t segment_id);
    void load(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length);

    // Write the records from the scratchpad to the sparse array
    void save(SparseArray::Chunk* chunk);
    void save(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length);
};


} // namespace

