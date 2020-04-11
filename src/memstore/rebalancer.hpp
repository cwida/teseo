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

class Rebalancer; // forward declaration
class RebalancerScratchPad; // forward declaration
struct RebalancingContext;

/**
 * Spread the content in the sparse array
 */
class Rebalancer {
    Rebalancer(const Rebalancer&) = delete;
    Rebalancer& operator=(const Rebalancer&) = delete;
    using Undo = teseo::internal::context::Undo;

    SparseArray* m_instance;
    RebalancerScratchPad& m_scratchpad; // used to load all the elements from the sparse array

    // Input
    const int64_t m_num_segments_input; // total number of segments loaded
    const int64_t m_num_segments_output; // total number of segments saved
    uint64_t m_space_required = 0; // total amount of space required

    // Write cursor
    uint64_t m_write_next_vertex = 0; // index to the last loaded vertex in the array entries
    uint64_t m_write_cursor = 0; // current position in the array elements within the serialiser
    int64_t m_save_space_used = 0;
    int64_t m_num_segments_saved = 0; // number of segments written so far

    void do_load(uint64_t* __restrict c_start, uint64_t* __restrict c_end, uint64_t* __restrict v_start, uint64_t* __restrict v_end);
    void do_save(SparseArray::Chunk* chunk, uint64_t segment_id);

    template<bool is_lhs> void write(int64_t target_len, SparseArray::SegmentMetadata* segment, int64_t* out_space_consumed);
    void write_content(uint64_t* dest_raw, uint64_t src_first_vertex, uint64_t src_start, uint64_t src_end);
    void write_versions(uint64_t* dest_raw, uint64_t src_start, uint64_t src_end, uint64_t backptr);

    template<bool is_lhs> void write_dump(SparseArray::SegmentMetadata* segment);
public:
    // Constructor
    // @param instance sparse array instance
    Rebalancer(SparseArray* instance, int64_t num_segments_input, int64_t num_segments_output, RebalancerScratchPad& scratchpad);

    // Destructor
    ~Rebalancer();

    // Load the content from the sparse arrays into the scratchpad
    void load(SparseArray::Chunk* chunk);
    void load(SparseArray::Chunk* chunk, uint64_t segment_id);
    void load(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length);

    // Write the records from the scratchpad to the sparse array
    void save(SparseArray::Chunk* chunk);
    void save(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length);

    // Check the cursors are at the expected positions
    void validate();
};


/**
 * An fixed size array, used to temporarily load the content of a section of the sparse array.
 */
class RebalancerScratchPad {
    RebalancerScratchPad(const RebalancerScratchPad&) = delete;
    RebalancerScratchPad& operator=(const RebalancerScratchPad&) = delete;

    const uint64_t m_capacity; // total number of elements that can be stored in the scratchpad
    uint64_t m_size = 0; // current size
    uint64_t m_last_vertex_loaded; // the index of the last vertex loaded

    union {
        SparseArray::SegmentVertex m_vertex;
        SparseArray::SegmentEdge m_edge;
    }* m_elements = nullptr;
    SparseArray::SegmentVersion* m_versions = nullptr; // array with the versions loaded


public:
    RebalancerScratchPad(uint64_t capacity);

    ~RebalancerScratchPad();

    /**
     * Retrieve the current number of elements loaded in the scratch pad
     */
    uint64_t size() const;

    /**
     * Reset the size of the scratch pad
     */
    void clear();

    /**
     * Retrieve the capacity of the scratch pad
     */
    uint64_t capacity() const;

    /**
     * Retrieve the element at the given position
     */
    SparseArray::SegmentVertex* get_vertex(uint64_t position) const;
    SparseArray::SegmentEdge* get_edge(uint64_t position) const;

    /**
     * Retrieve & unset the version for the element at the given position.
     */
    SparseArray::SegmentVersion move_version(uint64_t position);

    /**
     * Check whether the element at the given position has attached a version
     */
    bool has_version(uint64_t position) const;

    /**
     * Retrieve the last vertex loaded
     */
    SparseArray::SegmentVertex* get_last_vertex() const;

    /**
     * Check whether the index for the last vertex loaded is set
     */
    bool has_last_vertex() const;

    /**
     * Load a vertex into the scratchpad
     */
    void load_vertex(SparseArray::SegmentVertex* vertex, SparseArray::SegmentVersion* version);

    /**
     * Load an edge into the scratchpad
     */
    void load_edge(SparseArray::SegmentEdge* edge, SparseArray::SegmentVersion* version);

    /**
     * Unload the last vertex
     */
    void unload_last_vertex();

    /**
     * Set the version for the record at the given position
     */
    void set_version(uint64_t position, SparseArray::SegmentVersion* version);

    /**
     * Unset the version for the record at the given position
     */
    void unset_version(uint64_t position);
};

/**
 * Used to coordinate multiple rebalancers operating on the same chunk
 */
struct RebalancingContext {
    bool m_can_continue = true; // whether this rebalancer is required to continue the rebalancing
    bool m_can_be_stopped = false; // whether this rebalancer is already executing the spread/split operation
    int64_t m_gate_start = 0; // the first gate to rebalance (inclusive)
    int64_t m_gate_end = 0; // the last gate to rebalance (exclusive)
    int64_t m_space_filled = 0; // total amount of words in use
    std::vector<std::promise<void>*> m_threads2wait; // the threads we need to wait operating before we can proceed with the spread/split
};

} // namespace

