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
#include <cinttypes>
#include <future>
#include <vector>

#include "key.hpp"
#include "latch.hpp"

namespace teseo::internal::context {
class ThreadContext; // forward declaration
class Undo; // forward declaration
}

namespace teseo::internal::memstore {

class Gate; // forward declaration
class Index; // forward declaration
class Rebalancer; // forward declaration

class SparseArray {
    friend class Rebalancer;
    SparseArray(const SparseArray&) = delete;
    SparseArray& operator=(const SparseArray&) = delete;

    const uint64_t m_num_gates_per_chunk; // how many gates each chunk contains
    const uint64_t m_num_segments_per_lock; // how many segments are stored for each lock. Each gate actually store 2*spl separator keys, one for the lhs, one for the rhs of each segment
    const uint64_t m_num_qwords_per_segment; // how many qwords (8 bytes) can be stored inside a segment. That is the amount of space that compose a segment
    Index* m_index; // primary & sparse index to the created chunks/gates

    /**
     * A single entry retrieved from the index
     */
    struct IndexEntry {
        uint64_t m_gate_id:8;
        uint64_t m_chunk_id:56;
    };

    // The metadata associated to each chunk
    struct Chunk {
        Latch m_latch; // acquired when a thread needs to rebalance more segments than those contained in a single gate
    };

    // The metadata associated to each segment
    struct SegmentMetadata {
        uint16_t m_delta1_start; // the offset where the changes for the LHS of the segment start, in qwords
        uint16_t m_delta2_start; // the offset where the changes for the RHS of the segment start, in qwords
        uint16_t m_empty1_start; // the offset where the empty space for the LHS of the segment start, in qwords
        uint16_t m_empty2_start; // the offset where the empty space for the RHS of the segment start, in qwords
    };

    /**
     * A static vertex entry in the segment
     */
    struct SegmentStaticVertex {
        uint64_t m_vertex_id; // the id of the vertex
        uint64_t m_count; // number of static edges following the static vertex
    };

    /**
     * A static edge entry in the segment
     */
    struct SegmentStaticEdge {
        uint64_t m_destination; // the destination id of the given edge
        double m_weight; // the weight associated to the edge
    };

    /**
     * The header/metadata associated to each entry in the delta portion of the segment
     */
    struct SegmentDeltaMetadata {
        uint64_t m_insdel:1; // 0 = insert, 1 = delete
        uint64_t m_entity:1; // 0 = vertex, 1 = edge
        uint64_t m_version:62; // ptr to the transaction version
    };

    /**
     * An entry related to a vertex in the delta portion of the segment
     */
    struct SegmentDeltaVertex : public SegmentDeltaMetadata {
        uint64_t m_vertex_id; // the id of the vertex
    };

    /**
     * An entry related to an edge in the delta of portion of the segment
     */
    struct SegmentDeltaEdge : public SegmentDeltaMetadata {
        uint64_t m_source; // the source vertex of the edge
        uint64_t m_destination; // the destination vertex of the edge
        double m_weight; // the weight associated to the given edge (ignore, if this is a delete)
    };

    // The data associated to an update
    struct Update {
        enum { Vertex, Edge } m_entry_type;
        enum { Insert, Remove } m_update_type;
        uint64_t m_source = 0;
        uint64_t m_destination = 0;
        double m_weight = 0.0;
    };
    friend std::ostream& operator<<(std::ostream& out, const SparseArray::Update& update);

    // Retrieve the number of gates in each chunk
    uint64_t get_num_gates_per_chunk() const;

    // Retrieve the number of segments covered by each gate/lock
    uint64_t get_num_segments_per_lock() const;

    // Retrieve the total number of segments in a chunk
    uint64_t get_num_segments_per_chunk() const;

    // Retrieve the number of qwords (space available) in each segment
    uint64_t get_num_qwords_per_segment() const;

    // Retrieve the number of qwords used by each gate
    uint64_t get_num_qwords_per_gate() const;

    // Get the search key associated to an update
    static Key get_key(const Update& u);

    // Retrieve the chunk from the given IndexEntry
    Chunk* get_chunk(const IndexEntry entry);
    const Chunk* get_chunk(const IndexEntry entry) const;

    // Retrieve the gate with the given ID
    Gate* get_gate(const Chunk* chunk, uint64_t id);
    const Gate* get_gate(const Chunk* chunk, uint64_t id) const;

    // Retrieve the metadata associated to a segment
    SegmentMetadata* get_segment_metadata(const Chunk* chunk, uint64_t segment_id);
    const SegmentMetadata* get_segment_metadata(const Chunk* chunk, uint64_t segment_id) const;

    // Retrieve the start/end pointers of a segment
    uint64_t* get_segment_lhs_static_start(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_lhs_static_start(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_lhs_static_end(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_lhs_static_end(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_lhs_delta_start(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_lhs_delta_start(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_lhs_delta_end(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_lhs_delta_end(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_rhs_static_start(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_rhs_static_start(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_rhs_static_end(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_rhs_static_end(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_rhs_delta_start(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_rhs_delta_start(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_rhs_delta_end(const Chunk* chunk, uint64_t segment_id);
    const uint64_t* get_segment_rhs_delta_end(const Chunk* chunk, uint64_t segment_id) const;
    uint64_t* get_segment_static_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs);
    const uint64_t* get_segment_static_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const;
    uint64_t* get_segment_static_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs);
    const uint64_t* get_segment_static_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const;
    uint64_t* get_segment_delta_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs);
    const uint64_t* get_segment_delta_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const;
    uint64_t* get_segment_delta_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs);
    const uint64_t* get_segment_delta_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const;

    // The height of the calibrator tree in a chunk
    int64_t get_cb_height_per_chunk() const;

    // Retrieve the amount of free space in the given segment, in qwords
    uint64_t get_segment_free_space(const Chunk* chunk, uint64_t segment_id) const;

    // Retrieve the amount of used space in the given segment, in qwords
    uint64_t get_segment_used_space(const Chunk* chunk, uint64_t segment_id) const;

    // Retrieve the amount of free space in the segments of the given gate, in qwords
    uint64_t get_gate_free_space(const Chunk* chunk, uint64_t gate_id) const;
    uint64_t get_gate_free_space(const Chunk* chunk, const Gate* gate) const;

    // Retrieve the amount of used space in the segments of the given gate, in qwords
    uint64_t get_gate_used_space(const Chunk* chunk, uint64_t gate_id) const;
    uint64_t get_gate_used_space(const Chunk* chunk, const Gate* gate) const;

    // Check whether the record refers to an insertion or a removal
    static bool is_insert(const SegmentDeltaMetadata* metadata);
    static bool is_remove(const SegmentDeltaMetadata* metadata);
    static bool is_insert(const Update& update);
    static bool is_remove(const Update& update);

    // Check whether the record refers to a vertex or an edge
    static bool is_vertex(const SegmentDeltaMetadata* metadata);
    static bool is_edge(const SegmentDeltaMetadata* metadata);
    static bool is_vertex(const Update& metadata);
    static bool is_edge(const Update& metadata);

    // Reset the type of the record
    static void set_vertex(SegmentDeltaMetadata* metadata);
    static void set_edge(SegmentDeltaMetadata* metadata);
    static void set_type(SegmentDeltaMetadata* metadata, bool true_if_insert_or_false_if_remove);
    static void set_undo(SegmentDeltaMetadata* metadata, teseo::internal::context::Undo* undo);
    static void reset_header(SegmentDeltaMetadata* metadata, const Update& update);

    // Retrieve the vertex/edge from the static portion
    static SegmentStaticVertex* get_static_vertex(uint64_t* ptr);
    static SegmentStaticEdge* get_static_edge(uint64_t* ptr);

    // Retrieve the record's metadata associate to the given ptr
    static SegmentDeltaMetadata* get_delta_header(uint64_t* ptr);

    // Retrieve the vertex record from the ptr in the delta portion of the segment
    static SegmentDeltaVertex* get_delta_vertex(uint64_t* ptr);

    // Retrieve the edge record from the ptr in the delta portion of the segment
    static SegmentDeltaEdge* get_delta_edge(uint64_t* ptr);

    // Retrieve the UndoRecord associated to a delta record
    static teseo::internal::context::Undo* get_delta_undo(uint64_t* ptr);
    static teseo::internal::context::Undo* get_delta_undo(SegmentDeltaMetadata* ptr);

    // Retrieve the UndoRecord associated to a delta record

    // Allocate a new chunk of the sparse array
    Chunk* allocate_chunk();

    // Release a previously allocated chunk
    void free_chunk(Chunk* chunk);

    // Actual constructor
    struct InitSparseArrayInfo{ uint64_t m_num_gates_per_chunk; uint64_t m_num_segments_per_lock; uint64_t m_num_qwords_per_segment; };
    static InitSparseArrayInfo compute_alloc_params(uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint);
    SparseArray(InitSparseArrayInfo init);

    // Perform an update (write), by adding/removing a new vertex/edge in the sparse array
    void write(Update update);

    // Retrieve the Chunk and the Gate where to perform the insertion/deletion
    std::pair<Chunk*, Gate*> writer_on_entry(const Update& update);

    // Context switch on this gate & release the lock
    template<typename Lock> void writer_wait(Gate& gate, Lock& lock);

    // Attempt to perform an update inside the given gate. Return <true> in case of success, <false> otherwise.
    bool do_write_gate(Chunk* chunk, Gate* gate, Update& update);

    // Attempt to perform an update into the given segment. Return <true> in case of success, <false> otherwise
    bool do_write_segment(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update);
    void do_write_segment_vertex(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update);
    void do_write_segment_edge(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update);

    // Attempt to rebalance a gate (local rebalance)
    bool rebalance_gate(Chunk* chunk, Gate* gate, uint64_t segment_id);

    // Attempt to rebalance a chunk (global rebalance)
    void rebalance_chunk(Chunk* chunk, Gate* gate, uint64_t segment_id);

    // Determine the window to rebalance
    bool rebalance_gate_find_window(Chunk* chunk, Gate* gate, uint64_t segment_id, int64_t* inout_window_start, int64_t* inout_window_length) const;
    bool rebalance_chunk_find_window(Chunk* chunk, Gate* gate, int64_t* out_gate_start, int64_t* out_gate_end) const;

    // Lock a single gate and read the amount of space filled
    int64_t rebalance_chunk_acquire_lock(Chunk* chunk, uint64_t gate_id, std::vector<std::promise<void>>& waitlist);

    // Get the minimum and maximum amount of space allowed by the density thresholds in the calibrator tree
    std::pair<int64_t, int64_t> get_thresholds(int height) const;

    // Point look up in the index the given search key
    IndexEntry index_find(uint64_t vertex_id) const;
    IndexEntry index_find(uint64_t edge_source, uint64_t edge_destination) const;

public:

    /**
     * Create a new instance of a sparse array, as a list of chunks, each arranged according to the PMA layout.
     * @param num_qwords_per_segment the approximate number of qwords (8 byte words) per segment
     * @param num_segments_per_gate the number of contiguous segments protected by the same lock/gate
     * @param memory_footprint the size of each chunk in memory bytes, the default is 2MB
     */
    SparseArray(uint64_t num_qwords_per_segment = 512 /* 4 Kb */, uint64_t num_segments_per_gate = 8, uint64_t memory_footprint = 2097152ull /* 2 MB */);

    /**
     * Destructor
     */
    ~SparseArray();

    /**
     * Insert the given vertex in the sparse array
     */
    void insert_vertex(uint64_t vertex_id);

    /**
     * Remove the given vertex from the sparse array
     */
    void remove_vertex(uint64_t vertex_id);

    /**
     * Process an undo record
     */
    void process_undo(void* undo_payload, teseo::internal::context::Undo* next);

    /**
     * Dump the payload of an undo record
     */
    void dump_undo(void* undo_payload) const;
};


std::ostream& operator<<(std::ostream& out, const SparseArray::Update& update);

}
