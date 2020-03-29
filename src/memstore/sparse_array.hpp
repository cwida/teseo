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
#include "context/transaction_impl.hpp" // TransactionRollbackImpl

namespace teseo::internal::context {
class ThreadContext; // forward declaration
class Undo; // forward declaration
}

namespace teseo::internal::memstore {

class Gate; // forward declaration
class Index; // forward declaration
class Rebalancer; // forward declaration

class SparseArray : public context::TransactionRollbackImpl {
    friend class Rebalancer;
    SparseArray(const SparseArray&) = delete;
    SparseArray& operator=(const SparseArray&) = delete;
    using Transaction = teseo::internal::context::TransactionImpl; // shortcut

    const bool m_is_directed; // whether the semantic of the edge updates is for directed or undirected graphs. Note this flag only affects edge_insert and edge_remove
    const uint64_t m_num_gates_per_chunk; // how many gates each chunk contains
    const uint64_t m_num_segments_per_lock; // how many segments are stored for each lock. Each gate actually store 2*spl separator keys, one for the lhs, one for the rhs of each segment
    const uint64_t m_num_qwords_per_segment; // how many qwords (8 bytes) can be stored inside a segment. That is the amount of space that compose a segment
    Index* m_index; // primary & sparse index to the created chunks/gates

    /**
     * A single entry retrieved from the index
     */
    struct IndexEntry {
        uint64_t m_gate_id:16;
        uint64_t m_chunk_id:48;
    };

    // The metadata associated to each chunk
    struct Chunk {
        Latch m_latch; // acquired when a thread needs to rebalance more segments than those contained in a single gate
    };

    // The metadata associated to each segment
    struct SegmentMetadata {
        uint16_t m_versions1_start; // the offset where the changes for the LHS of the segment start, in qwords
        uint16_t m_versions2_start; // the offset where the changes for the RHS of the segment start, in qwords
        uint16_t m_empty1_start; // the offset where the empty space for the LHS of the segment start, in qwords
        uint16_t m_empty2_start; // the offset where the empty space for the RHS of the segment start, in qwords
    };

    /**
     * A static vertex entry in the segment
     */
    struct SegmentVertex {
        uint64_t m_vertex_id; // the id of the vertex
        uint64_t m_first :1; // whether this is the first vertex with this ID stored in a segment
        uint64_t m_count :63; // number of static edges following the static vertex
    };


    /**
     * A static edge entry in the segment
     */
    struct SegmentEdge {
        uint64_t m_destination; // the destination id of the given edge
        double m_weight; // the weight associated to the edge
    };

    /**
     * A single version entry
     */
    struct SegmentVersion {
        uint64_t m_insdel:1; // 0 = insert, 1 = delete
        uint64_t m_undo_length:3; // length of the version chain: 0, ..., 6; 7 => length >= 7
        uint64_t m_backptr:12; // offset to the content
        uint64_t m_version:48; // ptr to the transaction version
    };
    static constexpr uint64_t MAX_UNDO_LENGTH = (1ull<< /* num bits in m_undo_length */ 4) -1; // => 7


    /**
     * Vertex, Edge and Version offsets, as multiples of 8 bytes (qwords)
     */
    static_assert(sizeof(SegmentVertex) % 8 == 0);
    static_assert(sizeof(SegmentEdge) % 8 == 0);
    static_assert(sizeof(SegmentVersion) % 8 == 0);
    constexpr static uint64_t OFFSET_VERTEX = sizeof(SegmentVertex) / 8;
    constexpr static uint64_t OFFSET_EDGE = sizeof(SegmentEdge) / 8;
    constexpr static uint64_t OFFSET_VERSION = sizeof(SegmentVersion) / 8;

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
    static Key get_key(const Update* u);

    // Retrieve the chunk from the given IndexEntry
    Chunk* get_chunk(const IndexEntry entry);
    const Chunk* get_chunk(const IndexEntry entry) const;

    // Retrieve the gate with the given ID
    Gate* get_gate(const Chunk* chunk, uint64_t gate_id) const;

    // Retrieve the metadata associated to a segment
    SegmentMetadata* get_segment(const Chunk* chunk, uint64_t segment_id);
    const SegmentMetadata* get_segment(const Chunk* chunk, uint64_t segment_id) const;

    // Retrieve the start/end pointers of a segment
    uint64_t* get_segment_lhs_content_start(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_lhs_content_start(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_lhs_content_end(const Chunk* chunk, SegmentMetadata*  segment);
    const uint64_t* get_segment_lhs_content_end(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_lhs_versions_start(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_lhs_versions_start(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_lhs_versions_end(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_lhs_versions_end(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_rhs_content_start(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_rhs_content_start(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_rhs_content_end(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_rhs_content_end(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_rhs_versions_start(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_rhs_versions_start(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_rhs_versions_end(const Chunk* chunk, SegmentMetadata* segment);
    const uint64_t* get_segment_rhs_versions_end(const Chunk* chunk, const SegmentMetadata* segment) const;
    uint64_t* get_segment_content_start(const Chunk* chunk, SegmentMetadata* segment, bool is_lhs);
    const uint64_t* get_segment_content_start(const Chunk* chunk, const SegmentMetadata*  segment, bool is_lhs) const;
    uint64_t* get_segment_content_end(const Chunk* chunk, SegmentMetadata* segment, bool is_lhs);
    const uint64_t* get_segment_content_end(const Chunk* chunk, const SegmentMetadata* segment, bool is_lhs) const;
    uint64_t* get_segment_versions_start(const Chunk* chunk, SegmentMetadata* segment, bool is_lhs);
    const uint64_t* get_segment_versions_start(const Chunk* chunk, const SegmentMetadata* segment, bool is_lhs) const;
    uint64_t* get_segment_versions_end(const Chunk* chunk, SegmentMetadata* segment, bool is_lhs);
    const uint64_t* get_segment_versions_end(const Chunk* chunk, const SegmentMetadata* segment, bool is_lhs) const;

    // The height of the calibrator tree in a chunk
    int64_t get_cb_height_per_chunk() const;

    // Retrieve the amount of free space in the given segment, in qwords
    uint64_t get_segment_free_space(const Chunk* chunk, const SegmentMetadata* segment) const;

    // Retrieve the amount of used space in the given segment, in qwords
    uint64_t get_segment_used_space(const Chunk* chunk, const SegmentMetadata* segment) const;

    // Check whether the given segment is empty
    bool is_segment_empty(const Chunk* chunk, const SegmentMetadata* segment) const;
    bool is_segment_empty(const Chunk* chunk, const SegmentMetadata* segment, bool is_lhs) const;
    bool is_segment_lhs_empty(const Chunk* chunk, const SegmentMetadata* segment) const;
    bool is_segment_rhs_empty(const Chunk* chunk, const SegmentMetadata* segment) const;

    // Check whether the given segment is dirty (contains versions)
    bool is_segment_dirty(const Chunk* chunk, const SegmentMetadata* segment);
    bool is_segment_dirty(const Chunk* chunk, const SegmentMetadata* segment, bool is_lhs);

    // Retrieve the amount of free space in the segments of the given gate, in qwords
    uint64_t get_gate_free_space(const Chunk* chunk, const Gate* gate) const;

    // Retrieve the amount of used space in the segments of the given gate, in qwords
    uint64_t get_gate_used_space(const Chunk* chunk, const Gate* gate) const;

    // Retrieve the minimum stored in a given segment
    Key get_minimum(const Chunk* chunk, const SegmentMetadata* segment) const;
    Key get_minimum(const Chunk* chunk, const SegmentMetadata* segment, bool is_lhs) const;

    // Check whether the record refers to an insertion or a removal
    static bool is_insert(const SegmentVersion* version);
    static bool is_remove(const SegmentVersion* version);
    static bool is_insert(const Update& update);
    static bool is_insert(const Update* update);
    static bool is_remove(const Update& update);
    static bool is_remove(const Update* update);

    // Check whether the record refers to a vertex or an edge
    static bool is_vertex(const Update& metadata);
    static bool is_edge(const Update& metadata);

    // Retrieve the vertex/edge from the static portion
    static SegmentVertex* get_vertex(uint64_t* ptr);
    static const SegmentVertex* get_vertex(const uint64_t* ptr);
    static SegmentEdge* get_edge(uint64_t* ptr);
    static const SegmentEdge* get_edge(const uint64_t* ptr);

    // Retrieve the record's metadata associate to the given ptr
    static SegmentVersion* get_version(uint64_t* ptr);
    static const SegmentVersion* get_version(const uint64_t* ptr);

    // Get the offset (back pointer) to which this segment version refers
    static uint64_t get_backptr(uint64_t* ptr);
    static uint64_t get_backptr(const uint64_t* ptr);
    static uint64_t get_backptr(SegmentVersion* ptr);
    static uint64_t get_backptr(const SegmentVersion* ptr);

    // Retrieve the UndoRecord associated to a delta record
    static teseo::internal::context::Undo* get_undo(uint64_t* ptr);
    static const teseo::internal::context::Undo* get_undo(const uint64_t* ptr);
    static teseo::internal::context::Undo* get_undo(SegmentVersion* ptr);
    static const teseo::internal::context::Undo* get_undo(const SegmentVersion* ptr);

    // Reset the type of the record
    static void reset_header(uint64_t* version);
    static void reset_header(SegmentVersion* version);
    static void set_type(SegmentVersion* version, bool true_if_insert_or_false_if_remove);
    static void set_type(SegmentVersion* version, const Update& update);
    static void set_backptr(SegmentVersion* version, uint64_t offset);
    static void set_undo(SegmentVersion* version, teseo::internal::context::Undo* undo);
    static void unset_undo(SegmentVersion* version, teseo::internal::context::Undo* undo);

    // Prune the undo records
    static void prune_on_write(SegmentVersion* version, bool force = false);

    // Retrieve the update readable by the current transaction for the given delta record
    // @return true if the record is visible by the transaction, false otherwise
    static bool read_delta(const Transaction* transaction, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* ptr, Update* out_update);
    static bool read_delta_optimistic(Gate* gate, uint64_t version, const Transaction* transaction, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* ptr, Update* out_update);
    static bool read_delta_impl(const Transaction* transaction, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* ptr, const teseo::internal::context::Undo* undo, Update* out_update);

    // Allocate a new chunk of the sparse array
    Chunk* allocate_chunk();

    // Release a previously allocated chunk
    void free_chunk(Chunk* chunk);

    // Removing the linked list of undos from a segment
    void clear_undos(Chunk* chunk, SegmentMetadata* segment, bool is_lhs);

    // Actual constructor
    struct InitSparseArrayInfo{ bool m_is_directed; uint64_t m_num_gates_per_chunk; uint64_t m_num_segments_per_lock; uint64_t m_num_qwords_per_segment; };
    static InitSparseArrayInfo compute_alloc_params(bool is_directed, uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint_bytes);
    SparseArray(InitSparseArrayInfo init);

    // Perform the given insertion, taking care of the consistency. That is, it ensures that the source vertex (but not the destination vertex) actually exists
    void do_insert_edge(Transaction* transaction, Update& update);

    // Perform an update (write), by adding/removing a new vertex/edge in the sparse array
    void write(Transaction* transaction, Update& update, bool is_consistent);

    // Retrieve the Chunk and the Gate where to perform the insertion/deletion
    std::pair<Chunk*, Gate*> writer_on_entry(const Update& update);

    // Release the acquired gate
    void writer_on_exit(Chunk* chunk, Gate* gate);

    // Context switch on this gate & release the lock
    template<typename Lock> void writer_wait(Gate& gate, Lock& lock);

    // Attempt to perform an update inside the given gate. Return <true> in case of success, <false> otherwise.
    bool do_write_gate(Transaction* transaction, Chunk* chunk, Gate* gate, Update& update, bool is_consistent);

    // Attempt to perform an update into the given segment. Return <true> in case of success, <false> otherwise
    bool do_write_segment(Transaction* transaction, Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update, bool is_consistent);
    bool do_write_segment_vertex(Transaction* transaction, Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update);
    bool do_write_segment_edge(Transaction* transaction, Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update, bool is_consistent);

    // Attempt to rebalance a gate (local rebalance)
    bool rebalance_gate(Chunk* chunk, Gate* gate, uint64_t segment_id);

    // Attempt to rebalance a chunk (global rebalance)
    void rebalance_chunk(Chunk* chunk, Gate* gate);

    // Determine the window to rebalance
    bool rebalance_gate_find_window(Chunk* chunk, Gate* gate, uint64_t segment_id, int64_t* inout_window_start, int64_t* inout_window_length) const;
    bool rebalance_chunk_find_window(Chunk* chunk, Gate* gate, int64_t* out_gate_start, int64_t* out_gate_end);

    // Recompute the used space inside a chunk
    void rebalance_recompute_used_space(Chunk* chunk);
    void rebalance_recompute_used_space(Chunk* chunk, Gate* gate);

    // Lock a single gate and read the amount of space filled
    int64_t rebalance_chunk_acquire_lock(Chunk* chunk, uint64_t gate_id, std::vector<std::promise<void>>& waitlist);

    // Unlock a single gate, wake up all threads waiting on it
    void rebalance_chunk_release_lock(Chunk* chunk, uint64_t gate_id);

    // Update the fence keys in the given window
    Key update_fence_keys(Chunk* chunk, int64_t gate_window_start, int64_t gate_window_length, Key new_fence_key_max);

    // Update the separator keys of a given gate
    Key update_separator_keys(Chunk* chunk, Gate* gate, int64_t sep_key_start, int64_t sep_key_end);

    // Rollback an update in the given segment.
    void do_rollback_segment(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& undo, teseo::internal::context::Undo* next);

    // Get the minimum and maximum amount of space allowed by the density thresholds in the calibrator tree
    std::pair<int64_t, int64_t> get_thresholds(int height) const;

    // Point look up in the index the given search key
    IndexEntry index_find(uint64_t vertex_id) const;
    IndexEntry index_find(Key key) const;
    IndexEntry index_find(uint64_t edge_source, uint64_t edge_destination) const;

    // Add an entry in the index
    void index_insert(Key key, Chunk* chunk, uint64_t gate_id);

    // Insert the fence keys for the given chunk
    void index_insert(Chunk* chunk);
    void index_insert(Chunk* chunk, int64_t gate_window_start, int64_t gate_window_length);

    // Remove an entry from the index
    void index_remove(Key key);

    // Remove all min fence keys for the given chunk
    void index_remove(Chunk* chunk);
    void index_remove(Chunk* chunk, int64_t gate_window_start, int64_t gate_window_length);

    // Check whether the given vertex/edge exists
    bool has_item(Transaction* transaction, bool is_vertex, Key key) const;
    bool has_item_segment_optimistic(Transaction* transaction, const Chunk* chunk, Gate* gate, uint64_t gate_version, uint64_t segment_id, bool is_lhs, bool is_vertex, Key key) const;

    // Retrieve the Chunk and the Gate where to perform a read
    std::pair<const Chunk*, Gate*> reader_on_entry(Key key) const;
    std::tuple<const Chunk*, Gate*, uint64_t> reader_on_entry_optimistic(Key key) const;

    // Context switch on this gate & release the lock
    template<typename Lock> void reader_wait(Gate* gate, Lock& lock) const;

    // Release the lock from the gate
    void reader_on_exit(const Chunk* chunk, Gate* gate) const;

    // Check we are accessing the correct gate
    // @param gate_id [in/out] when false, updated to the next gate_id to acquire
    bool check_fence_keys(Gate* gate, int64_t& gate_id, Key key) const;

    // Dump the content of the sparse array
    void dump_chunk(std::ostream& out, const Chunk* chunk, uint64_t chunk_no, bool* integrity_check) const;
    void dump_segment(std::ostream& out, const Chunk* chunk, const Gate* gate, const SegmentMetadata* segment, bool is_lhs, Key fence_key_low, Key fence_key_high, bool* integrity_check) const;
    void dump_segment_item(std::ostream& out, uint64_t position, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* version, bool* integrity_check) const;
    void dump_validate_key(std::ostream& out, const SegmentVertex* vertex, const SegmentEdge* edge, Key fence_key_low, Key fence_key_high, bool* integrity_check) const;
//    void dump_segment_vertex(std::ostream& out, uint64_t rank, const SegmentVertex* vtx_static, const SegmentDeltaVertex* vtx_delta) const;
//    void dump_segment_edge(std::ostream& out, uint64_t rank, const SegmentVertex* vtx_static, const SegmentEdge* edge_static, const SegmentDeltaEdge* edge_delta) const;
    void dump_unfold_undo(std::ostream& out, const teseo::internal::context::Undo* undo) const; // unfold the chain of undo records

public:

    /**
     * Create a new instance of a sparse array, as a list of chunks, each arranged according to the PMA layout.
     * @param num_qwords_per_segment the approximate number of qwords (8 byte words) per segment
     * @param num_segments_per_gate the number of contiguous segments protected by the same lock/gate
     * @param memory_footprint the size of each chunk in memory bytes, the default is 2MB
     */
    SparseArray(bool directed, uint64_t num_qwords_per_segment = 512 /* 4 Kb */, uint64_t num_segments_per_gate = 8, uint64_t memory_footprint = 2097152ull /* 2 MB */);

    /**
     * Destructor
     */
    virtual ~SparseArray();

    /**
     * Insert the given vertex in the sparse array
     */
    void insert_vertex(Transaction* transaction, uint64_t vertex_id);

    /**
     * Check whether the given vertex is present
     */
    bool has_vertex(Transaction* transaction, uint64_t vertex_id) const;

    /**
     * Remove the given vertex from the sparse array
     */
    void remove_vertex(Transaction* transaction, uint64_t vertex_id);

    /**
     * Insert the given edge in the sparse array
     */
    void insert_edge(Transaction* transaction, uint64_t source, uint64_t destination, double weight);

    /**
     * Check whether the given edge exists
     */
    bool has_edge(Transaction* transaction, uint64_t source, uint64_t destination) const;

    /**
     * Remove the given edge from the sparse array
     */
    void remove_edge(Transaction* transaction, uint64_t source, uint64_t destination);

    /**
     * Check whether the semantic of edge updates tailors directed graphs
     */
    bool is_directed() const;

    /**
     * Check whether the semantic of edge updates tailors undirected graphs
     */
    bool is_undirected() const;

    /**
     * Process an undo record
     */
    void do_rollback(void* object, teseo::internal::context::Undo* next) override;

    /**
     * Remove all chunks of the sparse array. Invoked by the global instance before releasing the
     * object, to avoid memory leaks
     */
    void clear();

    /**
     * Dump the content of the sparse array
     */
    void dump() const;
};


std::ostream& operator<<(std::ostream& out, const SparseArray::Update& update);

}
