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

#include <chrono>
#include <cinttypes>
#include <future>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/util/circular_array_64k.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::aux { class PartialResult; } // forward declaration
namespace teseo::rebalance { class Crawler; } // forward declaration
namespace teseo::rebalance { class ScratchPad; } // forward declaration
namespace teseo::transaction { class Undo; } // forward declaration

namespace teseo::memstore {

class Context;
class CursorState;
class DenseFile;
class DirectPointer;
class LatchState;
class Leaf;
class RemoveVertex;
class SparseFile;

/**
 * A single segment of the sparse array
 */
class Segment {
    friend LatchState; // debug information regarding the latch
    friend Leaf* create_leaf();
    friend Leaf; // destroy_leaf
    friend rebalance::Crawler; // access to the latch
    Segment(); // use create_leaf()
    ~Segment(); // use destroy_leaf()
    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;

public:
    enum class State : uint8_t {
        FREE = 0, // no threads are operating on this gate
        READ, // one or more readers are active on this gate
        WRITE, // one & only one writer is active on this gate
        //TIMEOUT, // set by the timer manager on an occupied gate, the last reader/writer must ask to rebalance the gate
        REBAL, // this gate is closed and it's currently being rebalanced
    };

private:
    static constexpr uint16_t FLAG_FILE_TYPE = 0x1; // is this a dense or sparse file?
    static constexpr uint16_t FLAG_REBAL_REQUESTED = 0x2; // whether a request to rebalance was already sent before?
    static constexpr uint16_t FLAG_VERTEX_TABLE = 0x4; // the Merger s.t. should rebuild the vertex table for the segment

    uint8_t m_flags; // internal flags
    std::atomic<int32_t> m_used_space; // amount of space occupied in the segment, in terms of qwords

public:
    Key m_fence_key; // lower fence key for this segment

    // latch masks
    static constexpr uint64_t MASK_XLOCK = 1ull << 63; // the latch has been acquired in exclusive mode
    static constexpr uint64_t MASK_WRITER = 1ull << 62; // a writer is active in the segment, the state is WRITE
    static constexpr uint64_t MASK_REBALANCER = 1ull << 61; // a rebalancer accessed or is waiting to access the segment.
    static constexpr uint64_t MASK_WAIT = 1ull << 60; // there is at least one thread waiting in the queue. Used to implement a fair latch for readers.
    static constexpr uint64_t MASK_VERSION = (1ull << 48) -1; // the version of the latch/segment. Used by the optimistic readers
    static constexpr uint64_t MASK_READERS = (MASK_WAIT -1) & ~(MASK_VERSION); // current number of readers, when it's used as standard shared latch.

    uint64_t m_latch; // atomic to represent a latch
#if !defined(NDEBUG)
    int64_t m_writer_id = -1; // which thread_id is currently acting as writer, for debugging only
    int64_t m_rebalancer_id = -1; // which thread_id is currently acting as rebalancer, for debugging only
#endif

    struct SleepingBeauty{
        State m_purpose; // either read, write or rebal
        std::promise<void>* m_promise; // the thread waiting
    };
    util::CircularArray64k<SleepingBeauty> m_queue; // a queue with the threads waiting to access the array
private:
    std::chrono::steady_clock::time_point m_time_last_rebal; // the last time this gate was rebalanced
    rebalance::Crawler* m_crawler; // ptr to the context of the current rebalancer

    // Helper for the method #to_dense_file. Load the content of the sparse file (lhs/rhs) into the DenseFile::File output_file
    static void load_to_file(SparseFile* input, bool is_lhs, void* output_file, void* output_txlocks);

    // Retrieve the value associated to the given flag
    int get_flag(uint16_t flag) const;

    // Set the given flag
    void set_flag(uint16_t flag, int value);

    // Send a request for rebalance
    static void request_async_rebalance(Context& context);
public:
    // Retrieve the low fence key of the context's segment
    static Key get_lfkey(const Context& context);

    // Retrieve the high fence key of the context's segment
    static Key get_hfkey(const Context& context);

    // The amount of used space in the segment, in terms of qwords
    uint64_t used_space() const;

    // Get the current set of this segment
    State get_state() const noexcept;

    // Acquire a shared lock to the segment as a reader. Throw an error if there are too many readers active
    void reader_enter(bool fair_lock = true);

    // Release the acquired shared lock to the segment.
    void reader_exit() noexcept;

    // Acquire an optimistic lock to the segment.
    // @return the current version/epoch of the segment's latch
    uint64_t optimistic_enter();

    // Check whether the version of the latch matches the one given as argument
    bool has_optimistic_version(uint64_t version) const noexcept;

    // Validate the optimistic lock
    void optimistic_validate(uint64_t version) const;

    // Acquire exclusive access to the segment as a writer.
    void writer_enter() noexcept;

    // Release the latch in the segment
    void writer_exit() noexcept;

    // Acquire exclusive access to the segment as a writer. Assume that the segment has been just initialised.
    void writer_init_xlock() noexcept;

    // Acquire exclusive access to the segment as an asynchronous rebalancer
    static void async_rebalancer_enter(Context& context, Key lfkey, rebalance::Crawler* crawler); // it can raise Abort{} and RebalanceNotRecessary{}

    // Release the latch in the segment
    void async_rebalancer_exit() noexcept;

    // Mark this segment as just rebalanced
    void mark_rebalanced();

    // Check whether a rebalance request was issued on this segment
    bool has_requested_rebalance() const;

    // Cancel a previously made request of rebalance
    void cancel_rebalance_request();

    // Check whether it is necessary to perform an async rebalance to this segment
    bool need_async_rebalance() const;
    bool need_async_rebalance(Key lfkey) const;

    // Retrieve the version (latch's version) of this segment
    uint64_t get_version() const;

    // Perform the given update. This method always succeeds, or throws a NotSureIfVertexExists when the check on
    // the `has_source_vertex' fails.
    static void update(Context& context, const Update& update, bool has_source_vertex);

    // Perform the given rollback. This method either succeeds
    static void rollback(Context& context, const Update& update, transaction::Undo* next);

    // Remove the vertex and all of its attached outgoing edges
    static void remove_vertex(RemoveVertex& instance);

    // Unlock the vertex on the underlying file after an attempt of removing it
    static void unlock_vertex(RemoveVertex& instance);

    // Load all elements from the underlying file into the given buffer
    static void load(Context& context, rebalance::ScratchPad& buffer);

    // Save the elements from the buffer back to the underlying file
    static void save(Context& context, rebalance::ScratchPad& scratchpad, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget_achieved);

    // Check the existence of the element identified by the given `key'.
    // Forward the point look up to the underlying file. Assume the caller has acquired an optimistic lock
    static bool has_item_optimistic(Context& context, const Key& key, bool is_unlocked);

    // Retrieve the weight for the edge key.source -> key.dest
    // Forward the point look up to the underlying file. Assume the caller has acquired an optimistic lock
    static double get_weight_optimistic(Context& context, const Key& key);

    // Retrieve the degree for the given vertex
    static uint64_t get_degree(Context& context, Key& next);

    // Invoke the given callback, until it returns true, for all elements equal or greater than key
    // The callback must have a signature bool fn(uint64_t source, uint64_t destination, double weight);
    template<typename Callback>
    static bool scan(Context& context, Key& next, DirectPointer* state_load, CursorState* state_save, Callback&& callback);

    // Build the partial results for the aux view over this segment
    static bool aux_partial_result(Context& context, Key& next, aux::PartialResult* partial_result);

    // Remove all versions from the sparse file
    static void clear_versions(Context& context);

    // Wake the next thread in the waiting list
    void wake_next();

    // Wake all threads held into this segment waiting queue. Invoked by a rebalancer.
    void wake_all();

    // Retrieve the total number of elements in the underlying file
    static uint64_t cardinality(Context& context);

    // Retrieve the total number of words used in the underlying file
    static uint64_t used_space(Context& context);

    // Remove the unused records from the underlying file. Return the amount of filled space in the segment.
    static uint64_t prune(Context& context, bool rebuild_vertex_table = true);

    // Check whether this segment is not indexed. A segment does not have an index entry
    // when its low fence key is equal to its high fence key. It must also be empty.
    // This segment was created as part of a split, but no elements were loaded into.
    static bool is_unindexed(Context& context);

    // Check whether the segment is sparse
    bool is_sparse() const;

    // Check whether the segment is dense
    bool is_dense() const;

    // Transform to an (empty) sparse segment. If the segment is already sparse, this method does nothing.
    static void to_sparse_file(Context& context);

    // Transform the segment into dense, moving all existing items from the sparse to the dense segment
    static void to_dense_file(Context& context);

    // Retrieve the underlying sparse file
    static SparseFile* sparse_file(Context& context);

    // Retrieve the underlying dense file
    static DenseFile* dense_file(Context& context);

    // Get the crawler currently set
    rebalance::Crawler* get_crawler() const noexcept;

    // Check whether a crawler has been set
    bool has_crawler() const noexcept;

    // Set the crawler
    void set_crawler(rebalance::Crawler* crawler) noexcept;

    // Retrieve the max number of readers that can operate concurrently in the segment
    uint64_t max_num_readers() const;

    // Check whether the Merger thread should rebuild the vertex table for this segment
    bool need_rebuild_vertex_table() const;

    // Request the vertex table to be rebuilt
    void request_rebuild_vertex_table();

    // Set the flag `rebal_requested'. Only used for debugging and testing purposes.
    void set_flag_rebal_requested();

    // Retrieve a representation of the latch's state, for debugging & testing purposes
    LatchState latch_state() const;

    // Dump the content of the segment to stdout, for debugging purposes
    void dump();

    // Dump the segment and the underlying file to the output stream
    static void dump_and_validate(std::ostream& out, Context& context, bool* integrity_check);

    // Dump the underlying file to the output stream
    static void dump_file(std::ostream& out, Context& context, bool* integrity_check);

    // Helper method to dump the whole chain of undos
    static void dump_unfold_undo(std::ostream& out, const transaction::Undo* head);
};

// Write to the output stream a string representation of the state
std::ostream& operator<<(std::ostream& out, const Segment::State& state);


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
int Segment::get_flag(uint16_t flag) const {
    return static_cast<int>((m_flags & flag) >> __builtin_ctz(flag));
}

inline
void Segment::set_flag(uint16_t flag, int value){
    m_flags = (m_flags & ~flag) | (value << __builtin_ctz(flag));
}

inline
bool Segment::is_sparse() const {
    return get_flag(FLAG_FILE_TYPE) == 0;
}

inline
bool Segment::is_dense() const {
    return !is_sparse();
}

inline
bool Segment::has_optimistic_version(uint64_t version) const noexcept {
    return (m_latch & (MASK_WRITER | MASK_REBALANCER | MASK_VERSION)) == version;
}

inline
void Segment::optimistic_validate(uint64_t version) const {
    if(!has_optimistic_version(version)) throw Abort {};
}

inline
uint64_t Segment::get_version() const {
    return m_latch & MASK_VERSION;
}

inline
uint64_t Segment::used_space() const {
    return m_used_space;
}

inline
void Segment::request_rebuild_vertex_table() {
    set_flag(FLAG_VERTEX_TABLE, 1);
}

} // namespace

