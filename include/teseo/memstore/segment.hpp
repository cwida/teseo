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
class Leaf;
class RemoveVertex;
class SparseFile;

/**
 * A single segment of the sparse array
 */
class Segment {
    friend Leaf* create_leaf();
    friend Leaf; // destroy_leaf
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

    State m_state; // the current state of this segment
    uint8_t m_flags; // internal flags
    int16_t m_num_active_threads; // how many readers are currently accessing the gate?
    std::atomic<int32_t> m_used_space; // amount of space occupied in the segment, in terms of qwords

public:
    Key m_fence_key; // lower fence key for this segment

    util::OptimisticLatch<0> m_latch; // protection latch
#if !defined(NDEBUG)
    bool m_locked = false; // keep track whether the spin lock has been acquired, for debugging purposes
    int64_t m_owned_by = -1; // which thread_id acquired the lock (if m_locked == true)
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

    // Load from sparse to dense file
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
    State get_state() const;

    // Set the state of this segment
    void set_state(State state);

    // Get the number of active threads in the segment
    int get_num_active_threads() const;

    // Increment, by 1, the number of active threads
    void incr_num_active_threads();

    // Decrement, by 1, the number of active threads
    void decr_num_active_threads();

    // Mark this segment as just rebalanced
    void mark_rebalanced();

    // Check whether a rebalance request was issued on this segment
    bool has_requested_rebalance() const;

    // Cancel a previously made request of rebalance
    void cancel_rebalance_request();

    // Check whether enough time from the last time has passed
    bool need_async_rebalance() const;

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
    static bool scan(Context& context, Key& next, CursorState* cs, Callback&& callback);

    // Build the partial results for the aux view over this segment
    static bool aux_partial_result(Context& context, Key& next, aux::PartialResult* partial_result);

    // Remove all versions from the sparse file
    static void clear_versions(Context& context);

    // Acquire the spin lock protecting this segment
    void lock();

    // Release the spin lock protecting this segment
    void unlock();

    // Invalidate the spin lock protected this segment
    void invalidate();

    // Hold the current thread on this segment until it becomes accessible
    template<State role, typename Lock>
    void wait(Lock& lock);

    // Wake the next thread in the waiting list
    void wake_next();

    // Wake all threads held into this segment waiting queue. Invoked by a rebalancer.
    void wake_all();

    // Retrieve the total number of elements in the underlying file
    static uint64_t cardinality(Context& context);

    // Retrieve the total number of words used in the underlying file
    static uint64_t used_space(Context& context);

    // Remove the unused records from the underlying file
    static void prune(Context& context);

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
    rebalance::Crawler* get_crawler() const;

    // Check whether a crawler has been set
    bool has_crawler() const;

    // Set the crawler
    void set_crawler(rebalance::Crawler* crawler);

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
#if defined(NDEBUG)
inline
void Segment::lock(){
    m_latch.lock();
}

inline
void Segment::unlock(){
    m_latch.unlock();
}

inline
void Segment::invalidate(){
    m_latch.invalidate();
}
#endif

template<Segment::State role, typename Lock>
void Segment::wait(Lock& lock) {
    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();
    m_queue.append({ role, &producer } );
    lock.unlock();
    consumer.wait();
}

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
Segment::State Segment::get_state() const {
    return m_state;
}

inline
void Segment::set_state(Segment::State state){
    m_state = state;
}

inline
int Segment::get_num_active_threads() const {
    return m_num_active_threads;
}

inline
void Segment::incr_num_active_threads() {
    m_num_active_threads ++;
}

inline
void Segment::decr_num_active_threads() {
    assert(m_num_active_threads > 0 && "Underflow");
    m_num_active_threads --;
}

inline
uint64_t Segment::used_space() const {
    return m_used_space;
}

inline
rebalance::Crawler* Segment::get_crawler() const {
    return m_crawler;
}

inline
bool Segment::has_crawler() const {
    return m_crawler != nullptr;
}

inline
void Segment::set_crawler(rebalance::Crawler* crawler){
    m_crawler = crawler;
}

inline
bool Segment::has_requested_rebalance() const {
    return get_flag(FLAG_REBAL_REQUESTED);
}

inline
bool Segment::need_async_rebalance() const {
    return has_requested_rebalance() && std::chrono::steady_clock::now() >= m_time_last_rebal;
}

inline
void Segment::mark_rebalanced(){
    m_time_last_rebal = std::chrono::steady_clock::now();
    set_flag(FLAG_REBAL_REQUESTED, 0);
}

inline
void Segment::cancel_rebalance_request() {
    set_flag(FLAG_REBAL_REQUESTED, 0);
}


} // namespace

