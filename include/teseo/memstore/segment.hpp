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
#include "teseo/util/circular_array.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::rebalance { class Context; } // forward declaration
namespace teseo::transaction { class Undo; } // forward declaration

namespace teseo::memstore {

class Context;
class DenseFile;
class Leaf;
class RemoveVertex;
class SparseFile;

/**
 * A single segment of the sparse array
 */
class Segment {
    friend Leaf* create_leaf();
    friend void destroy_leaf(Leaf*);
    Segment(); // use create_leaf()
    ~Segment(); // use destroy_leaf()
    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;

    // Load from sparse to dense file
    void load_to_file(SparseFile* input, bool is_lhs, void* output_file, void* output_txlocks);

public:
    enum class State : uint16_t {
        FREE, // no threads are operating on this gate
        READ, // one or more readers are active on this gate
        WRITE, // one & only one writer is active on this gate
        //TIMEOUT, // set by the timer manager on an occupied gate, the last reader/writer must ask to rebalance the gate
        REBAL, // this gate is closed and it's currently being rebalanced
    };
    State m_state = State::FREE; // whether reader/writer/rebalance in progress?
    int16_t m_num_active_threads; // how many readers are currently accessing the gate?
    Key m_fence_key; // lower fence key for this segment

    util::OptimisticLatch<1> m_latch; // protection latch
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
    util::CircularArray<SleepingBeauty> m_queue; // a queue with the threads waiting to access the array
    std::chrono::steady_clock::time_point m_time_last_rebal; // the last time this gate was rebalanced
    rebalance::Context* m_rebal_context; // ptr to the context of the current rebalancer

    // Retrieve the low fence key of the context's segment
    static Key get_lfkey(Context& context);

    // Retrieve the high fence key of the context's segment
    static Key get_hfkey(Context& context);

    // Perform the given update. This method always succeeds, or throws a NotSureIfVertexExists when the check on
    // the `has_source_vertex' fails.
    void update(Context& context, const Update& update, bool has_source_vertex);

    // Perform the given rollback. This method either succeeds
    void rollback(Context& context, const Update& update, transaction::Undo* next);

    // Remove the vertex and all of its attached outgoing edges
    void remove_vertex(RemoveVertex& instance);

    // Unlock the vertex on the underlying file after an attempt of removing it
    void unlock_vertex(RemoveVertex& instance);

    // Check the existence of the element identified by the given `key'.
    // Forward the point look up to the underlying file. Assume the caller has acquired an optimistic lock
    bool has_item_optimistic(Context& context, const Key& key, bool is_unlocked) const;

    // Retrieve the weight for the edge key.source -> key.dest
    // Forward the point look up to the underlying file. Assume the caller has acquired an optimistic lock
    double get_weight_optimistic(Context& context, const Key& key) const;

    // Acquire the spin lock protecting this gate
    void lock();

    // Release the spin lock protecting this gate
    void unlock();

    // Hold the current thread on this segment until it becomes accessible
    template<State role, typename Lock>
    void wait(Lock& lock);

    // Wake the next thread in the waiting list
    void wake_next();

    // Wake all threads held into this segment waiting queue. Invoked by a rebalancer.
    void wake_all();

    // Check whether the segment is sparse
    bool is_sparse() const;

    // Check whether the segment is dense
    bool is_dense() const;

    // Transform to an (empty) sparse segment. If the segment is already sparse, this method does nothing.
    void to_sparse_file(Context& context);

    // Transform the segment into dense, moving all existing items from the sparse to the dense segment
    void to_dense_file(Context& context);

    // Retrieve the underlying sparse file
    SparseFile* sparse_file(Context& context) const;

    // Retrieve the underlying dense file
    DenseFile* dense_file(Context& context) const;

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
bool Segment::is_sparse() const {
    return m_latch.get_payload() == 0;
}

inline
bool Segment::is_dense() const {
    return !is_sparse();
}

} // namespace

