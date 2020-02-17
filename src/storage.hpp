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
#include <future>
#include <iostream>
#include <string>

#include "circular_array.hpp"
#include "latch.hpp"

namespace teseo::internal {

class Storage {
public:

    /**
     * A single key in the StaticIndex consists of an edge, that is a pair <source, destination>
     */
    class Key {
        uint64_t m_source; // the source of the edge
        uint64_t m_destination; // the destination of the edge

    public:
        // Constructor
        Key(); // an invalid key, a pair <int_max, int_max>
        Key(uint64_t vertex_id); // vertex_id -> 0 represents the start of all items for the given vertex
        Key(uint64_t source, uint64_t destination); // the edge source -> destination

        uint64_t get_source() const;
        uint64_t get_destination() const;
        void set(uint64_t vertex_id);
        void set(uint64_t source, uint64_t destination);
        bool operator==(const Key& other) const;
        bool operator!=(const Key& other) const;
        bool operator<(const Key& other) const;
        bool operator<=(const Key& other) const;
        bool operator>(const Key& other) const;
        bool operator>=(const Key& other) const;
        static Key min();
        static Key max();
    };

    /**
     * An entry gate acts as an ultimate read/write latch to a contiguous sequence of segments in a sparse array
     */
    class Gate {
        Gate(const Gate&) = delete;
        Gate& operator=(const Gate&) = delete;

    public:
        const uint16_t m_gate_id; // the ID of this gate in the leaf, from 0 up to the total number of gates -1
        const uint16_t m_num_segments; // the number of segments in the gate
//        const uint32_t m_window_start; // the first segment of this gate
//        const uint32_t m_window_length; // the number of segments controlled by this gate

        enum class State : uint16_t {
            FREE, // no threads are operating on this gate
            READ, // one or more readers are active on this gate
            WRITE, // one & only one writer is active on this gate
            TIMEOUT, // set by the timer manager on an occupied gate, the last reader/writer must ask to rebalance the gate
            REBAL, // this gate is closed and it's currently being rebalanced
        };
        State m_state = State::FREE; // whether reader/writer/rebalancing in progress?
        int16_t m_num_active_threads; // how many readers are currently accessing the gate?

        SpinLock m_spin_lock; // sync the access to the gate
    #if !defined(NDEBUG)
        bool m_locked = false; // keep track whether the spin lock has been acquired, for debugging purposes
        int64_t m_owned_by = -1;
    #endif
    public:
        uint64_t m_space_left; // the amount of empty space to write new elements in the gate, in 8 bytes words
    private:
        Key m_fence_low_key; // the minimum key that can be stored in this gate (inclusive)
        Key m_fence_high_key; // the maximum key that can be stored in this gate (exclusive)

        struct SleepingBeauty{
            State m_purpose; // either read or write
            std::promise<void>* m_promise; // the thread waiting
        };
        CircularArray<SleepingBeauty> m_queue; // a queue with the threads waiting to access the array

        // Get the base address where the separator keys are stored
        Key* separator_keys();
        const Key* separator_keys() const;

    public:
        /**
         * Constructor
         */
        Gate(uint64_t gate_id, uint64_t num_segments);

        /**
         * Destructor
         */
        ~Gate();

        /**
         * Retrieve the ID of this gate
         */
        int64_t gate_id() const noexcept;

        /**
         * Retrieve the ID of the first segment in this gate
         */
        int64_t window_start() const noexcept;

        /**
         * Retrieve the number of segments in this gate
         */
        int64_t window_length() const noexcept;

        /**
         * Acquire the spin lock protecting this gate
         */
        void lock();

        /**
         * Release the spin lock protecting this gate
         */
        void unlock();

        /**
         * Retrieve the segment associated to the given key.
         * Precondition: the gate has been acquired by the thread
         */
        uint64_t find(Key key) const;

        /**
         * Set the separator key at the given offset
         */
        void set_separator_key(size_t segment_id, Key key);

        /**
         * Retrieve the segment key for a given segment
         */
        Key get_separator_key(size_t segment_id) const;

        /**
         * The output of the method #check_fence_keys()
         */
        enum class Direction{
            LEFT, // the given key is lower than m_fence_low_key, check the gate on the left
            RIGHT, // the given key is greater or equal than m_fence_high_key, check the gate on the right
            GO_AHEAD, // the given key is in the interval of the gate fence keys
            INVALID, // the gate has been invalidated, the leaf has been marked for deletion, and the whole logical operation needs to be restarted from scratch
        };

        /**
         * Check whether the current search key belongs to this gate
         */
        Direction check_fence_keys(Key key) const;

        /**
         * Reset the value for the fence keys. The content of the keys in the gate has to be in [min, max].
         */
        void set_fence_keys(Key min, Key max);

        /**
         * Retrieve the amount of space required to store the given gate, together with the associated separator keys, in bytes.
         */
        static uint64_t memory_footprint(uint64_t num_segments);

//        /**
//         * Retrieve the next writer or set of readers from the queue to be wake up.
//         * Precondition: the caller holds the lock for this gate
//         */
//        void wake_next(WakeList& wake_list);
//        void wake_next(ClientContext* context); // shortcut
//
//        /**
//         * Retrieve the list of workers to wake up in this gate
//         */
//        void wake_all(WakeList& wake_list);
    };


    struct DynamicEntry {
        uint64_t m_insdel:1; // 0 = insert, 1 = delete
        uint64_t m_entity:1; // 0 = vertex, 1 = edge
        uint64_t m_version:62; // ptr to the transaction version
    };

    struct DynamicVertex : public DynamicEntry {
        uint64_t m_vertex_id;

        DynamicVertex(uint64_t vertex_id, bool is_insertion);
    };


    class Segment {
        Segment(const Segment&) = delete;
        Segment& operator=(const Segment&) = delete;

        uint16_t m_delta1_start; // the offset where the changes for the LHS of the segment start
        uint16_t m_delta2_start; // the offset where the changes for the RHS of the segment start
        uint16_t m_empty1_start; // the offset where the empty space for the LHS of the segment start
        uint16_t m_empty2_start; // the offset where the empty space for the RHS of the segment start

    public:
        Segment(uint64_t space);

        void set_section_offsets(uint64_t delta1_start, uint64_t delta2_start, uint64_t empty1_start, uint64_t empty2_start);

        uint64_t* data(); // where the data of the segment resides
        const uint64_t* data() const; // where the data of the segment resides


        /**
         * Insert the given item in the left hand side of the segment
         */
        bool insert_lhs(uint64_t vertex_id);

        // Get the amount of space left, in words
        uint64_t space_left() const;

        /**
         * Dump the content of this segment to stdout, for debugging purposes
         */
        void dump() const;
    };

    /**
     * A leaf
     */
    class Leaf {
        const uint16_t m_num_gates;
        const uint16_t m_num_segments_per_gate;
        const uint32_t m_space_per_segment;
        Latch m_latch_rebalancer; // acquired when a thread needs to rebalance more segments than those contained in a single gate

        Leaf(uint16_t num_gates, uint16_t num_segments_per_gate, uint32_t space_per_segment);

        ~Leaf();

        // Retrieve the total amount of space used by one gate and its associated segments, in bytes
        uint64_t get_total_gate_size() const;

    public:
        static Leaf* allocate(uint64_t memory_footprint = 2097152ull /* 2 MB */, uint64_t num_segments_per_gate = 8, uint64_t space_per_segment = 4096 /* 4 KB */);

        static void deallocate(Leaf* leaf);

        Gate* get_gate(uint64_t gate_id);

        Gate* get_gate_by_segment_id(uint64_t segment_id);

        Segment* get_segment(uint64_t segment_id);

        int64_t num_gates() const;

        int64_t num_segments() const;

        // Dump to stdout the content of this leaf, for debugging purposes
        void dump() const;
    };
};

std::ostream& operator<<(std::ostream& out, const Storage::Key& key);

} // namespace
