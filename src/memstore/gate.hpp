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

#include "util/circular_array.hpp"
#include "latch.hpp"
#include "key.hpp"

namespace teseo::internal::memstore {

/**
 * A gate acts as a latch and additional control state to access a portion of a sparse array.
 */
class Gate {
    Gate(const Gate&) = delete;
    Gate& operator=(const Gate&) = delete;

public:
    const uint16_t m_gate_id; // the ID of this gate in the leaf, from 0 up to the total number of gates -1
    const uint16_t m_num_separator_keys; // the number of separator keys in the gate

    enum class State : uint16_t {
        FREE, // no threads are operating on this gate
        READ, // one or more readers are active on this gate
        WRITE, // one & only one writer is active on this gate
        //TIMEOUT, // set by the timer manager on an occupied gate, the last reader/writer must ask to rebalance the gate
        REBAL, // this gate is closed and it's currently being rebalanced
    };
    State m_state = State::FREE; // whether reader/writer/rebalancing in progress?
    int16_t m_num_active_threads; // how many readers are currently accessing the gate?

    OptimisticLatch<0> m_latch; // sync the access to the gate
#if !defined(NDEBUG)
    bool m_locked = false; // keep track whether the spin lock has been acquired, for debugging purposes
    int64_t m_owned_by = -1;
#endif
    std::atomic<int64_t> m_used_space; // number of qwords filled inside the segments belonging to this gate
    Key m_fence_low_key; // the minimum key that can be stored in this gate (inclusive)
    Key m_fence_high_key; // the maximum key that can be stored in this gate (exclusive)

    struct SleepingBeauty{
        State m_purpose; // either read, write or rebal
        std::promise<void>* m_promise; // the thread waiting
    };
    util::CircularArray<SleepingBeauty> m_queue; // a queue with the threads waiting to access the array

    // Get the base address where the separator keys are stored
    Key* separator_keys();
    const Key* separator_keys() const;

public:
    /**
     * Constructor
     */
    Gate(uint64_t gate_id, uint64_t num_separator_keys);

    /**
     * Destructor
     */
    ~Gate();

    /**
     * Retrieve the ID of this gate
     */
    int64_t id() const noexcept;

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
     * Retrieve the segment associated to the given key, in [0, num_segments)
     * Precondition: the gate has been acquired by the thread
     */
    uint64_t find(Key key) const;

    /**
     * Set the separator key at the given offset
     */
    void set_separator_key(uint64_t position, Key key);

    /**
     * Retrieve the segment key for a given segment
     */
    Key get_separator_key(uint64_t position) const;

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
    static uint64_t memory_footprint(uint64_t num_separator_keys);

    /**
     * Wake up the next threads waiting to update the gate.
     * Precondition: the caller holds the lock for this gate
     */
    void wake_next();

    /**
     * Wake all threads waiting in this gate. Invoked by a rebalancer.
     */
    void wake_all();
};

} // namespace

