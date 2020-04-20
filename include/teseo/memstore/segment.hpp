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
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/util/circular_array.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::rebalance { class Context; } // forward declaration

namespace teseo::memstore {

class Context;
class Leaf;
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
    util::CircularArray<SleepingBeauty> m_queue; // a queue with the threads waiting to access the array
    std::chrono::steady_clock::time_point m_time_last_rebal; // the last time this gate was rebalanced
    rebalance::Context* m_rebal_context; // ptr to the context of the current rebalancer


    /**
     * Acquire the spin lock protecting this gate
     */
    void lock();

    /**
     * Release the spin lock protecting this gate
     */
    void unlock();

    /**
     * Wait on this segment
     */
    template<State role, typename Lock>
    void wait(Lock& lock) {
        std::promise<void> producer;
        std::future<void> consumer = producer.get_future();
        m_queue.append({ role, &producer } );
        lock.unlock();
        consumer.wait();
    }

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

