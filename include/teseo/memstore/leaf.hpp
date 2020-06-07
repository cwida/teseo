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
#include <cassert>
#include <future>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/util/latch.hpp"
#include "teseo/util/circular_array.hpp"

namespace teseo::gc { class GarbageCollector; } // forward declaration

namespace teseo::memstore {

/**
 * A leaf of the Fat Tree. It consists of a sequence of segments of a sparse array.
 */
class Leaf {
    friend Leaf* create_leaf();

    Leaf(); // use create_leaf();
    ~Leaf(); // use destroy_leaf();

    Leaf(const Leaf&) = delete;
    Leaf& operator=(const Leaf&) = delete;

    util::Latch m_latch; // acquired when a thread needs to rebalance more segments than those contained in a single gate
    bool m_active = false; // true if a rebalancer is currently exploring multiple gates
    util::CircularArray<std::promise<void>*> m_queue; // additional rebalancers requesting access to the chunk
    Key m_fence_key; // the max fence key for this leaf
    std::atomic<int64_t> m_ref_count =1; // number of live references to this leaf

    // Release an existing instance of a leaf
    static void destroy_leaf(Leaf*);

public:
    /**
     * Retrieve the segment at the given ID
     */
    Segment* get_segment(uint64_t segment_id) const;

    /**
     * Retrieve the total number of segments
     */
    uint64_t num_segments() const;

    /**
     * Retrieve the min fence key for this leaf
     */
    Key get_lfkey() const;

    /**
     * Retrieve the max fence key for this leaf
     */
    Key get_hfkey() const;

    /**
     * Set the min fence key for this leaf
     */
    void set_lfkey(Key key);

    /**
     * Set the max fence key for this leaf
     */
    void set_hfkey(Key key);

    /**
     * Verify the search key belongs to interval identified by the fence keys of the given segment
     * @param segment_id [in/out] when false, updated to the next gate_id to acquire
     */
    bool check_fence_keys(int64_t& segment_id, Key search_key) const;

    /**
     * Lock this leaf for exclusive use of a single rebalancer
     */
    void lock();

    /**
     * Unlock this leaf
     */
    void unlock();

    /**
     * Mark the leaf being used by a rebalancer
     */
    void set_active(bool value);

    /**
     * Check whether a rebalancer is already busy on this leaf
     */
    bool is_active() const;

    /**
     * Append a rebalancer in the waiting list
     */
    void wait(std::promise<void>* producer);

    /**
     * Wake the next rebalancer in the waiting list
     */
    void wake_next();

    /**
     * Reference counting
     */
    void incr_ref_count();
    void decr_ref_count();
    void decr_ref_count(gc::GarbageCollector* garbage_collector); // explicitly provide a GC instance
    uint64_t ref_count() const; // access, only for testing purposes

    // Dump the whole content of this leaf to the output stream, for debugging purposes
    static void dump_and_validate(std::ostream& out, Context& context, bool* integrity_check);
};

/**
 * Create a new instance of a leaf
 */
Leaf* create_leaf();

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/

inline
Segment* Leaf::get_segment(uint64_t segment_id) const {
    assert(segment_id < num_segments());
    Segment* base_addr = const_cast<Segment*>(reinterpret_cast<const Segment*>(this + 1));
    return base_addr + segment_id;
}

inline
uint64_t Leaf::num_segments() const {
    return context::StaticConfiguration::memstore_num_segments_per_leaf;
}

inline
void Leaf::lock(){
    m_latch.lock_write();
}

inline
void Leaf::unlock(){
    m_latch.unlock_write();
}

inline
void Leaf::set_active(bool value){
    m_active = value;
}

inline
bool Leaf::is_active() const {
    return m_active;
}

inline
void Leaf::wait(std::promise<void>* producer){
    m_queue.append(producer);
}

inline
void Leaf::wake_next(){
    if(!m_queue.empty()){
        m_queue[0]->set_value();
        m_queue.pop();
    }
}

} // namespace

