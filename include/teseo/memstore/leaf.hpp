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

class Leaf; // forward declaration
class Memstore; // forward declaration
namespace internal { // only used for testing
    Leaf* allocate_leaf(uint64_t num_segments = context::StaticConfiguration::memstore_max_num_segments_per_leaf);
    void deallocate_leaf(Leaf* leaf);
} // namespace internal


/**
 * The return code (rc) from the method #check_fence_keys
 */
enum class FenceKeysDirection {
    INVALID, // the segment is not valid anymore (abort)
    LEFT, // proceed backwards (segment -1)
    OK, // correct segment
    RIGHT, // proceed forwards (segment +1)
};

/**
 * A leaf of the Fat Tree. It consists of a sequence of segments of a sparse array.
 */
class Leaf {
    friend Leaf* internal::allocate_leaf(uint64_t num_segments);
    friend void internal::deallocate_leaf(Leaf* leaf);

    Leaf(uint32_t num_segments); // use create_leaf();
    ~Leaf(); // use destroy_leaf();

    Leaf(const Leaf&) = delete;
    Leaf& operator=(const Leaf&) = delete;

    util::Latch m_latch; // acquired when a thread needs to rebalance more segments than those contained in a single gate
    bool m_active = false; // true if a rebalancer is currently exploring multiple gates
    const uint32_t m_num_segments; // number of segments in this leaf
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
     * Get the total space dedicated to the keys / values in the leaf
     */
    static uint64_t data_size_bytes(uint64_t num_segments); // in terms of bytes
    static uint64_t data_size_qwords(uint64_t num_segments); // in terms of qwords (8 bytes)

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
     */
    FenceKeysDirection check_fence_keys(int64_t segment_id, Key search_key) const noexcept;

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
     * Check whether this is the first leaf in the fat tree
     */
    bool is_first() const;

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

    // Dump the content of this leaf
    void dump(Memstore* root);
};

/**
 * Create a new instance of a leaf
 */
Leaf* create_leaf(uint64_t num_segments = context::StaticConfiguration::memstore_max_num_segments_per_leaf);


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/

inline
Segment* Leaf::get_segment(uint64_t segment_id) const {
    assert(segment_id < num_segments() && "Invalid segment ID");
    Segment* base_addr = const_cast<Segment*>(reinterpret_cast<const Segment*>(this + 1));
    return base_addr + segment_id;
}

inline
uint64_t Leaf::num_segments() const {
    return m_num_segments;
}

inline
uint64_t Leaf::data_size_bytes(uint64_t num_segments) {
    constexpr uint64_t segment_size = context::StaticConfiguration::memstore_segment_size * sizeof(uint64_t);
    return num_segments * (sizeof(Segment) + segment_size);
}

inline
uint64_t Leaf::data_size_qwords(uint64_t num_segments) {
    return data_size_bytes(num_segments) / sizeof(uint64_t);
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

