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

#include <cassert>
#include <future>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/util/latch.hpp"
#include "teseo/util/circular_array.hpp"

namespace teseo::memstore {

/**
 * A leaf of the Fat Tree. It consists of a sequence of segments of a sparse array.
 */
class Leaf {
    friend Leaf* create_leaf();
    friend void destroy_leaf(Leaf*);
    Leaf(); // use create_leaf();
    ~Leaf(); // use destroy_leaf();

    Leaf(const Leaf&) = delete;
    Leaf& operator=(const Leaf&) = delete;



    util::Latch m_latch; // acquired when a thread needs to rebalance more segments than those contained in a single gate
    bool m_active = false; // true if a rebalancer is currently exploring multiple gates
    util::CircularArray<std::promise<void>*> m_queue; // additional rebalancers requesting access to the chunk
    Key m_fence_key; // the max fence key for this leaf

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

    // Dump the whole content of this leaf to the output stream, for debugging purposes
    static void dump_and_validate(std::ostream& out, Context& context, bool* integrity_check);
};

/**
 * Create a new instance of a leaf
 */
Leaf* create_leaf();

/**
 * Destroy an existing instance of a leaf
 */
void destroy_leaf(Leaf* addr);


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

} // namespace

