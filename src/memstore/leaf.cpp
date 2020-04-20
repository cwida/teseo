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

#include "teseo/memstore/leaf.hpp"

#include <cstdlib>
#include <stdexcept>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/util/debug.hpp"

namespace teseo::memstore {


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
Leaf::Leaf(){
    m_fence_key = KEY_MAX;
}

Leaf::~Leaf(){

}

Leaf* create_leaf(){
    profiler::ScopedTimer profiler { profiler::SA_ALLOCATE_CHUNK };

    constexpr uint64_t num_segments_per_leaf = context::StaticConfiguration::memstore_num_segments_per_leaf;
    constexpr uint64_t segment_size = context::StaticConfiguration::memstore_segment_size * sizeof(uint64_t);
    uint64_t space_required = sizeof(Leaf) + num_segments_per_leaf * (sizeof(Segment) + segment_size);

    void* heap { nullptr };
    int rc = posix_memalign(&heap, /* alignment = */ 2097152ull /* 2MB */,  /* size = */ space_required);
    if(rc != 0) throw std::runtime_error("[create leaf] cannot obtain a chunk of aligned memory");
    Leaf* leaf = new (heap) Leaf();


    Segment* base_segment = reinterpret_cast<Segment*>(leaf + 1);
    uint64_t* base_file = reinterpret_cast<uint64_t*>(base_segment + context::StaticConfiguration::memstore_num_segments_per_leaf);
//    return reinterpret_cast<SparseFile*>(base_file + segment_id() * context::StaticConfiguration::memstore_segment_size);

    // init the segments
    for(uint64_t i = 0; i < num_segments_per_leaf; i++){
        new (base_segment +i) Segment();
        new (base_file + i * context::StaticConfiguration::memstore_num_segments_per_leaf) SparseFile();
    }

    COUT_DEBUG("leaf: " << leaf);
    return leaf;
}

void destroy_leaf(Leaf* leaf){
    if(leaf != nullptr){
        COUT_DEBUG("leaf: " << chunk);

        for(uint64_t i = 0; i < leaf->num_segments(); i++){
            Segment* segment = leaf->get_segment(i);
            segment->~Segment();
        }

        leaf->~Leaf();

        free(leaf);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Fence keys                                                              *
 *                                                                           *
 *****************************************************************************/
Key Leaf::get_lfkey() const {
    return get_segment(0)->m_fence_key;
}

Key Leaf::get_hfkey() const {
    return m_fence_key;
}

void Leaf::set_lfkey(Key key) {
    get_segment(0)->m_fence_key = key;
}

void Leaf::set_hfkey(Key key) {
    m_fence_key = key;
}

bool Leaf::check_fence_keys(int64_t& segment_id, Key search_key) const {
    Segment* segment = get_segment(segment_id);

    // check the low fence key
    Key lfkey = segment->m_fence_key;
    if(lfkey == KEY_MAX) { // this array is not valid anymore, restart the operation
        throw Abort {};
    } else if (search_key < lfkey){ // left direction
        segment_id--;
        if(segment_id < 0){
            throw Abort{}; // go to the previous leaf
        } else {
            return false;
        }
    }

    // check the high fence key
    Key hfkey = (segment_id +1 == num_segments()) ? get_hfkey() : get_segment(segment_id +1)->m_fence_key;
    if(search_key >= hfkey){ // right direction
        segment_id++;
        if(segment_id >= num_segments()){
            throw Abort{}; // fetch the next leaf
        } else {
            return false;
        }
    }

    assert(lfkey <= search_key && search_key < hfkey);
    return true;
}

}
