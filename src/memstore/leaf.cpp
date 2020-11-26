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

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <iomanip>
#include <ostream>
#include <string>
#include <stdexcept>

#include "teseo/context/global_context.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/profiler/scoped_timer.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

#if defined(DEBUG)
static atomic<int64_t> g_debug_num_invocations_create = 0;
static atomic<int64_t> g_debug_num_invocations_destroy = 0;
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
Leaf::Leaf(uint32_t num_segments) : m_num_segments(num_segments), m_fence_key (KEY_MAX) {

}

Leaf::~Leaf(){

}

Leaf* create_leaf(uint64_t num_segments){
    profiler::ScopedTimer profiler { profiler::LEAF_CREATE };
    Leaf* leaf = internal::allocate_leaf(num_segments);

    Segment* base_segment = reinterpret_cast<Segment*>(leaf + 1);
    uint64_t* base_file = reinterpret_cast<uint64_t*>(base_segment + num_segments);

    // init the segments
    for(uint64_t i = 0; i < num_segments; i++){
        new (base_segment +i) Segment();
        new (base_file + i * context::StaticConfiguration::memstore_segment_size) SparseFile();
    }

    return leaf;
}

namespace internal {
Leaf* allocate_leaf(uint64_t num_segments){
    assert(num_segments <= numeric_limits<uint32_t>::max() && "Type overflow, num_segments is ultimately stored into a uint32_t");

    const uint64_t space_required = sizeof(Leaf) + Leaf::data_size_bytes(num_segments) * 2 /* x2 = vertices/edges + weights */;
//    int rc = posix_memalign(&heap, /* alignment = */ 2097152ull /* 2MB */,  /* size = */ space_required); // with huge pages
//    int rc = posix_memalign(&heap, /* alignment = */ 1ull << 12 /* 4 Kb */,  /* size = */ space_required); // with the buffer manager
    void* heap = malloc(space_required);
    if(heap == nullptr) throw std::runtime_error("[create_leaf] cannot obtain a chunk of aligned memory");
    Leaf* leaf = new (heap) Leaf(num_segments);

    COUT_DEBUG("leaf: " << heap << ", "
               "num segments: " << leaf->num_segments() << ", "
               "allocation size: " << space_required << " bytes, "
               "num invocations: " << ++g_debug_num_invocations_create << ", "
               "total leaves: " << (g_debug_num_invocations_create - g_debug_num_invocations_destroy)
               );

    return leaf;
} // method
} // namespace

void Leaf::destroy_leaf(Leaf* leaf){
    if(leaf != nullptr){
        COUT_DEBUG("leaf: " << leaf << ", "
                   "num invocations: " << ++g_debug_num_invocations_destroy << ", "
                   "total leaves: " << (g_debug_num_invocations_create - g_debug_num_invocations_destroy));

        Segment* base_segment = reinterpret_cast<Segment*>(leaf + 1);
        uint64_t* base_file = reinterpret_cast<uint64_t*>(base_segment + leaf->num_segments());
        for(uint64_t i = 0; i < leaf->num_segments(); i++){
            Segment* segment = base_segment + i;
            uint64_t* file = base_file + i * context::StaticConfiguration::memstore_segment_size;

            if(segment->is_sparse()){
                SparseFile* sf = reinterpret_cast<SparseFile*>(file);
                sf->~SparseFile();
            } else {
                DenseFile* df = reinterpret_cast<DenseFile*>(file);
                df->~DenseFile();
            }

            segment->~Segment();
        }

        leaf->~Leaf();

        free(leaf);
    }
}

namespace internal {
void deallocate_leaf(Leaf* leaf){
    if(leaf != nullptr){
        leaf->~Leaf();
        free(leaf);
    }
}
} // namespace

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

FenceKeysDirection Leaf::check_fence_keys(int64_t segment_id, Key search_key) const noexcept {
    Segment* segment = get_segment(segment_id);

    // check the low fence key
    Key lfkey = segment->m_fence_key;
    if(lfkey == KEY_MAX) { // this array is not valid anymore, restart the operation
        return FenceKeysDirection::INVALID;
    } else if (search_key < lfkey){ // left direction
        return FenceKeysDirection::LEFT;
    }

    // check the high fence key
    Key hfkey = (segment_id +1 == (int64_t) num_segments()) ? get_hfkey() : get_segment(segment_id +1)->m_fence_key;
    if(search_key >= hfkey){ // right direction
        return FenceKeysDirection::RIGHT;
    }

    return FenceKeysDirection::OK;
}

bool Leaf::is_first() const {
    return get_lfkey() == Key::min();
}

/*****************************************************************************
 *                                                                           *
 *   Reference counting                                                      *
 *                                                                           *
 *****************************************************************************/

void Leaf::incr_ref_count(){
    m_ref_count++;
}

void Leaf::decr_ref_count(){
    assert(m_ref_count > 0 && "Underflow");
    if(--m_ref_count == 0){
        context::thread_context()->gc_mark(this, (void (*)(void*)) destroy_leaf);
    }
}

void Leaf::decr_ref_count(gc::GarbageCollector* garbage_collector) {
    assert(m_ref_count > 0 && "Underflow");
    if(--m_ref_count == 0){
        garbage_collector->mark(this, (void (*)(void*)) destroy_leaf);
    }
}

uint64_t Leaf::ref_count() const {
    return m_ref_count;
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void Leaf::dump_and_validate(std::ostream& out, Context& context, bool* integrity_check){
    assert(context.m_tree != nullptr && "Memstore not set");
    assert(context.m_leaf != nullptr && "Leaf not set");
    assert(context.m_segment == nullptr && "Segment already set");

    Leaf* leaf = context.m_leaf;
    out << "[LEAF] " << leaf << ", num segments: " << leaf->num_segments() << ", "
            "fence keys: [" << leaf->get_lfkey() << ", " << leaf->get_hfkey() << "), "
            "rebalancer active: " << boolalpha << leaf->m_active << ", "
            "reference count: " << leaf->m_ref_count << "\n";
    for(uint64_t i = 0; i < leaf->num_segments(); i++){
        context.m_segment = leaf->get_segment(i);
        Segment::dump_and_validate(out, context, integrity_check);
    }
    context.m_segment = nullptr;
}

void Leaf::dump(Memstore* root) {
    Context context ( root );
    context.m_leaf = this;
    dump_and_validate(cout, context, nullptr);
}


#if defined(DEBUG)
namespace {
struct LeafDumpProperties {
    LeafDumpProperties(){
        constexpr uint64_t num_segments = context::StaticConfiguration::memstore_segment_size;
        const uint64_t min_space_required = sizeof(Leaf) + Leaf::data_size_bytes(num_segments/2) * 2 /* x2 = vertices/edges + weights */;
        const uint64_t max_space_required = sizeof(Leaf) + Leaf::data_size_bytes(num_segments) * 2 /* x2 = vertices/edges + weights */;

        COUT_DEBUG("leaf header size: " << sizeof(Leaf) << " bytes, "
                   "segment header size: " << sizeof(Segment) << " bytes, "
                   "words per segment: " << context::StaticConfiguration::memstore_segment_size << ", "
                   "num segments: [" << num_segments/2 << ", " << num_segments << "], "
                   "min allocated size: " << min_space_required << " bytes, "
                   "max allocated size: " << max_space_required << " bytes");
    }

    ~LeafDumpProperties(){
        if(g_debug_num_invocations_create == g_debug_num_invocations_destroy){
            COUT_DEBUG("num invocations to create/destroy leaf: " << g_debug_num_invocations_create);
        } else {
            COUT_DEBUG("num allocated leaves: " << (g_debug_num_invocations_create - g_debug_num_invocations_destroy)  << " (0 expected), "
                       "num invocations to create_leaf: " << g_debug_num_invocations_create << ", "
                       "num invocations to destroy_leaf: " << g_debug_num_invocations_destroy)
        }
    }
};
static LeafDumpProperties _leaf_dump_size;
} // anonymous namespace
#endif /* end if defined(DEBUG) */

} // teseo::memstore
