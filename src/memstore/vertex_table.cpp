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

#include "teseo/memstore/vertex_table.hpp"

#include <iostream>
#include <limits>
#include <sstream>
#include <string>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/compiler.hpp"
#include "teseo/util/numa.hpp"

// GCC only: it complains that Element is not trivially copyable. Which it's okay, as to avoid data races there is a special purpose
// copy ctor/assignment. However, we use memcpy in places where it is safe to copy it.
#if defined(__GNUC__) && __GNUC__ >= 8 /* GCC only */
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif

using namespace std;

namespace teseo::memstore {

constexpr uint64_t NUM_NODES = context::StaticConfiguration::numa_num_nodes;

VertexTable::VertexTable() : m_capacity(context::StaticConfiguration::vertex_table_min_capacity), m_num_elts(0), m_num_tombstones(0), m_latch(0) {
    for(uint64_t i = 0; i < NUM_NODES; i++){
        Element* table = nullptr;
        if(context::StaticConfiguration::numa_enabled){
            table = (Element*) util::NUMA::malloc((m_capacity +1) * sizeof(Element) , i);
        } else {
            table = (Element*) util::NUMA::malloc((m_capacity +1) * sizeof(Element));
        }
        if(table == nullptr) throw std::bad_alloc{};

        memset(table, 0, (m_capacity +1) * sizeof(Element));
        m_hashtables[i] = table + 1; // the first position is reserved for the element 1, which has the same value of the tombstone
    }
}

VertexTable::~VertexTable(){
    clear();
}

void VertexTable::clear(){
    if(m_hashtables[0] == nullptr) return; // already cleared

    // decrease the number of pointers to the leaves
    if(m_hashtables[0][-1].m_key == TOMBSTONE){ // this actually the key 1 == TOMBSTONE!
        m_hashtables[0][-1].m_value.leaf()->decr_ref_count();
        m_hashtables[0][-1].m_value.unset();
        m_hashtables[0][-1].m_key = EMPTY;
    }
    Element* __restrict table = m_hashtables[0];
    for(uint64_t i = 0, capacity = m_capacity; i < capacity; i++){
        if(table[i].m_key != EMPTY && table[i].m_key != TOMBSTONE){
            table[i].m_value.leaf()->decr_ref_count();
            table[i].m_value.unset();
            table[i].m_key = EMPTY;
        }
    }

    for(uint64_t i = 0; i < NUM_NODES; i++){
        Element* table = m_hashtables[i] -1;
        util::NUMA::free(table);
        m_hashtables[i] = nullptr;
    }
}

// Based on growt - https://github.com/TooBiased/growt.git
[[maybe_unused]] static constexpr uint64_t hashf_seed0 = 12923598712359872066ull;
[[maybe_unused]] static constexpr uint64_t hashf_seed1 = hashf_seed0 * 7467732452331123588ull;
int64_t VertexTable::hashf(uint64_t vertex_id, uint64_t capacity) { // crc
    if(vertex_id == TOMBSTONE) {
        return -1;
    } else {
        assert((capacity <= (uint64_t) numeric_limits<int64_t>::max()) && "Overflow");
//#if defined(__SSE4_2__)
//        return ( __builtin_ia32_crc32di(vertex_id, hashf_seed0) | (__builtin_ia32_crc32di(vertex_id, hashf_seed1) << 32)) % capacity;
//#else
        return vertex_id % capacity;
//#endif
    }
}

DirectPointer VertexTable::get(uint64_t vertex_id, uint64_t numa_node) const noexcept {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "The thread must be inside an epoch");
    assert(numa_node < NUM_NODES && "Invalid node");

    int64_t capacity = 0;
    Element* __restrict table = nullptr;
    uint64_t v0, v1;

    do {
        v0 = m_latch & MASK_VERSION;
        util::compiler_barrier();
        capacity = m_capacity;
        table = m_hashtables[numa_node];
        util::compiler_barrier();
        v1 = m_latch & MASK_VERSION;
    } while(v0 != v1);

    int64_t h = hashf(vertex_id, capacity);

    while(true){
        if(table[h].m_key == vertex_id){
            return table[h].m_value;
        } else if(table[h].m_key == EMPTY){
            return DirectPointer{}; // not found
        } else { // next item, tombstone or different key
            h ++;
            if(h >= capacity) h = 0;
        }
    }
}

void VertexTable::upsert(uint64_t vertex_id, const DirectPointer& pointer) noexcept {
    if(fill_factor() > context::StaticConfiguration::vertex_table_max_fill_factor){ resize(); }

    Leaf* leaf_old = nullptr; // Remove the old leaf in case of mismatch

    // find the position where to insert the element
    const int64_t capacity = m_capacity;
    int64_t h0 = hashf(vertex_id, capacity);
    for(uint64_t numa_node = 0; numa_node < NUM_NODES; numa_node ++){
        Element* table = m_hashtables[numa_node];
        int64_t h = h0;
        bool done = false;
        while(!done){
            if(table[h].m_key == vertex_id){ // update
                if(numa_node == 0 && pointer.leaf() != table[h].m_value.leaf()){ // reference counting
                    leaf_old = table[h].m_value.leaf();
                    pointer.leaf()->incr_ref_count();
                }
                table[h].m_value = pointer;
                done = true;
            } else if(table[h].m_key <= TOMBSTONE /* 1 */){ // insert
                if(numa_node == 0){ pointer.leaf()->incr_ref_count(); }
                table[h].m_value = pointer;
                util::compiler_barrier();
                table[h].m_key = vertex_id;
                m_num_elts++;
                done = true;
            } else { // next
                h = (h +1) % capacity;
            }
        }
    }

    if(leaf_old != nullptr){
        leaf_old->decr_ref_count();
    }
}

bool VertexTable::update(uint64_t vertex_id, const DirectPointer& pointer) noexcept {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "The thread must be inside an epoch");

    Leaf* leaf_old = nullptr; // remove the old leaf in case of mismatch
    uint64_t numa_node =0;
    uint64_t v0 /* version start */, v1 /* version end */;

    while(numa_node < NUM_NODES){
        __atomic_load(&m_latch, &v0, /* whatever */ __ATOMIC_SEQ_CST);
        v0 = v0 & MASK_VERSION;
        if(v0 % 2 == 1){ wait(); continue; }// resize in progress

        // optimistic latch
        int64_t capacity = m_capacity;
        Element* __restrict table = m_hashtables[numa_node];
        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        if(v0 != v1) continue; // restart

        int64_t h = hashf(vertex_id, capacity);
        bool done = false;
        while(!done){
            if(table[h].m_key == vertex_id){
                if(numa_node == 0 && table[h].m_value.leaf() != pointer.leaf() && leaf_old == nullptr){ // reference counting
                    leaf_old = table[h].m_value.leaf();
                    pointer.leaf()->incr_ref_count();
                }
                table[h].m_value = pointer;
                done = true;
            } else if(table[h].m_key == EMPTY){
                return false; // nothing to update => the element is not present
            } else { // skip
                h = (h +1) % capacity;
            }
        } // end while (!done)

        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        if(v0 != v1) continue; // restart

        // next node
        numa_node++;
    }

    if(leaf_old != nullptr){
        leaf_old->decr_ref_count();
    }

    return true;
}

void VertexTable::remove(uint64_t vertex_id) noexcept {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "The thread must be inside an epoch");

    Leaf* leaf_old = nullptr; // remove the old leaf in case of mismatch
    uint64_t numa_node = 0;
    uint64_t v0 /* version start */, v1 /* version end */;

    while(numa_node < NUM_NODES){
        __atomic_load(&m_latch, &v0, /* whatever */ __ATOMIC_SEQ_CST);
        v0 = v0 & MASK_VERSION;
        if(v0 % 2 == 1){ wait(); continue; } // resize in progress

        // optimistic latch
        int64_t capacity = m_capacity;
        Element* __restrict table = m_hashtables[numa_node];
        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        v1 &= MASK_VERSION;
        if(v0 != v1) continue; // restart

        int64_t h = hashf(vertex_id, capacity);
        bool done = false;
        while(!done){
            if(table[h].m_key == vertex_id || /* special case */ h == -1){
                if(numa_node == 0 && leaf_old == nullptr){ // reference counting
                    leaf_old = table[h].m_value.leaf();
                }
                table[h].m_value.unset();
                util::compiler_barrier();
                table[h].m_key = h >= 0 ? TOMBSTONE : EMPTY;
                done = true;
            } else if(table[h].m_key == EMPTY){
                return; // nothing to update => the element is not present
            } else { // skip tombstones as well
                h = (h +1) % capacity;
            }
        } // end while (!done)

        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        v1 &= MASK_VERSION;
        if(v0 != v1) continue; // restart

        // next node
        numa_node++;
    }

    assert(leaf_old != nullptr && "Element not removed. In case of unsuccess, it should have left this method ahead with <return>");
    m_num_tombstones++; // it can be an approximate count
    leaf_old->decr_ref_count();
}

void VertexTable::resize(){
    xlock();
    do_resize();
    xunlock();
}

void VertexTable::do_resize(){
    Element* hashtables[context::StaticConfiguration::numa_num_nodes];
    int64_t capacity_new = std::max<int64_t>(static_cast<double>(m_num_elts - m_num_tombstones) / 0.3, context::StaticConfiguration::vertex_table_min_capacity);

    Element* table_new { nullptr };
    uint64_t num_elts_new = 0;
    if(context::StaticConfiguration::numa_enabled){
        table_new = (Element*) util::NUMA::malloc((capacity_new +1) * sizeof(Element) , 0);
    } else {
        table_new = (Element*) util::NUMA::malloc((capacity_new +1) * sizeof(Element));
    }
    if(table_new == nullptr) throw std::bad_alloc{};
    memset(table_new, 0, (capacity_new +1) * sizeof(Element));
    table_new = table_new +1;
    hashtables[0] = table_new;
    Element* table_old { m_hashtables[0] };

    // copy the special element at slot -1
    if(table_old[-1].m_key == 1){
        table_new[-1].m_key = 1;
        table_new[-1].m_value = table_old[-1].m_value;
    }

    // copy the elements from the old table to the new table
    for(int64_t i = 0; i < m_capacity; i++){
        uint64_t key = table_old[i].m_key;
        if(key > TOMBSTONE){
            uint64_t h = hashf(key, capacity_new);
            while(table_new[h].m_key != EMPTY){ h = (h +1) % capacity_new; }
            table_new[h].m_key = key;
            table_new[h].m_value = table_old[i].m_value;
            num_elts_new ++;
        }
    }

    // clone the remaining tables
    for(uint64_t numa_node = 1; numa_node < NUM_NODES; numa_node++){
        Element* table2 = (Element*) util::NUMA::malloc((capacity_new +1) * sizeof(Element), numa_node);
        if(table2 == nullptr) throw std::bad_alloc{};
        memcpy(table2, table_new -1, (capacity_new +1) * sizeof(Element));
        hashtables[numa_node] = table2 + 1;
    }

    // swap the pointers for the new tables
    m_capacity = std::min<int64_t>(m_capacity, capacity_new); // avoid overflows
    util::compiler_barrier();
    auto gc = context::global_context()->gc();
    for(uint64_t numa_node = 0; numa_node < NUM_NODES; numa_node++){
        gc->mark(m_hashtables[numa_node] -1, util::NUMA::free);
        m_hashtables[numa_node] = hashtables[numa_node];
    }
    util::compiler_barrier();
    m_capacity = capacity_new;
    m_num_elts = num_elts_new;
    m_num_tombstones = 0;
}

double VertexTable::fill_factor() const{
    return static_cast<double>(m_num_elts - m_num_tombstones) / m_capacity;
}

void VertexTable::wait() {
    uint64_t expected = m_latch;
    bool done = false;
    while(!done){
        uint64_t desired = expected | MASK_XLOCK;
        if((expected & MASK_VERSION) % 2 == 0){
            done = true;
        } else if ((expected & MASK_XLOCK) != 0){
            util::pause();
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
            std::promise<void> producer;
            std::future<void> consumer = producer.get_future();
            m_queue.append(&producer);
            __atomic_store(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

            consumer.wait();

            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST); // reload expected
        }
    }
}

void VertexTable::xlock(){
    uint64_t expected = m_latch;
    bool done = false;
    while(!done){
        uint64_t desired = (expected + 1) & MASK_VERSION;
        assert((desired % 2) == 1 && "Already locked");
        if((expected & MASK_XLOCK) != 0){
            util::pause();
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
            done = true;
        }
    }
}

void VertexTable::xunlock(){
    uint64_t expected = m_latch;
    bool done = false;
    while(!done){
        assert((expected % 2) == 1 && "Otherwise it is already unlocked. The convention is latch mod 2 == 0 ~> unlocked, latch mod 2 == 1 ~> locked");
        uint64_t desired = ((expected + 1) & MASK_VERSION) | MASK_XLOCK;
        if((expected & MASK_XLOCK) != 0){ // spin lock
            util::pause();
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if ( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
            while(!m_queue.empty()){
                m_queue[0]->set_value();
                m_queue.pop();
            }

            desired = desired & ~MASK_XLOCK;
            __atomic_store(&m_latch, &desired, /* whatever */ __ATOMIC_SEQ_CST); // unlock
            done = true;
        }
    }
}


void VertexTable::dump() const {
    cout << "[VertexTable] capacity: " << m_capacity << ", num elts: " << m_num_elts << ", num tombstones: " << m_num_tombstones << ", "
            "latch: " << m_latch << ", waiting list size: " << m_queue.size() << ", numa nodes: " << NUM_NODES << endl;
    for(uint64_t numa_node = 0; numa_node < NUM_NODES; numa_node++){
        Element* table = m_hashtables[numa_node];
        cout << "-- Node #" << numa_node << ":\n";
        for(int64_t i = -1; i < m_capacity; i++){
            cout << "[" << i << "] key: " << table[i].m_key << ", value: " << table[i].m_value << "\n";
        }
    }
    cout << endl;
}

} // namespace


