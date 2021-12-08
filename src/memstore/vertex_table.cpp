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

#include <cassert>
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

//#define DEBUG
#include "teseo/util/debug.hpp"

// GCC only: it complains that Element is not trivially copyable. Which it's okay, as to avoid data races there is a special purpose
// copy ctor/assignment. However, we use memcpy in places where it is safe to copy it.
//#if defined(__GNUC__) && __GNUC__ >= 8 /* GCC only */
//#pragma GCC diagnostic ignored "-Wclass-memaccess"
//#endif

using namespace std;

namespace teseo::memstore {

constexpr uint64_t NUM_NODES = context::StaticConfiguration::numa_num_nodes;

VertexTable::VertexTable() : m_num_entries(context::StaticConfiguration::vertex_table_min_capacity), m_num_elts(0), m_num_tombstones(0), m_latch(0) {
    static_assert(sizeof(Entry) == 48 /* bytes */, "To ensure the value is always aligned to 16 bytes in the hash table"); // otherwise => SEGV

    for(uint64_t i = 0; i < NUM_NODES; i++){
        std::tie(m_allocations[i], m_hashtables[i]) = allocate_hash_table(m_num_entries, i);
    }

    assert(reinterpret_cast<uint64_t>(&(m_vertex1.m_scalar)) % 16 == 0 && "The field must aligned to 16 bytes for the CAS instructions");
    m_vertex1.m_scalar = 0;
}

VertexTable::~VertexTable(){
    clear();
}

auto VertexTable::allocate_hash_table(uint64_t num_entries, int numa_node) -> std::pair<void*, Entry*> {
    uint64_t* pointer = nullptr;
    if(context::StaticConfiguration::numa_enabled){
        pointer = (uint64_t*) util::NUMA::malloc((num_entries) * sizeof(Entry) + /* alignment */ sizeof(uint64_t), numa_node);
    } else {
        assert(numa_node == 0 && "libnuma is not enabled, we cannot allocate the hash table into a specific node");
        pointer = (uint64_t*) util::NUMA::malloc((num_entries) * sizeof(Entry) + /* alignment */ sizeof(uint64_t));
    }
    if(pointer == nullptr) throw std::bad_alloc{};
    assert(reinterpret_cast<uint64_t>(pointer) % 8 == 0 && "All memory allocations are always aligned by 8 bytes");
    Entry* table = reinterpret_cast<Entry*>( reinterpret_cast<uint64_t>(pointer) % 16 == 0 ? pointer : pointer +1 );
    assert(reinterpret_cast<uint64_t>(table) % 16 == 0 && "The hash table must be aligned by 16 bytes, to ensure the CAS on 128 bits");
    memset(table, 0, (num_entries) * sizeof(Entry));

    return make_pair(pointer, table);
}


void VertexTable::clear(){
    if(m_hashtables[0] == nullptr) return; // already cleared

    remove_vertex1(); // special case

    // decrease the number of pointers to the leaves
    Entry* __restrict table = m_hashtables[0];
    for(uint64_t i = 0; i < m_num_entries; i++){
        reset_elt(table[i].m_key1, table[i].m_value1);
        reset_elt(table[i].m_key2, table[i].m_value2);
    }

    // release back the memory used from the hash tables
    for(uint64_t i = 0; i < NUM_NODES; i++){
        util::NUMA::free(m_allocations[i]);
        m_hashtables[i] = nullptr;
        m_allocations[i] = nullptr;
    }

    m_num_entries = 0;
    m_num_elts = 0;
    m_num_tombstones = 0;
}

void VertexTable::reset_elt(uint64_t& /* in/out */ key, CompressedDirectPointer& /* in/out */ value) {
    if(key != EMPTY && key != TOMBSTONE){
        key = EMPTY;
        value.m_scalar = 0;
    }
}

// Based on growt - https://github.com/TooBiased/growt.git
[[maybe_unused]] static constexpr uint64_t hashf_seed0 = 12923598712359872066ull;
[[maybe_unused]] static constexpr uint64_t hashf_seed1 = hashf_seed0 * 7467732452331123588ull;
uint64_t VertexTable::hashf(uint64_t vertex_id, uint64_t capacity) { // crc
    assert(vertex_id != TOMBSTONE && "This vertex should be stored in the special entry m_vertex1");

#if defined(__SSE4_2__)
    return ( __builtin_ia32_crc32di(vertex_id, hashf_seed0) | (__builtin_ia32_crc32di(vertex_id, hashf_seed1) << 32)) % capacity;
#else
    return vertex_id % capacity;
#endif
}

DirectPointer VertexTable::get(uint64_t vertex_id, uint64_t numa_node) const noexcept {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "The thread must be inside an epoch");
    assert(numa_node < NUM_NODES && "Invalid node");
    if(vertex_id == TOMBSTONE) { return get_vertex1(); } // special case

    uint64_t num_entries = 0;
    Entry* __restrict table = nullptr;
    uint64_t v0, v1;

    do {
        v0 = m_latch & MASK_VERSION;
        util::compiler_barrier();
        num_entries = m_num_entries;
        table = m_hashtables[numa_node];
        util::compiler_barrier();
        v1 = m_latch & MASK_VERSION;
    } while(v0 != v1);

    uint64_t h0 = hashf(vertex_id, /* capacity, total number of elts */ num_entries * 2);
    uint64_t h = h0/2; // entry id

    // probe the second element of the first entry
    if( h0 % 2 != 0 ){
        if(table[h].m_key2 == vertex_id){
            return load(table[h].m_value2);
        } else if(table[h].m_key2 == EMPTY){
            return DirectPointer{}; // not found
        } else {
            h ++;
            if(h >= num_entries) h = 0;
        }
    }

    // probe the remaining entries
    while(true){
        if(table[h].m_key1 == vertex_id){
            return load(table[h].m_value1);
        } else if(table[h].m_key1 == EMPTY){
            return DirectPointer{}; // not found
        } else if(table[h].m_key2 == vertex_id){
            return load(table[h].m_value2);
        } else if(table[h].m_key2 == EMPTY){
            return DirectPointer{}; // not found
        } else {
            h++;
            if(h >= num_entries) h = 0;
        }
    }
}

void VertexTable::upsert(uint64_t vertex_id, const DirectPointer& dptr_new) noexcept {
    assert(dptr_new.leaf() != nullptr && "Pointer to a leaf not set");
    if(vertex_id == TOMBSTONE){ return upsert_vertex1(dptr_new); } // special case
    if(fill_factor() > context::StaticConfiguration::vertex_table_max_fill_factor){ resize(); }
    // There is no need to lock anything here, only the Merger service can invoke this method and there
    // is only one of these guys around.

    CompressedDirectPointer cdptr_new = dptr_new.compress();

    // find the position where to insert the element
    uint64_t h0 = hashf(vertex_id, /* capacity, total number of elts */ m_num_entries * 2);
    for(uint64_t numa_node = 0; numa_node < NUM_NODES; numa_node ++){
        Entry* table = m_hashtables[numa_node];
        uint64_t h = h0 /2;

        bool done = false;

        // second element of the first entry
        if( h0 % 2 != 0 ){
            if(table[h].m_key2 == vertex_id){ // update
                store(table[h].m_value2, cdptr_new);
                done = true;
            } else if(table[h].m_key2 <= TOMBSTONE /* 1 */){ // insert
                store(table[h].m_value2, cdptr_new);
                util::compiler_barrier();
                table[h].m_key2 = vertex_id;
                m_num_elts++;
                done = true;
            } else { // next
                h = (h +1) % m_num_entries;
            }
        }

        // remaining entries
        while(!done){
            if(table[h].m_key1 == vertex_id){ // update
                store(table[h].m_value1, cdptr_new);
                done = true;
            } else if(table[h].m_key1 <= TOMBSTONE /* 1 */){ // insert
                store(table[h].m_value1, cdptr_new);
                util::compiler_barrier();
                table[h].m_key1 = vertex_id;
                m_num_elts++;
                done = true;
            } else if(table[h].m_key2 == vertex_id){ // I mean, what are the chances of confusing key1 and key2 in this menial as duplicated code
                store(table[h].m_value2, cdptr_new);
                done = true;
            } else if(table[h].m_key2 <= TOMBSTONE /* 1 */){ // insert
                store(table[h].m_value2, cdptr_new);
                util::compiler_barrier();
                table[h].m_key2 = vertex_id;
                m_num_elts++;
                done = true;
            } else { // next
                h = (h +1) % m_num_entries;
            }
        }
    }
}

bool VertexTable::update(uint64_t vertex_id, const DirectPointer& dptr_new) noexcept {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "The thread must be inside an epoch");
    assert(dptr_new.leaf() != nullptr && "Pointer to a leaf not set");
    if(vertex_id == TOMBSTONE){ return update_vertex1(dptr_new); } // special case

    CompressedDirectPointer cdptr_new = dptr_new.compress();
    uint64_t numa_node =0;
    uint64_t v0 /* version start */, v1 /* version end */;

    while(numa_node < NUM_NODES){
        __atomic_load(&m_latch, &v0, /* whatever */ __ATOMIC_SEQ_CST);
        v0 = v0 & MASK_VERSION;
        if(v0 % 2 == 1){ wait(); continue; }// resize in progress

        // optimistic latch
        uint64_t num_entries = m_num_entries;
        Entry* __restrict table = m_hashtables[numa_node];
        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        if(v0 != v1) continue; // restart

        uint64_t h0 = hashf(vertex_id, /* capacity, total number of elts */ num_entries * 2);
        uint64_t h = h0/2;

        bool done = false;

        // second element of the first entry
        if(h0 % 2 != 0){
            if(table[h].m_key2 == vertex_id){
                store(table[h].m_value2, cdptr_new);
                done = true;
            } else if(table[h].m_key2 == EMPTY){
                assert(numa_node == 0 && "It must be missing in all NUMA nodes because the element can only be inserted by a single writer in mutual exclusion");
                return false; // nothing to update => the element is not present
            } else { // skip
                h = (h +1) % num_entries;
            }
        }

        // remaining entries
        while(!done){
            if(table[h].m_key1 == vertex_id){
                store(table[h].m_value1, cdptr_new);
                done = true;
            } else if(table[h].m_key1 == EMPTY){
                assert(numa_node == 0 && "It must be missing in all NUMA nodes because the element can only be inserted by a single writer in mutual exclusion");
                return false; // nothing to update => the element is not present
            } else if(table[h].m_key2 == vertex_id){
                store(table[h].m_value2, cdptr_new);
                done = true;
            } else if(table[h].m_key2 == EMPTY){
                assert(numa_node == 0 && "It must be missing in all NUMA nodes because the element can only be inserted by a single writer in mutual exclusion");
                return false; // nothing to update => the element is not present
            } else { // skip
                h = (h +1) % num_entries;
            }
        } // end while (!done)

        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        if(v0 != v1) continue; // restart

        // next node
        numa_node++;
    }

    return true;
}

void VertexTable::remove(uint64_t vertex_id) noexcept {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "The thread must be inside an epoch");
    if(vertex_id == TOMBSTONE){ return remove_vertex1(); } // special case

    uint64_t numa_node = 0;
    uint64_t v0 /* version start */, v1 /* version end */;

    while(numa_node < NUM_NODES){
        __atomic_load(&m_latch, &v0, /* whatever */ __ATOMIC_SEQ_CST);
        v0 = v0 & MASK_VERSION;
        if(v0 % 2 == 1){ wait(); continue; } // resize in progress

        // optimistic latch
        uint64_t num_entries = m_num_entries;
        Entry* __restrict table = m_hashtables[numa_node];
        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        v1 &= MASK_VERSION;
        if(v0 != v1) continue; // restart

        uint64_t h0 = hashf(vertex_id, /* capacity, total number of elts */ num_entries *2);
        uint64_t h = h0/2;
        bool done = false;

        // second element of the first entry
        if( h0 % 2 != 0){
            if(table[h].m_key2 == vertex_id){
                util::atomic_store_16(table[h].m_value2.m_scalar, 0);
                util::compiler_barrier();
                table[h].m_key2 = TOMBSTONE;
                done = true;
            } else if(table[h].m_key2 == EMPTY){
                return; // nothing to update => the element is not present
            } else { // skip tombstones as well
                h = (h +1) % num_entries;
            }
        }

        // remaining entries
        while(!done){
            if(table[h].m_key1 == vertex_id){
                util::atomic_store_16(table[h].m_value1.m_scalar, 0);
                util::compiler_barrier();
                table[h].m_key1 = TOMBSTONE;
                done = true;
            } else if(table[h].m_key1 == EMPTY){
                return; // nothing to update => the element is not present
            } else if(table[h].m_key2 == vertex_id){
                util::atomic_store_16(table[h].m_value2.m_scalar, 0);
                util::compiler_barrier();
                table[h].m_key2 = TOMBSTONE;
                done = true;
            } else if(table[h].m_key2 == EMPTY){
                return; // nothing to update => the element is not present
            } else { // skip tombstones as well
                h = (h +1) % num_entries;
            }
        } // end while (!done)

        __atomic_load(&m_latch, &v1, /* whatever */ __ATOMIC_SEQ_CST);
        v1 &= MASK_VERSION;
        if(v0 != v1) continue; // restart

        // next node
        numa_node++;
    }

    m_num_tombstones++; // it can be an approximate count
}

DirectPointer VertexTable::get_vertex1() const noexcept {
    return load(m_vertex1);
}

bool VertexTable::has_vertex1() const noexcept {
    return m_vertex1.m_scalar != static_cast<unsigned __int128>(0);
}

void VertexTable::upsert_vertex1(const DirectPointer& dptr_new) noexcept {
    store(m_vertex1, dptr_new.compress());
}

bool VertexTable::update_vertex1(const DirectPointer& dptr_new) noexcept {
    // write - write conflicts cannot occur, the atomicity on m_vertex1 is only to protect from write - read conflicts
    if(has_vertex1()) {
        store(m_vertex1, dptr_new.compress());
        return true;
    } else {
        return false;
    }
}

void VertexTable::remove_vertex1() noexcept {
    if(has_vertex1()){
        util::atomic_store_16(m_vertex1.m_scalar, 0);
    }
}

void VertexTable::resize(){
    xlock();
    do_resize();
    xunlock();
}

void VertexTable::do_resize(){
    Entry* hashtables[context::StaticConfiguration::numa_num_nodes];
    void* allocations[context::StaticConfiguration::numa_num_nodes];
    uint64_t num_entries_new = std::max<int64_t>(static_cast<double>(m_num_elts - m_num_tombstones) /2.0 / 0.3, context::StaticConfiguration::vertex_table_min_capacity);
    std::tie(allocations[0], hashtables[0]) = allocate_hash_table(num_entries_new, 0);
    Entry* table_new = hashtables[0];
    Entry* table_old = m_hashtables[0];

    // copy the elements from the old table to the new table
    auto set_elt_table_new = [table_new, num_entries_new](uint64_t key, CompressedDirectPointer value){
        uint64_t h0 = hashf(key, /* capacity, total number of elts */ num_entries_new *2);
        uint64_t h = h0/2;
        bool done = false;

        if(h0%2 != 0){
            if(table_new[h].m_key2 == EMPTY){
                table_new[h].m_key2 = key;
                table_new[h].m_value2 = value;
                done = true;
            } else {
                h = (h+1) % num_entries_new;
            }
        }

        while(!done){
            if(table_new[h].m_key1 == EMPTY){
                table_new[h].m_key1 = key;
                table_new[h].m_value1 = value;
                done = true;
            } else if (table_new[h].m_key2 == EMPTY){
                table_new[h].m_key2 = key;
                table_new[h].m_value2 = value;
                done = true;
            } else {
                h = (h+1) % num_entries_new;
            }
        }
    };

    uint64_t num_elts_new = 0;
    for(uint64_t i = 0; i < m_num_entries; i++){
        if(table_old[i].m_key1 > TOMBSTONE){
            set_elt_table_new(table_old[i].m_key1, table_old[i].m_value1);
            num_elts_new++;
        }
        if(table_old[i].m_key2 > TOMBSTONE){
            set_elt_table_new(table_old[i].m_key2, table_old[i].m_value2);
            num_elts_new++;
        }
    }

    // clone the remaining tables
    for(uint64_t numa_node = 1; numa_node < NUM_NODES; numa_node++){
        std::tie(allocations[numa_node], hashtables[numa_node]) = allocate_hash_table(num_entries_new, numa_node);
        memcpy(hashtables[numa_node], table_new, num_entries_new * sizeof(Entry));
    }

    // swap the pointers for the new tables
    m_num_entries = std::min<uint64_t>(num_entries_new, m_num_entries); // avoid overflows
    util::compiler_barrier();
    auto gc = context::global_context()->gc();
    for(uint64_t numa_node = 0; numa_node < NUM_NODES; numa_node++){
        gc->mark(m_allocations[numa_node], util::NUMA::free);
        m_hashtables[numa_node] = hashtables[numa_node];
        m_allocations[numa_node] = allocations[numa_node];
    }
    util::compiler_barrier();
    m_num_entries = num_entries_new;
    m_num_elts = num_elts_new;
    m_num_tombstones = 0;
}

double VertexTable::fill_factor() const{
    return static_cast<double>(m_num_elts - m_num_tombstones) / (m_num_entries *2);
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

CompressedDirectPointer VertexTable::load(CompressedDirectPointer& variable) {
    CompressedDirectPointer cdptr;
    cdptr.m_scalar = util::atomic_load_16(variable.m_scalar);
    return cdptr;
}

void VertexTable::store(CompressedDirectPointer& variable, CompressedDirectPointer value){
    util::atomic_store_16(variable.m_scalar, value.m_scalar);
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
    cout << "[VertexTable] num entries: " << m_num_entries << ", num elts: " << m_num_elts << ", num tombstones: " << m_num_tombstones << ", "
            "latch: " << m_latch << ", waiting list size: " << m_queue.size() << ", numa nodes: " << NUM_NODES << ", fill factor: " << fill_factor();
    if(has_vertex1()){
        cout << ", vertex1 not set" << endl;
    } else {
        DirectPointer dptr = m_vertex1;
        cout << "\n  vertex1: " << dptr << endl;
    }

    for(uint64_t numa_node = 0; numa_node < NUM_NODES; numa_node++){
        Entry* table = m_hashtables[numa_node];
        cout << "-- Node #" << numa_node << ":\n";
        for(uint64_t i = 0; i < m_num_entries; i++){
            DirectPointer dptr1 { table[i].m_value1 };
            cout << "[" << i << ", h: " << (i*2) << "] key: " << table[i].m_key1 << ", value: " << dptr1 << "\n";
            DirectPointer dptr2 { table[i].m_value2 };
            cout << "[" << i << ", h: " << (i*2 +1) << "] key: " << table[i].m_key2 << ", value: " << dptr2 << "\n";
        }
    }
    cout << endl;
}

} // namespace


