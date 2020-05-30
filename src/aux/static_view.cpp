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
#include "teseo/aux/static_view.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <iostream>

#if defined(HAVE_NUMA)
#include <numa.h>
#endif

#include "teseo/aux/builder.hpp"
#include "teseo/aux/item.hpp"
#include "teseo/context/property_snapshot.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/numa.hpp"

#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::aux {

StaticView::StaticView(uint64_t num_vertices, const ItemUndirected* degree_vector, const HashParams& hash) :
        View(/* is static ? */ true),
        m_num_vertices(num_vertices), m_degree_vector(degree_vector), m_hash_direct(hash.m_direct), m_hash_capacity(hash.m_capacity), m_hash_const(hash.m_const) {

    if(!hash.m_initialised){
        create_vertex_id_mapping();
    }
}

StaticView::HashParams::HashParams(uint64_t max_vertex_id, uint64_t num_vertices) {

    uint64_t capacity0 = num_vertices * context::StaticConfiguration::aux_hash_multiplier;
    // The hash function is a going to be the modulo over m_hash_const ("the division method"). However, that
    // is, theoretically, rather terrible as we purposedly want the capacity be a power of 2, so that the hash
    // can be computed quickly, with a mask on m_hash_const, and close value for the vertices end up in subsequent
    // slots, to take advantage of the sequential locality of scans.
    // If, upon profiling, this approach does not work, we can readdress the hash function.
    // For the time being, let's compute the next power of 2:
    uint64_t power = ceil(log2(capacity0));
    uint64_t capacity = (1<<power) *2 /* x2 as we store both the key and the value in the hash map */;
    if(context::StaticConfiguration::aux_direct_table_enabled && capacity > max_vertex_id){
        m_direct = true;
        m_capacity = max_vertex_id +1;
        m_const = std::numeric_limits<uint64_t>::max(); // all 1
    } else {
        m_direct = false;
        m_capacity = capacity;
        m_const = (m_capacity /2) -1;
    }
    m_initialised = false;
}

StaticView::~StaticView(){
    util::NUMA::free((void*) m_degree_vector); m_degree_vector = nullptr;
}

void StaticView::cleanup(gc::GarbageCollector* garbage_collector) {
    for(uint64_t j = 0; j < m_num_vertices; j++){
        if(garbage_collector == nullptr){
            m_degree_vector[j].m_pointer.leaf()->decr_ref_count();
        } else {
            m_degree_vector[j].m_pointer.leaf()->decr_ref_count(garbage_collector);
        }
    }
}

void StaticView::create_vertex_id_mapping(){
    profiler::ScopedTimer profiler { profiler::AUX_STATIC_BUILD_HASHMAP };
    uint64_t* __restrict ht = hash_table();
    memset(ht, /* 255 */ numeric_limits<uint8_t>::max(), sizeof(uint64_t) * m_hash_capacity);

    for(uint64_t i = 0; i < m_num_vertices; i++){
        uint64_t vertex_id = m_degree_vector[i].m_vertex_id;

        if(m_hash_direct){
            ht[vertex_id] = i;
        } else {
            uint64_t slot = hash(vertex_id);
            while(ht[slot] != aux::NOT_FOUND){
                slot += 2; // we store both the key and the value in the hash
                if(slot == m_hash_capacity) slot = 0;
            }

            ht[slot] = vertex_id;
            ht[slot+1] = i;
        }
    }
}

uint64_t StaticView::vertex_id(uint64_t logical_id) const noexcept {
    if(logical_id >= m_num_vertices){
        return NOT_FOUND;
    } else {
        return m_degree_vector[logical_id].m_vertex_id;
    }
}

uint64_t StaticView::degree(uint64_t id, bool is_logical_id) const noexcept {
    uint64_t logical_id = is_logical_id ? id : this->logical_id(id);

    if(logical_id >= m_num_vertices){
        return NOT_FOUND;
    } else {
        return m_degree_vector[logical_id].m_degree;
    }
}

memstore::IndexEntry StaticView::direct_pointer(uint64_t id, bool is_logical_id) const {
    uint64_t logical_id = is_logical_id ? id : this->logical_id(id);

    if(logical_id >= m_num_vertices){
        RAISE(InternalError, "Invalid ID: " << id << " (logical: " << is_logical_id << ")");
    } else {
        return m_degree_vector[logical_id].m_pointer;
    }
}

void StaticView::update_pointer(uint64_t id, bool is_logical_id, memstore::IndexEntry value_old, memstore::IndexEntry value_new) {
    uint64_t logical_id = is_logical_id ? id : this->logical_id(id);
    if(logical_id >= m_num_vertices){ RAISE(InternalError, "Invalid ID: " << id << " (logical: " << is_logical_id << ")"); }

    uint64_t* pointer = reinterpret_cast<uint64_t*>(&(m_degree_vector[logical_id].m_pointer));
    uint64_t* expected = reinterpret_cast<uint64_t*>(&value_old);
    uint64_t* desired = reinterpret_cast<uint64_t*>(&value_new);

    bool success = __atomic_compare_exchange(pointer, expected, desired, /* the rest is blah blah for non x86 archs */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    if(success){ // ref count
        auto leaf_old = value_old.leaf();
        auto leaf_new = value_new.leaf();

        if(leaf_new != leaf_old){
            leaf_new->incr_ref_count();
            leaf_old->decr_ref_count();
        }
    }
}

uint64_t StaticView::num_vertices() const noexcept {
    return m_num_vertices;
}

const ItemUndirected* StaticView::degree_vector() const {
    return m_degree_vector;
}

void StaticView::create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction, StaticView** out_array, uint64_t out_sz){
    profiler::ScopedTimer profiler { profiler::AUX_STATIC_CREATE };
    assert(transaction->is_read_only() && "Expected a read-only transaction");

    if(out_sz < 1) RAISE(InternalError, "out_sz < 1");
    if(out_sz > context::StaticConfiguration::numa_num_nodes) RAISE(InternalError, "Invalid value for out_size: " << out_sz << ", number of NUMA nodes: " << context::StaticConfiguration::numa_num_nodes);

    // first node
    Builder builder;
    memstore->aux_view(transaction, &builder);
    uint64_t num_vertices = transaction->graph_properties().m_vertex_count;
    ItemUndirected* degree_vector = builder.create_dv_undirected(num_vertices);
    HashParams hp { num_vertices > 0 ? degree_vector[num_vertices -1].m_vertex_id : numeric_limits<uint64_t>::max(), num_vertices };
    uint64_t size = sizeof(StaticView) + sizeof(uint64_t) * hp.m_capacity;
    void* heap = util::NUMA::malloc(size);
    out_array[0] = new (heap) StaticView{ num_vertices, degree_vector, hp };

    // remaining nodes
    hp.m_initialised = true;
    for(uint64_t i = 1; i < out_sz; i++){
        ItemUndirected* copy_dv = (ItemUndirected*) util::NUMA::copy(degree_vector, i);
        for(uint64_t j = 0; j < num_vertices; j++){
            copy_dv[j].m_pointer.leaf()->incr_ref_count();
        }

        void* copy_view = util::NUMA::copy(heap, i);
        out_array[i] = new (copy_view) StaticView{ num_vertices, copy_dv, hp };

    }
}

StaticView* StaticView::create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction){ // old API
    StaticView* res = {nullptr};
    create_undirected(memstore, transaction, &res, 1);
    return res;
}

StaticView* StaticView::create_undirected(uint64_t num_vertices, const ItemUndirected* degree_vector){ // old API
    HashParams hp { num_vertices > 0 ? degree_vector[num_vertices -1].m_vertex_id : numeric_limits<uint64_t>::max(), num_vertices };
    uint64_t size = sizeof(StaticView) + sizeof(uint64_t) * hp.m_capacity;
    void* heap = util::NUMA::malloc(size);
    return new (heap) StaticView{ num_vertices, degree_vector, hp };
}

void StaticView::dump() const {
    cout << "num_vertices: " << m_num_vertices << ", size of the hashmap: " << m_hash_capacity << ", logical IDs:\n";
    for(uint64_t i = 0; i < m_num_vertices; i++){
        cout << "[" << i << "] " << m_degree_vector[i];

        cout << ", hashmap match: ";
        auto hashres = logical_id(m_degree_vector[i].m_vertex_id);
        if(hashres == aux::NOT_FOUND){
            cout << "not found";
        } else if(hashres != i){
            cout << "no, retrieved: " << hashres;
        } else {
            cout << "yes";
        }
        cout << endl; // flush
    }
}

} // namespace
