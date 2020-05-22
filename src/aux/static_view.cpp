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

#include "teseo/aux/builder.hpp"
#include "teseo/aux/item.hpp"
#include "teseo/context/property_snapshot.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"

#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::aux {

StaticView::StaticView(uint64_t num_vertices, const ItemUndirected* degree_vector) : StaticView(num_vertices, degree_vector, HashParams(num_vertices)){

}

StaticView::StaticView(uint64_t num_vertices, const ItemUndirected* degree_vector, const HashParams& hash) :
        m_num_vertices(num_vertices), m_degree_vector(degree_vector), m_hash_capacity(hash.m_capacity), m_hash_const(hash.m_const), m_hash_array(nullptr){
    create_vertex_id_mapping();

    if(context::StaticConfiguration::aux_profile_collisions){
        profile_collisions();
    }
}

StaticView::HashParams::HashParams(uint64_t num_vertices) {
    uint64_t capacity0 = num_vertices * context::StaticConfiguration::aux_hash_multiplier;
    // The hash function is a going to be the modulo over m_hash_const ("the division method"). However, that
    // is, theoretically, rather terrible as we purposedly want the capacity be a power of 2, so that the hash
    // can be computed quickly, with a mask on m_hash_const, and close value for the vertices end up in subsequent
    // slots, to take advantage of the sequential locality of scans.
    // If, upon profiling, this approach does not work, we can readdress the hash function.
    // For the time being, let's compute the next power of 2:
    uint64_t power = ceil(log2(capacity0));
    m_capacity = (1<<power);
    m_const = m_capacity -1;
}

StaticView::~StaticView(){
    delete[] m_degree_vector;
    free(m_hash_array);
}

void StaticView::create_vertex_id_mapping(){
    profiler::ScopedTimer profiler { profiler::AUX_STATIC_BUILD_HASHMAP };
    m_hash_array = (uint64_t*) malloc(sizeof(uint64_t) * m_hash_capacity);
    if(m_hash_array == nullptr) throw std::bad_alloc{};
    memset(m_hash_array, /* 255 */ numeric_limits<uint8_t>::max(), sizeof(uint64_t) * m_hash_capacity);

    for(uint64_t i = 0; i < m_num_vertices; i++){
        uint64_t vertex_id = m_degree_vector[i].m_vertex_id;
        uint64_t slot = hash(vertex_id);
        while(m_hash_array[slot] != aux::NOT_FOUND){
            slot++;
            if(slot == m_hash_capacity) slot = 0;
        }

        m_hash_array[slot] = i;
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

uint64_t StaticView::num_vertices() const noexcept {
    return m_num_vertices;
}

const ItemUndirected* StaticView::degree_vector() const {
    return m_degree_vector;
}

StaticView* StaticView::create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction){
    profiler::ScopedTimer profiler { profiler::AUX_STATIC_CREATE };
    assert(transaction->is_read_only() && "Expected a read-only transaction");

    Builder builder;
    memstore->aux_view(transaction, &builder);
    uint64_t num_vertices = transaction->graph_properties().m_vertex_count;
    ItemUndirected* degree_vector = builder.create_dv_undirected(num_vertices);
    return new StaticView(num_vertices, degree_vector);
}

void StaticView::profile_collisions() const {
    unique_ptr<uint64_t[]> ptr_collisions { new uint64_t[m_num_vertices] };
    uint64_t* __restrict collisions = ptr_collisions.get();
    uint64_t sum = 0;
    uint64_t max_num_collisions = 0;

    for(uint64_t i = 0; i < m_num_vertices; i++){
        uint64_t vertex_id = m_degree_vector[i].m_vertex_id;
        uint64_t slot = hash(vertex_id);
        uint64_t num_collisions = 0;
        while(m_hash_array[slot] != aux::NOT_FOUND){
            if(m_degree_vector[ m_hash_array[slot] ].m_vertex_id == vertex_id ){
                break;
            }
            slot = ((slot + 1) & m_hash_const);

            num_collisions++;
        }

        max_num_collisions = max(num_collisions, max_num_collisions);
        sum += num_collisions;
        collisions[i] = num_collisions;
    }

    uint64_t mean = static_cast<double>(sum) / m_num_vertices;
    sort(collisions, collisions + m_num_vertices);
    uint64_t median = collisions[m_num_vertices /2];

    COUT_DEBUG_FORCE("size: " << m_hash_capacity << ", mean: " << mean << ", median: " << median << ", max: " << max_num_collisions)
}

void StaticView::dump() const {
    cout << "num_vertices: " << m_num_vertices << ", size of the hashmap: " << m_hash_capacity << ", logical IDs:\n";
    for(uint64_t i = 0; i < m_num_vertices; i++){
        cout << "[" << i << "] vertex_id: " << m_degree_vector[i].m_vertex_id << ", degree: " << m_degree_vector[i].m_degree;

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



