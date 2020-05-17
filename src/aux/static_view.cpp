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
#include <iostream>

#include "teseo/aux/builder.hpp"
#include "teseo/aux/item.hpp"
#include "teseo/context/property_snapshot.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"

using namespace std;

namespace teseo::aux {

StaticView::StaticView(uint64_t num_vertices, const ItemUndirected* degree_vector) : m_num_vertices(num_vertices), m_degree_vector(degree_vector){
    create_vertex_id_mapping();
}

StaticView::~StaticView(){
    delete[] m_degree_vector;
}

void StaticView::create_vertex_id_mapping(){
    profiler::ScopedTimer profiler { profiler::AUX_STATIC_BUILD_HASHMAP };
    assert(m_vertex_ids.empty() && "Was the hashmap already created?");
    m_vertex_ids.reserve(m_num_vertices);
    for(uint64_t i = 0; i < m_num_vertices; i++){
        m_vertex_ids[m_degree_vector[i].m_vertex_id] = i;
    }
}

uint64_t StaticView::vertex_id(uint64_t logical_id) const noexcept {
    if(logical_id >= m_num_vertices){
        return NOT_FOUND;
    } else {
        return m_degree_vector[logical_id].m_vertex_id;
    }
}

uint64_t StaticView::logical_id(uint64_t vertex_id) const noexcept  {
    auto it = m_vertex_ids.find(vertex_id);
    if(it == m_vertex_ids.end()){
        return NOT_FOUND;
    } else {
        return it->second;
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

void StaticView::dump() const {
    cout << "num_vertices: " << m_num_vertices << ", size of the hashmap: " << m_vertex_ids.size() << ", logical IDs:\n";
    for(uint64_t i = 0; i < m_num_vertices; i++){
        cout << "[" << i << "] vertex_id: " << m_degree_vector[i].m_vertex_id << ", degree: " << m_degree_vector[i].m_degree;

        cout << ", hashmap match: ";
        auto it = m_vertex_ids.find(m_degree_vector[i].m_vertex_id);
        if(it == m_vertex_ids.end()){
            cout << "not found";
        } else if(it->second != i){
            cout << "no, retrieved: " << it->second;
        } else {
            cout << "yes";
        }
        cout << endl; // flush
    }
}

} // namespace



