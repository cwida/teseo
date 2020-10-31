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
#include "teseo/rebalance/scratchpad.hpp"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::rebalance {

ScratchPad::ScratchPad() : m_capacity(0), m_elements(nullptr), m_versions(nullptr) {
    m_last_vertex_loaded = numeric_limits<uint64_t>::max();
}

ScratchPad::ScratchPad(uint64_t capacity) : m_capacity(capacity){
    m_last_vertex_loaded = numeric_limits<uint64_t>::max();
    m_elements = (decltype(m_elements)) malloc(capacity * sizeof(m_elements[0]));
    m_versions = (decltype(m_versions)) malloc(capacity * sizeof(m_versions[0]));
    if(m_elements == nullptr || m_versions == nullptr){
        COUT_DEBUG_FORCE("bad alloc at " << __FILE__ << ":" << __LINE__ << ", capacity: " << capacity << ", m_elements: " << m_elements << ", m_versions: " << m_versions);
        throw std::bad_alloc{};
    }
}

ScratchPad::~ScratchPad(){
    free(m_elements); m_elements = nullptr;
    free(m_versions); m_versions = nullptr;
}

uint64_t ScratchPad::capacity() const {
    return m_capacity;
}

uint64_t ScratchPad::size() const {
    return m_size;
}

void ScratchPad::ensure_capacity(uint64_t capacity_new) {
    if(m_capacity >= capacity_new) return; /* nop */

    auto elements_new = (decltype(m_elements)) malloc(capacity_new * sizeof(m_elements[0]));
    auto versions_new = (decltype(m_versions)) malloc(capacity_new * sizeof(m_versions[0]));
    if(elements_new == nullptr || versions_new == nullptr){
        COUT_DEBUG_FORCE("bad alloc at " << __FILE__ << ":" << __LINE__ << ", capacity: " << capacity_new << ", elements_new: " << elements_new << ", versions_new: " << versions_new);
        throw std::bad_alloc{};
    }

    memcpy(elements_new, m_elements, m_size * sizeof(m_elements[0]));
    memcpy(versions_new, m_versions, m_size * sizeof(m_versions[0]));

    free(m_elements);
    free(m_versions);

    m_elements = elements_new;
    m_versions = versions_new;
    m_capacity = capacity_new;
}

void ScratchPad::clear() {
    m_size = 0;
    m_last_vertex_loaded = -1;
}

void ScratchPad::load_vertex(const memstore::Vertex* vertex, const memstore::Version* version){
    assert(m_size < m_capacity && "Overflow");

    m_elements[m_size].m_vertex = *vertex;
    set_version(m_size, version);
    m_last_vertex_loaded = m_size;
    m_size ++;
}

void ScratchPad::unload_last_vertex(){
    assert(m_size > 0 && "Empty");
    assert(has_last_vertex() && "No last vertex registered");
    m_size = m_last_vertex_loaded;
    m_last_vertex_loaded = numeric_limits<uint64_t>::max();
}

void ScratchPad::shift_back(uint64_t position, uint64_t shift) {
    assert(position < m_size);
    assert(position >= shift);
    if(shift == 0) return;
    memcpy(m_elements + position - shift, m_elements + position, sizeof(m_elements[0]));
    m_versions[position - shift] = m_versions[position];
}

void ScratchPad::set_size(uint64_t new_size) {
    assert(new_size <= m_capacity);
    m_size = new_size;
}

void ScratchPad::load_edge(const memstore::Edge* edge, const memstore::Version* version){
    assert(m_size < m_capacity && "Overflow");
    m_elements[m_size].m_edge = edge; // C++ operator overloading
    set_version(m_size, version);
    m_size++;
}

void ScratchPad::load_edge(uint64_t destination, double weight, const memstore::Version* version) {
    assert(m_size < m_capacity && "Overflow");
    m_elements[m_size].m_edge.m_destination = destination;
    m_elements[m_size].m_edge.m_weight = weight;
    set_version(m_size, version);
    m_size++;
}

void ScratchPad::set_version(uint64_t position, const memstore::Version* version){
    if(version == nullptr || version->m_version == 0){
        unset_version(position);
    } else {
        m_versions[position] = *version;
    }
}

void ScratchPad::unset_version(uint64_t position) {
    reinterpret_cast<uint64_t*>(m_versions)[position] = 0;
}

memstore::Vertex* ScratchPad::get_vertex(uint64_t position) const {
    assert(position < m_capacity && "Invalid position");
    return &(m_elements[position].m_vertex);
}

rebalance::WeightedEdge* ScratchPad::get_edge(uint64_t position) const {
    assert(position < m_capacity && "Invalid position");
    return &(m_elements[position].m_edge);
}

memstore::Version* ScratchPad::get_version(uint64_t position) const {
    assert(position < m_capacity && "Invalid position");
    return m_versions + position;
}

memstore::Version ScratchPad::move_version(uint64_t position) {
    assert(position < m_capacity && "Invalid position");
    assert(has_version(position) && "No version set");
    auto version = m_versions[position];
    unset_version(position);
    return version;
}

memstore::Vertex* ScratchPad::get_last_vertex() const {
    return has_last_vertex() ? &(m_elements[m_last_vertex_loaded].m_vertex) : nullptr;
}

bool ScratchPad::has_last_vertex() const{
    return m_last_vertex_loaded != numeric_limits<uint64_t>::max();
}

bool ScratchPad::has_version(uint64_t position) const {
    assert(position < m_capacity && "Invalid position");
    return reinterpret_cast<uint64_t*>(m_versions)[position] != 0;
}

void ScratchPad::unset_element(uint64_t position){
    assert(position < m_capacity && "Invalid position");
    // as vertices start from 1, we can treat the `0' as flag
    // If the element were actually an edge, then we would actually set the edge.m_destination = 0, which is also fine
    // as it represents a vertex as well.
    m_elements[position].m_vertex.m_vertex_id = 0;
}


bool ScratchPad::has_element(uint64_t position) const {
    assert(position < m_capacity && "Invalid position");
    return m_elements[position].m_vertex.m_vertex_id == 0; // see note for #unset_element
}

void ScratchPad::dump() const {
    memstore::Vertex* vertex = nullptr;
    int64_t num_edges = 0;

    for(uint64_t i = 0; i < size(); i++){
        cout << "[" << i << "] ";
        if(num_edges == 0){
            vertex = get_vertex(i);
            num_edges = vertex->m_count;
            cout << vertex->to_string(get_version(i));
        } else {
            WeightedEdge* edge = get_edge(i);
            cout << edge->to_string(vertex, get_version(i));

            num_edges --;
            if(num_edges == 0){ vertex = nullptr; }
        }
        cout << endl;
    }
}

} // namespace
