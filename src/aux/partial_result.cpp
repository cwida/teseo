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

#include "teseo/aux/partial_result.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

#include "teseo/aux/builder.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"

#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::aux {

PartialResult::PartialResult(Builder* builder, uint64_t id, const memstore::Key& from, const memstore::Key& to) :
    m_builder(builder), m_id(id), m_from(from), m_to(to), m_array(nullptr), m_last(-1), m_capacity(0)
        {
    static_assert(context::StaticConfiguration::aux_partial_init_capacity > 0);
    resize(context::StaticConfiguration::aux_partial_init_capacity);
    assert(capacity() > 0 && "Error in the resize");
}

PartialResult::~PartialResult(){
    delete[] m_array; m_array = nullptr;
    m_capacity = 0;
}

void PartialResult::resize(uint64_t new_capacity){
    ItemUndirected* new_array = new ItemUndirected[new_capacity]();
    memcpy((void*) new_array, m_array, size() * sizeof(new_array[0]));
    delete[] m_array;
    m_array = new_array;
    m_capacity = new_capacity;
}

void PartialResult::incr_degree(uint64_t vertex_id, uint64_t increment){
    if(!empty() && m_array[m_last].m_vertex_id == vertex_id){
        m_array[m_last].m_degree += increment;
    } else { // m_array[m_last].first != vertex_id
        if(size() == capacity()) resize(capacity() * 2);
        assert((m_last < 0 || m_array[m_last].m_vertex_id < vertex_id) && "Sorted order not respected");
        m_last++;
        m_array[m_last].m_vertex_id = vertex_id;
        m_array[m_last].m_degree = increment;
    }
}

void PartialResult::done(){
    m_builder->collect(this);
}

uint64_t PartialResult::id() const noexcept {
    return m_id;
}

const memstore::Key& PartialResult::key_from() const noexcept {
    return m_from;
}

const memstore::Key& PartialResult::key_to() const noexcept {
    return m_to;
}

uint64_t PartialResult::capacity() const noexcept {
    return m_capacity;
}

uint64_t PartialResult::size() const noexcept {
    return m_last +1;
}

bool PartialResult::empty() const noexcept {
    return size() == 0;
}

const ItemUndirected& PartialResult::get(uint64_t index) const {
    assert(index < size() && "Overflow");
    return m_array[index];
}

const ItemUndirected& PartialResult::at(uint64_t index) const {
    return get(index);
}

void PartialResult::dump() const {
    cout << "[PartialResult] id: " << id() << ", interval: [" << key_from() << ", " << key_to() << "), size: " << size() << ", capacity: " << capacity() << "\n";
    for(uint64_t i = 0, end = size(); i < end; i++){
        cout << "[" << i << "] vertex id: " << at(i).m_vertex_id << ", degree: " << at(i).m_degree << "\n";
    }
    flush(cout);
}

} // namespace
