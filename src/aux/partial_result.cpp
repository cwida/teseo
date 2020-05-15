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

#include "teseo/aux/builder.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"

#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::aux {

PartialResult::PartialResult(Builder* builder, uint64_t id, const memstore::Key& from, const memstore::Key& to) :
    m_builder(builder), m_id(id), m_from(from), m_to(to), m_array(nullptr), m_size(0), m_capacity(0), m_last(0)
        {
    static_assert(context::StaticConfiguration::aux_partial_init_capacity > 0);
    resize(context::StaticConfiguration::aux_partial_init_capacity);
    assert(capacity() > 0 && "Error in the resize");
    m_array[0].first = from.source();
    m_array[0].second = 0;
    m_size = 1;
}

PartialResult::~PartialResult(){
    delete[] m_array; m_array = nullptr;
    m_size = m_capacity = 0;
}

void PartialResult::resize(uint64_t new_capacity){
    item_t* new_array = new item_t[new_capacity];
    memcpy((void*) new_array, m_array, m_size * sizeof(new_array[0]));
    delete[] m_array;
    m_array = new_array;
    m_capacity = new_capacity;
}

void PartialResult::incr_degree(uint64_t vertex_id, uint64_t increment){
    if(increment == 0) return; // ignore
    if(m_array[m_last].first == vertex_id){
        m_array[m_last].second += increment;
    } else { // m_array[m_last].first != vertex_id
        assert(vertex_id > m_array[m_last].first && "The vertices should be inserted in sorted order");
        if(size() == capacity()) resize(capacity() * 2);
        m_last++;
        m_array[m_last].first = vertex_id;
        m_array[m_last].second = increment;
    }
}

void PartialResult::done(){
    m_builder->collect(this);
}

uint64_t PartialResult::capacity() const {
    return m_capacity;
}

uint64_t PartialResult::size() const {
    return m_size;
}

pair<uint64_t, uint64_t> PartialResult::get(uint64_t index) const {
    assert(index < m_size && "Overflow");
    return m_array[index];
}

pair<uint64_t, uint64_t> PartialResult::at(uint64_t index) const {
    return get(index);
}

} // namespace
