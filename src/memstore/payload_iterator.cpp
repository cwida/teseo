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

#include "teseo/memstore/payload_iterator.hpp"

#include <cassert>

#include "teseo/memstore/payload_file.hpp"
#include "teseo/util/debug.hpp"

namespace teseo::memstore {

PayloadIterator::PayloadIterator(const PayloadFile* file) : m_block(file), m_start(nullptr), m_position(0), m_length(0) {
    if(m_block != nullptr){
        if(m_block->m_empty_lhs > 0){
            m_start = m_block->data();
            m_length = m_block->m_empty_lhs;
        } else {
            m_start = m_block->data() + m_block->capacity() + m_block->m_empty_rhs;
            m_length = m_block->capacity() - m_block->m_empty_rhs;
        }
    }
}

double PayloadIterator::next(){
    assert(m_block != nullptr && "Iterator exhausted");
    assert(m_position < m_length && "Iterator exhausted");

    double value = m_start[m_position];
    m_position++;

    // move to the next section
    if(m_position >= m_length){

        bool is_lhs = m_start == m_block->data();
        if(is_lhs && m_block->m_empty_rhs < m_block->m_capacity){ // move to the rhs in the current block
            m_start = m_block->data() + m_block->m_empty_rhs;
            m_length = m_block->m_capacity - m_block->m_empty_rhs;
            m_position = 0;
        } else { // move to the next block
            m_block = m_block->m_next;
            if(m_block != nullptr){
                if(m_block->m_empty_lhs > 0){ // lhs
                    m_start = m_block->data();
                    m_length = m_block->m_empty_lhs;
                } else { // rhs
                    assert(m_block->m_empty_rhs < m_block->m_capacity && "Both the lhs and rhs are empty");
                    m_start = m_block->data() + m_block->capacity() + m_block->m_empty_rhs;
                    m_length = m_block->capacity() - m_block->m_empty_rhs;
                }
            } else {
                assert(m_block == nullptr);
                m_start = nullptr;
                m_length = 0;
            }
        }
        m_position = 0;
    }

    return value;
}

bool PayloadIterator::has_next() const {
    return m_block != nullptr && m_position < m_length;
}

void PayloadIterator::skip(uint64_t num_elements) {
    if(num_elements == 0) return; // nothing to do
    assert(m_block != nullptr && "Iterator exhausted");

    // check the current block
    const bool is_lhs = m_block->data() == m_start;
    if(!is_lhs || m_position > 0){
        if(is_lhs){
            if(num_elements < static_cast<uint64_t>(m_length - m_position)){
                m_position += num_elements;
                return; // done
            } else { // move to the rhs
                num_elements -= (m_length - m_position);
                m_start = m_block->data() + m_block->m_empty_rhs;
                m_position = 0;
                m_length = m_block->m_capacity - m_block->m_empty_rhs;
            }
        }
        { // rhs
            if(num_elements < static_cast<uint64_t>(m_length - m_position)){
                m_position += num_elements;
                return; // done
            } else { // move to the next block
                assert(m_block->m_next != nullptr && "Iterator exhausted");
                num_elements -= (m_length - m_position);
                m_block = m_block->m_next;
                m_start = m_block->data();
                m_position = 0;
                m_length = m_block->m_empty_lhs;
            }
        }
    }

    // find the correct block
    while(num_elements >= m_block->cardinality()){
        assert(m_block->m_next != nullptr && "Invalid `num elements' to skip, the iterator is exhausted");
        num_elements -= m_block->m_cardinality;
        m_block = m_block->m_next;
        m_start = m_block->data();
        m_position = 0;
        m_length = m_block->m_empty_lhs;
    }

    // check again the current block
    assert(m_block->data() == m_start && "Expected in the lhs");
    if(num_elements < static_cast<uint64_t>(m_length - m_position)){
        m_position += num_elements;
        return; // done
    } else { // move to the rhs
        num_elements -= (m_length - m_position);
        m_length = m_block->m_capacity - m_block->m_empty_rhs;
        if(num_elements >= m_length){ // iterator exhausted
            m_block = nullptr;
            m_start = nullptr;
            m_position = m_length = 0;
        } else {
            m_start = m_block->data() + m_block->m_empty_rhs;
            assert(num_elements <= static_cast<uint64_t>(m_length) && "Iterator exhausted");
            m_position = num_elements;
        }
    }
}

} // namespace
