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

#include "teseo/gc/simple_queue.hpp"

#include <cassert>
#include <cstring>
#include <emmintrin.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>

#include "teseo/context/static_configuration.hpp"
#include "teseo/gc/item.hpp"
#include "teseo/util/compiler.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::gc {

SimpleQueue::SimpleQueue(uint32_t capacity) : m_array(nullptr), m_start(0), m_end(1),
        m_capacity(capacity == 0 ? context::StaticConfiguration::gc_queue_initial_capacity : capacity) {
    m_array = new Item[ m_capacity ];
}

SimpleQueue::~SimpleQueue(){
    delete[] m_array; m_array = nullptr;
}

bool SimpleQueue::empty() const {
    // yes, we're wasting one slot, because we need to discriminate between the cases
    // of whether the queue is empty or full
    uint32_t start = m_start;
    uint32_t end = m_end;
    return (start + 1 == end) || (start == m_capacity -1 && end == 0);
}

bool SimpleQueue::full() const {
    return m_start == m_end;
}

void SimpleQueue::resize(){
    assert(full() && "The protocol is to resize only iff the queue is full");
    uint32_t new_capacity = m_capacity * 2;

    Item* new_array = new Item[ new_capacity ];

    // copy the elements from the old array to the new one
    assert(m_end == m_start && "The protocol is to resize only iff the queue is full");
    uint32_t new_start = new_capacity - (m_capacity - m_start);

    // move the elements in [m_end, old capacity) at the end of the new array.
    memcpy(new_array, m_array, sizeof(Item) * m_end +1);
    memcpy(new_array + new_start +1, m_array + m_start +1, sizeof(Item) * (m_capacity - m_start -1));

    COUT_DEBUG("old array: " << m_array << ", new array: " << new_array);

    delete[] m_array;
    m_array = new_array;
    m_capacity = new_capacity;

    // finally report the queue is not full anymore
    util::compiler_barrier(); //x86 guarantees the write order is respected
    m_start = new_start;
}

uint64_t SimpleQueue::size() const {
    uint32_t start = m_start;
    uint32_t end = m_end;

    if (end > start)
        return end - start -1;
    else
        return end + (m_capacity - start -1);
}

bool SimpleQueue::push(const Item& item){
    // Now, invoking full() may return true even when it is not. This occurs when the queue has been resized
    // in the meanwhile by the garbage collector. But that's fine, the thread context will insert the item
    // into its own queue. What cannot occur is that the queue is full and the method returns false.
    if(full()) return false;

    uint32_t end = m_end;
    m_array[end] = item;

    if(end +1 == m_capacity){
        m_end = 0;
    } else {
        m_end = end +1;
    }

    return true;
}

Item& SimpleQueue::get(int64_t i){
    assert(m_start < m_capacity && "As m_start in [0, m_capacity)");
    int64_t begin_sz = m_capacity - m_start -1;
    if (i < begin_sz){
        return m_array[m_start + i +1];
    } else {
        return m_array[i - begin_sz];
    }
}

Item& SimpleQueue::operator[](int64_t i) {
    return get(i);
}

void SimpleQueue::pop(uint64_t num_elements){
    uint64_t begin_sz = m_capacity - m_start;
    if(num_elements < begin_sz){
        m_start += num_elements;
    } else {
        m_start = num_elements - begin_sz;
    }
}

void SimpleQueue::dump() const {
    cout << "[SimpleQueue] size: " << size() << ", capacity: " << m_capacity << ", start: " << m_start << ", end: " << m_end << ", full: " << boolalpha << full() << "\n";
    for(uint64_t i = 0, sz = size(); i < sz; i++){
        cout << "[" << i << "] " << const_cast<SimpleQueue*>(this)->get(i) << "\n";
    }
}

} // namespace
