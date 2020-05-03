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

#pragma once

#include <atomic>
#include <cstdint>

namespace teseo::gc {

class Item; // a single entry in the queue

/**
 * The queue shared between a garbage collector and a thread context.
 * The idea is that the thread context can only invoke the method #push(), which may fail
 * from time to time, returning false. When this happens, the thread context should
 * push its item into its local (non shared) queue.
 *
 * This class is not thread-safe: only one thread context can operate at the time.
 */
class SimpleQueue {
    Item* m_array; //  the actual queue
    uint32_t m_start; // start index (incl)
    volatile uint32_t m_end; // last index (excl)
    uint32_t m_capacity; // the capacity of the queue

public:
    // Create an empty queue
    // @capacity the initiial capacity of the queue ( 0 = auto )
    SimpleQueue(uint32_t capacity = 0);

    // Destructor
    ~SimpleQueue();

    // Is the queue full?
    bool full() const;

    // Is the queue empty ?
    bool empty() const;

    // Resize the queue, by doubling its capacity
    // Precondition: the queue must be full
    void resize();

    // Retrieve the cardinality of the queue
    uint64_t size() const;

    // Append a single entry in the queue
    bool push(const Item& item);

    // Remove N elements from the queue
    void pop(uint64_t num_elements = 1);

    // Retrieve an element at given position
    Item& get(int64_t i);

    // Alias for #get
    Item& operator[](int64_t i);

    // Dump the content of the array to stdout, for debugging purposes
    void dump() const;
};

} // namespace
