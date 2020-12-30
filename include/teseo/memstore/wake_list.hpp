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

#include <cstdint>

namespace teseo::memstore {

/**
 * This is a list of threads that needs to be awaken after the latch associated to
 * a segment has been released
 */
class WakeList {
    WakeList(const WakeList&) = delete;
    WakeList& operator=(const WakeList&) = delete;

    // This is an optimisation
    // The encoding is:
    //  m_list == nullptr, then the list is empty
    //  m_list % 2 == 1 => m_list contains a ptr to a single future to wake up
    //  m_list % 2 == 0 => m_list[0] contains the length of the list, and m_list[1], m_list[2], ... are the futures to release
    void* m_list = nullptr;

public:
    // Create an empty instance
    WakeList() noexcept;

    // Move the content of the list
    WakeList(WakeList&& copy) noexcept;
    WakeList& operator=(WakeList&& copy) noexcept;

    // Destructor
    ~WakeList();

    // Empty the content of the list
    void reset() noexcept;

    // Set the content of the list to the first N elements of the priority queue. The copied elements are removed from
    // the priority queue.
    // @param queue a reference to the segment's internal priority queue
    // @param n the number of elements to copy
    void set(void* queue, uint64_t n = 1);

    // Wake all threads in the list
    void wake() noexcept;
};

} // namespace
