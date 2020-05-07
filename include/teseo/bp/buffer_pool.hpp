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

#include <cinttypes>
#include <deque>
#include <mutex>

#include "teseo/bp/physical_memory.hpp"

namespace teseo::bp {

/**
 * A local cache of (huge) pages
 *
 * This class is thread safe.
 */
class BufferPool {
    std::deque<uint64_t> m_freelist; // free list for the older pages
    uint64_t m_threshold; // threshold to decide whether to insert at the front or at the back of the free list
    mutable std::mutex m_mutex; // better safe than sorry
    PhysicalMemory m_physical_memory; // acquire more pages from the physical memory

public:
    /**
     * Initialise the buffer pool
     */
    BufferPool();

    /**
     * Destructor
     */
    ~BufferPool();

    /**
     * Allocate a new page/frame from the buffer pool
     */
    void* allocate_page();

    /**
     * Return the used page/frame to the buffer pool
     */
    void deallocate_page(void* address);

    /**
     * Rebuild the free list
     */
    void rebuild_free_list();

    /**
     * Current size of the free list
     */
    uint64_t get_num_available_pages() const;

    /**
     * Retrieve the size of a page/frame
     */
    uint64_t get_page_size() const noexcept;

    /**
     * Dump the internal content to stdout, for debugging purposes
     */
    void dump() const;
};

} // namespace
