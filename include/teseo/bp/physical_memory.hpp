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

namespace teseo::bp {


/**
 * Data structure to obtain chunks of physical memory from the O.S.
 *
 * The class is not thread safe.
 */
class PhysicalMemory {
    PhysicalMemory(const PhysicalMemory&) = delete;
    PhysicalMemory& operator=(const PhysicalMemory&) = delete;

    void* m_start_address; // the start address in virtual memory of the reserved region
    uint64_t m_num_allocated_pages; // number of pages allocated so far
    int m_handle_physical_memory; // the handle to the allocated physical memory, as file descriptor

    // Reset the number of allocated pages in physical memory
    void resize(uint64_t num_pages);

public:
    /**
     * Constructor. Allocate the given number of `num_pages' pages of physical memory
     */
    PhysicalMemory(uint64_t num_pages);

    /**
     * Destructor
     */
    ~PhysicalMemory();

    /**
     * Retrieve the pointer to the start virtual memory space
     */
    void* get_start_address() const noexcept;

    /**
     * Retrieve the address of the given page
     */
    void* get_page(uint64_t page_id) const noexcept;

    /**
     * The opposite of get_page, retrieve the page ID from the address of the page
     */
    uint64_t get_page_id(void* address) const noexcept;

    /**
     * Extend the amount of allocated memory
     */
    void extend(uint64_t num_pages);

    /**
     * Reduce the amount of allocated memory
     */
    void shrink(uint64_t num_pages);

    /**
     * Retrieve the number of allocated pages
     */
    uint64_t get_num_allocated_pages() const noexcept;

    /**
     * Retrieve the total amount of physical memory, in bytes, allocated so far
     */
    uint64_t get_allocated_memory() const noexcept;

    /**
     * Retrieve the maximum amount of memory that can be allocated, in bytes
     */
    static uint64_t get_max_logical_memory();
    static uint64_t get_max_logical_memory(bool huge_pages);

    /**
     * The size of each allocated page, in bytes
     */
    static uint64_t page_size() noexcept;
};




} // namespace
