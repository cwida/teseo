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

#include "teseo/bp/physical_memory.hpp"

#include <atomic>
#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <linux/memfd.h>
#include <mutex>
#if __has_include(<sys/memfd.h>)
#include <sys/memfd.h>
#endif
#include <sys/mman.h> // mmap
#include <sys/syscall.h>
#include <unistd.h>

#include "teseo/context/static_configuration.hpp"
#include "teseo/util/error.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::bp {

PhysicalMemory::PhysicalMemory(uint64_t num_pages) : m_start_address(nullptr), m_num_allocated_pages(0), m_handle_physical_memory(-1){
    // create the fd to the physical memory
    string id = "teseo_bp_";
    static atomic<int> g_internal_id = 0;
    id += to_string(g_internal_id++);
    m_handle_physical_memory = memfd_create(id.c_str(), context::StaticConfiguration::huge_pages ? MFD_HUGETLB : 0);
    if(m_handle_physical_memory < 0){
        ERROR("Cannot allocate the O.S. file descriptor to physical memory. "
              "Error raised by #memfd_create: " << strerror(errno) << " (errno: " << errno << "), "
              "huge pages enabled: " << boolalpha << context::StaticConfiguration::huge_pages);
    }


    COUT_DEBUG("num pages to allocate: " << num_pages << ", "
               "size of each page: " << page_size() << " bytes, "
               "virtual memory reserved: " << get_max_logical_memory() << " bytes");

    // allocate the physical memory
    resize(num_pages);

    // memory map the physical memory to a virtual space
    void* mmap_ret = mmap(
        /* starting address, NULL means arbitrary */ NULL,
        /* length in bytes */ get_max_logical_memory(),
        /* memory protection */ PROT_READ | PROT_WRITE,
        /* flags */ MAP_PRIVATE,
        /* file descriptor */ m_handle_physical_memory,
        /* offset, in terms of multiples of the page size */ 0);
    if(mmap_ret == MAP_FAILED){
        ERROR("Cannot map " << get_max_logical_memory() << " bytes of virtual memory. Error raised by "
              "#mmap: " << strerror(errno) << " (errno: " << errno << ")"); }

    m_start_address = mmap_ret;
}

PhysicalMemory::~PhysicalMemory(){
    // release the reserved virtual memory
    if(m_start_address != nullptr){
        int rc = munmap(m_start_address, get_max_logical_memory());
        if(rc < 0){
            COUT_DEBUG_FORCE("Error while releasing the virtual memory, munmap error: " << strerror(errno) << " (errno: " << errno << ")");
        }
    }

    // release the acquired physical memory
    if(m_handle_physical_memory >= 0){
        int rc = close(m_handle_physical_memory);
        if(rc < 0){
            COUT_DEBUG_FORCE("Error while releasing the physical memory, fh: " << m_handle_physical_memory << ": " << strerror(errno) << " (errno: " << errno << ")");
        }
        m_handle_physical_memory = -1;
    }
}

void PhysicalMemory::resize(uint64_t num_pages){
    if(num_pages == m_num_allocated_pages) return;

    // check whether we are allowed to allocate the amount of _physical_ memory requested
    size_t size_physical_memory = page_size() * num_pages;
    if(size_physical_memory > get_max_logical_memory()){
        ERROR("Cannot allocate " << num_pages << " pages and " << size_physical_memory << " bytes of physical memory. "
               "The amount of reserved virtual memory is: " << get_max_logical_memory() << " bytes.");
    }

    // allocate the physical memory
    int rc = ftruncate(m_handle_physical_memory, size_physical_memory);
    if(rc != 0){
        ERROR("Cannot allocate " << num_pages << " pages and "<< size_physical_memory << " bytes of physical memory. "
               "Error raised by #ftruncate: " << strerror(errno) << " (errno: " << errno << ")");
    }

    m_num_allocated_pages = num_pages;
}

void PhysicalMemory::extend(uint64_t num_pages){
    resize(get_num_allocated_pages() + num_pages);
}

void PhysicalMemory::shrink(uint64_t num_pages){
    assert(get_num_allocated_pages() >= num_pages && "Underflow");
    resize(get_num_allocated_pages() - num_pages);
}

void* PhysicalMemory::get_start_address() const noexcept {
    return m_start_address;
}

void* PhysicalMemory::get_page(uint64_t page_id) const noexcept {
    assert(page_id < get_num_allocated_pages() && "Overflow");
    char* start_address = (char*) get_start_address();
    return start_address + page_id * page_size();
}

uint64_t PhysicalMemory::get_page_id(void* address) const noexcept {
    char* start_address = (char*) get_start_address();
    char* page_address = (char*) address;
    return (page_address - start_address) / page_size();
}

uint64_t PhysicalMemory::get_num_allocated_pages() const noexcept {
    return m_num_allocated_pages;
}

uint64_t PhysicalMemory::get_allocated_memory() const noexcept {
    return get_num_allocated_pages() * page_size();
}

uint64_t PhysicalMemory::page_size() noexcept {
    return context::StaticConfiguration::bp_page_size;
}

uint64_t PhysicalMemory::get_max_logical_memory(){
    return get_max_logical_memory(context::StaticConfiguration::huge_pages);
}

uint64_t PhysicalMemory::get_max_logical_memory(bool huge_pages) {
    static uint64_t g_max_logical_memory = context::StaticConfiguration::bp_max_logical_memory;
    static mutex g_mutex;

    if(g_max_logical_memory == 0){ // test, lock, test again
        lock_guard<mutex> xlock(g_mutex);
        if(g_max_logical_memory == 0) { // set it only once
            uint64_t max_logical_memory = 1ull << 41; // 1TB should be enough

            int fd = memfd_create("get_max_logical_memory", huge_pages ? MFD_HUGETLB : 0);
            assert(fd >= 0);

            void* mmap_ret = nullptr;

            do {
                max_logical_memory /= 2;
                assert(max_logical_memory > 0);
                COUT_DEBUG("Trying with " << max_logical_memory << " bytes ... ");

                mmap_ret = mmap(
                /* starting address, NULL means arbitrary */ NULL,
                /* length in bytes */ max_logical_memory,
                /* memory protection */ PROT_READ | PROT_WRITE,
                /* flags */ MAP_SHARED,
                /* file descriptor */ fd,
                /* offset, in terms of multiples of the page size */ 0
                );

            } while(mmap_ret == MAP_FAILED);

            munmap(mmap_ret, max_logical_memory); /* ignore rc */
            close(fd); /* ignore rc */

            g_max_logical_memory = max_logical_memory;
        }
    }

    return g_max_logical_memory;
}


} // namespace


