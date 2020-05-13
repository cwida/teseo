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

#include "teseo/bp/buffer_pool.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>

#include "teseo/bp/frame.hpp"
#include "teseo/context/static_configuration.hpp"


//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::bp {

BufferPool::BufferPool() : m_threshold(0), m_physical_memory(context::StaticConfiguration::bp_min_num_pages){

    for(uint64_t i = 0; i < m_physical_memory.get_num_allocated_pages(); i++){
        m_freelist.push_back(i);
    }
}

BufferPool::~BufferPool(){
    // should we assert whether all pages have been released?
}

void* BufferPool::allocate_page(){
    lock_guard<mutex> xlock(m_mutex);
    uint64_t page_id = 0;

    if(m_freelist.empty()){ // .. assuming they are any available ...
        uint64_t num_allocated_pages_before = m_physical_memory.get_num_allocated_pages();
        m_physical_memory.extend(context::StaticConfiguration::bp_min_num_pages);
        uint64_t num_allocated_pages_after = m_physical_memory.get_num_allocated_pages();
        for(uint64_t i = num_allocated_pages_before; i < num_allocated_pages_after; i++){
            m_freelist.push_back(i);
        }
    }

    page_id = m_freelist.front();
    m_freelist.pop_front();

    COUT_DEBUG("allocate page: " << page_id << " @ " << m_physical_memory.get_page(page_id));
    Frame* frame = reinterpret_cast<Frame*>(m_physical_memory.get_page(page_id));
    frame->m_buffer_pool = this;
    return reinterpret_cast<void*>(frame +1);
}

void BufferPool::do_deallocate_page(Frame* frame){
    assert(frame != nullptr);
    assert(frame->m_buffer_pool == this && "This allocation does not belong to this BP instance");

    uint64_t page_id = m_physical_memory.get_page_id((void*) frame);
    assert(page_id < m_physical_memory.get_num_allocated_pages() && "The page does not belong to the buffer pool");

    COUT_DEBUG("deallocate page: " << page_id << " @ " << m_physical_memory.get_page(page_id));

    lock_guard<mutex> xlock(m_mutex);
    assert(std::find(begin(m_freelist), end(m_freelist), page_id) == end(m_freelist) && "Page already released");

    if(page_id < m_threshold){
        m_freelist.push_front(page_id);
    } else {
        m_freelist.push_back(page_id);
    }
}

void BufferPool::deallocate_page(void* address){
    if(address == nullptr) return; // nop
    Frame* frame = reinterpret_cast<Frame*>(address) -1;
    frame->m_buffer_pool->do_deallocate_page(frame);
}

void BufferPool::rebuild_free_list(){
    lock_guard<mutex> xlock(m_mutex);
    uint64_t total_pages = m_physical_memory.get_num_allocated_pages();
    std::sort(begin(m_freelist), end(m_freelist));

    uint64_t expected_value = total_pages -1;
    int64_t i = static_cast<int64_t>(m_freelist.size()) -1;
    uint64_t num_removed_pages = 0;
    while(i >= 0 && m_freelist[i] == expected_value && total_pages - num_removed_pages >= context::StaticConfiguration::bp_min_num_pages){
        i--;
        expected_value --;
        num_removed_pages++;
    }

    // always shrink only of a multiple of context::StaticConfiguration::bp_min_num_pages
    num_removed_pages -= (num_removed_pages % context::StaticConfiguration::bp_min_num_pages);
    // remove the pages
    m_freelist.erase(end(m_freelist) - num_removed_pages, end(m_freelist));
    m_physical_memory.shrink(num_removed_pages);

    // set the new threshold
    m_threshold = m_physical_memory.get_num_allocated_pages() - context::StaticConfiguration::bp_min_num_pages;

    COUT_DEBUG("removed pages: " << num_removed_pages << ", free list size: " << m_freelist.size());
}

uint64_t BufferPool::get_page_size() const noexcept {
    return m_physical_memory.page_size();
}

uint64_t BufferPool::get_num_available_pages() const {
    lock_guard<mutex> xlock(m_mutex);
    return m_freelist.size();
}

void BufferPool::dump() const{
    cout << "[BufferPool] number of slots allocated: " << m_physical_memory.get_num_allocated_pages() << ", "
            "page size: " << get_page_size() << " bytes, "
            "threshold: " << m_threshold << ", "
            "free list size: " << m_freelist.size() << ": \n";
    for(uint64_t i = 0; i < m_freelist.size(); i++){
        cout << "[" << i << "] " << m_freelist[i] << "\n";
    }
    cout << endl;;
}

} // namespace


