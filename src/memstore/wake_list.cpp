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
#include "teseo/memstore/wake_list.hpp"

#include <cassert>
#include <iostream>
#include <limits>

#include "teseo/memstore/segment.hpp"
//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

WakeList::WakeList() noexcept { }

WakeList::WakeList(WakeList&& copy) noexcept : m_list(copy.m_list){
    copy.m_list = nullptr;
}

WakeList& WakeList::operator=(WakeList&& copy) noexcept {
    if(this != &copy){
        reset();
        m_list = copy.m_list;
        copy.m_list = nullptr;
    }
    return *this;
}

WakeList::~WakeList(){
    reset();
}

void WakeList::reset() noexcept {
    if(m_list != nullptr && reinterpret_cast<uint64_t>(m_list) % 2 == 0){
        free(m_list);
    }
    m_list = nullptr;
}

void WakeList::set(void* ptr_queue, uint64_t n){
    reset();
    assert(ptr_queue != nullptr && "Null pointer");
    auto& queue = *(reinterpret_cast<util::CircularArray64k<Segment::SleepingBeauty>*>(ptr_queue));

#if defined(DEBUG)
    COUT_DEBUG("queue sz: " << queue.size() << ", num threads to wake up: " << n);
    for(uint64_t i = 0; i < queue.size(); i++){
        cout << "[" << i << "] " << queue[i].m_purpose << ": " << queue[i].m_promise << "\n";
    }
#endif

    if(n == 0){ // nop
        return ;
    } else if(n == 1){
        std::promise<void>* item = queue[0].m_promise;
        queue.pop();

        assert(reinterpret_cast<uint64_t>(item) % 2 == 0 && "because it is a pointer, it should be aligned by 4");
        m_list = reinterpret_cast<void*>( reinterpret_cast<uint64_t>(item) | 1ull );
    } else { // n > 1
        uint64_t* list = (uint64_t*) malloc(sizeof(uint64_t) * (n + 1));
        if(list == nullptr) throw std::bad_alloc{};
        list[0] = n;
        for(uint64_t i = 0; i < n; i++){
            list[i +1] = reinterpret_cast<uint64_t>(queue[0].m_promise);
            queue.pop();
        }
        m_list = list;
    }
}

void WakeList::wake() noexcept {
    COUT_DEBUG("list: " << m_list);

    if(m_list == nullptr){ // nop
        return ;
    } else if (reinterpret_cast<uint64_t>(m_list) % 2 == 1){ // the list contains a single value
        std::promise<void>* item = reinterpret_cast<std::promise<void>*>(reinterpret_cast<uint64_t>(m_list) & (std::numeric_limits<uint64_t>::max() << 1));
        COUT_DEBUG("thread to awake: " << item);
        item->set_value();
        m_list = nullptr;
    } else {
        uint64_t* header = reinterpret_cast<uint64_t*>(m_list);
        uint64_t size = header[0];
        std::promise<void>** __restrict items = reinterpret_cast<std::promise<void>**>(header +1);
        COUT_DEBUG("num threads to awake: " << size);
        for(uint64_t i = 0; i < size; i++){
            COUT_DEBUG("[" << i << "] " << (items + i));
            items[i]->set_value();
        }

        free(m_list); m_list = nullptr;
    }
}

} // namespace
