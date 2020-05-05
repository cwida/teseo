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
#include "teseo/context/tc_list.hpp"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/gc/garbage_collector.hpp"

using namespace std;

namespace teseo::context {

TcList::TcList(GlobalContext* global_context) : m_global_context(global_context), m_list(nullptr), m_size(0), m_capacity(StaticConfiguration::context_tclist_initial_capacity -1){
    m_list = (ThreadContext**) calloc(sizeof(ThreadContext*), m_capacity);
    if(m_list == nullptr) throw std::bad_alloc{};
}

TcList::~TcList(){
    free(m_list); m_list = nullptr;
}

void TcList::resize(){
    uint32_t new_capacity = (m_capacity +1) * 2;
    ThreadContext** new_list = (ThreadContext**)  calloc(sizeof(ThreadContext*), new_capacity);
    if(new_list == nullptr) throw std::bad_alloc{};
    memcpy(new_list, m_list, m_size * sizeof(ThreadContext*));
    m_global_context->gc()->mark(m_list, free);
    m_list = new_list;
    m_capacity = new_capacity -1; // 64 -> 63, last position is a marker with a nullptr
}

void TcList::insert(ThreadContext* thread_context){
   scoped_lock<util::OptimisticLatch<0>> xlock(m_latch);
   if(m_size == capacity()) resize();
   assert(m_size < capacity() && "The list is full");
   m_list[m_size] = thread_context;
   m_size++;
}

void TcList::remove(ThreadContext* thread_context){
    // atb it is safer to acquire the latch in the global context. Otherwise the logic needs to be
    // readjusted a bit to move the xlock here.
    //scoped_lock<util::OptimisticLatch<0>> xlock(m_latch);
    assert(m_latch.is_locked() && "It should have been acquired in the global context");

    ThreadContext** __restrict tc_list = m_list;
    uint32_t pos = 0;
    uint32_t size = m_size;

    while(pos < size && tc_list[pos] != thread_context) pos++;
    assert(pos < size && "Thread context not found");
    // shift the existing elements to the left
    while(pos < size){ tc_list[pos] = tc_list[pos +1]; pos++; }  /* let it overflow at size+1, it will copy nullptr */
    m_size = size - 1;
}

uint32_t TcList::capacity() const {
    return m_capacity;
}

} // namespace

