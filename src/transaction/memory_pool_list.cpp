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

#include "teseo/transaction/memory_pool_list.hpp"

#include <mutex>

#include "teseo/context/static_configuration.hpp"
#include "teseo/transaction/memory_pool.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::transaction {

MemoryPoolList::MemoryPoolList() {
    /* nop */
}

MemoryPoolList::~MemoryPoolList(){
    while(!m_ready.empty()){
        MemoryPool::destroy(m_ready[0]);
        m_ready.pop();
    }
    while(!m_idle.empty()){
        MemoryPool::destroy(m_idle[0]);
        m_idle.pop();
    }
}

MemoryPool* MemoryPoolList::acquire(){
    return exchange(nullptr);
}

MemoryPool* MemoryPoolList::exchange(MemoryPool* mempool_old) {
    MemoryPool* mempool_new = nullptr;

    m_latch.lock();

    if(!m_ready.empty()){
        assert(m_ready[0] != nullptr);
        assert(m_ready[0]->fill_factor() <= context::StaticConfiguration::transaction_memory_pool_ffreuse);
        mempool_new = m_ready[0];
        m_ready.pop();
    }

    if(mempool_old != nullptr){
        m_idle.append(mempool_old);
        //m_max_num_ready_lists ++;
    }

    m_latch.unlock();

    if(mempool_new == nullptr){
        mempool_new = MemoryPool::create();
    }

    COUT_DEBUG("old: " << mempool_old << ", new: " << mempool_new)
    return mempool_new;
}

void MemoryPoolList::release(MemoryPool* mempool){
    if(mempool != nullptr){
        scoped_lock<util::SpinLock> lock(m_latch);
        m_idle.append(mempool);
        //if(m_max_num_ready_lists > 1) { m_max_num_ready_lists--; }
    }
}

void MemoryPoolList::cleanup(){
    scoped_lock<util::SpinLock> lock(m_latch);

    // let's start with the idle lists
    for(uint64_t i = 0, sz = m_idle.size(); i < sz; i++){
        if(m_idle[i] == nullptr) continue;
        m_idle[i]->rebuild_free_list();
        if(m_idle[i]->is_empty() && m_ready.size() >= context::StaticConfiguration::transaction_memory_pool_list_cache_size){
            MemoryPool::destroy(m_idle[i]);
            m_idle[i] = nullptr;
        } else if(m_idle[i]->fill_factor() <= context::StaticConfiguration::transaction_memory_pool_ffreuse){
            m_ready.append(m_idle[i]);
            m_idle[i] = nullptr;
        }
    }
    while(!m_idle.empty() && m_idle[0] == nullptr){ m_idle.pop(); }

    for(uint64_t i = 0, sz = m_ready.size(); i < sz; i++){
        assert(m_ready[i] != nullptr);
        m_ready[i]->rebuild_free_list();
    }

    // remove the extra memory pools
    while(m_ready.size() > context::StaticConfiguration::transaction_memory_pool_list_cache_size && m_ready[0]->is_empty()){
        MemoryPool::destroy(m_ready[0]);
        m_ready.pop();
    }

    COUT_DEBUG("idle queue size: " << m_idle.size() << ", ready queue size: " << m_ready.size());
}

} // namespace
