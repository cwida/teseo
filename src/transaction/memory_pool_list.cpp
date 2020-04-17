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

using namespace std;

namespace teseo::transaction {

MemoryPoolList::MemoryPoolList() : m_next(0) {
    /* nop */
}

MemoryPoolList::~MemoryPoolList(){
    while(!m_queue.empty()){
        delete m_queue[0];
        m_queue.pop();
    }
}

MemoryPool* MemoryPoolList::acquire(){
    return exchange(nullptr);
}

MemoryPool* MemoryPoolList::exchange(MemoryPool* mempool_old) {
    MemoryPool* mempool_new = nullptr;

    scoped_lock<util::SpinLock> lock(m_latch);
    if(!m_queue.empty()){
        uint64_t i = 0;
        do {
            MemoryPool* candidate = m_queue[i];
            if(candidate != nullptr && candidate->fill_factor() <= context::StaticConfiguration::transaction_memory_pool_ffreuse){
                mempool_new = candidate;
                m_queue[i] = nullptr;
            }

            i++;
        } while(i < m_queue.size() && mempool_new == nullptr);

        while(m_queue[0] == nullptr){ m_queue.pop(); }
    }

    if(mempool_new == nullptr){
        mempool_new = new MemoryPool();
    }

    if(mempool_old != nullptr){
        m_queue.append(mempool_old);
    }

    return mempool_new;
}

void MemoryPoolList::release(MemoryPool* mempool){
    if(mempool != nullptr){
        scoped_lock<util::SpinLock> lock(m_latch);
        m_queue.append(mempool);
    }
}

} // namespace
