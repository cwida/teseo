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

#include "teseo/gc/tc_queue.hpp"

#include <cassert>

#include "teseo/context/static_configuration.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/gc/item.hpp"
#include "teseo/gc/simple_queue.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

namespace teseo::gc {

TcQueue::TcQueue(GarbageCollector* gc) : m_local( new SimpleQueue() ), m_shared( gc->create_shared_queue() ), m_gc (gc)
#if !defined(NDEBUG)
    , m_thread_id(util::Thread::get_thread_id())
#endif
{

}

TcQueue::~TcQueue(){
    release();
}

void TcQueue::mark(void* pointer, void (*deleter)(void*)){
    COUT_DEBUG("pointer: " << pointer);

    // Invoking the local GC is not thread safe and must be done only inside the same thread owning this queue
    assert(m_thread_id == util::Thread::get_thread_id() && "Attempting to invoke the local GC from another thread");

    // Try to flush the local queue, as far as possible
    bool success = true;
    int num_elts_flushed = 0;
    int sz = m_local->size();
    while(num_elts_flushed < sz && success){
        Item& item = m_local->get(num_elts_flushed);
        success = m_shared->push(item);
        num_elts_flushed += success;
    }
    m_local->pop(num_elts_flushed);

    // Finally, mark the object we are supposed to delete either in the shared or in the local queue
    Item item { pointer, deleter };
    success = false;
    if(m_local->empty()){
        success = m_shared->push(item);
    }
    if(!success){
        success = m_local->push(item);
        if(!success){
            m_local->resize();
            m_local->push(item);
        }
    }
}

void TcQueue::release(){
    if(m_local != nullptr){
        m_gc->unregister(m_local, m_shared);
        m_local = m_shared = nullptr;
    }
}


} // namespace


