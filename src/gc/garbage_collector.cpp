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

#include "teseo/gc/garbage_collector.hpp"

#include <iostream>

#include "teseo/context/global_context.hpp"
#include "teseo/gc/item.hpp"
#include "teseo/gc/simple_queue.hpp"
#include "teseo/profiler/event_thread.hpp"
#include "teseo/profiler/scoped_timer.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::gc {

GarbageCollector::GarbageCollector(context::GlobalContext* global_context) : m_global_context(global_context), m_local(new SimpleQueue()), m_profiler(nullptr){

}

GarbageCollector::~GarbageCollector(){
    assert(m_shared_queues.empty() && "There are still clients");

    for(uint64_t i = 0; i < m_private_queues.size(); i++){
        remove_all(m_private_queues[i]);
        delete m_private_queues[i];
        m_private_queues[i] = nullptr;
    }

    remove_all(m_local);
    delete m_local; m_local = nullptr;
}


void GarbageCollector::remove_all(SimpleQueue* queue){
    while(!queue->empty()){
        queue->get(0).process();
        queue->pop();
    }
}

SimpleQueue* GarbageCollector::create_shared_queue(){
    util::WriteLatch lock(m_latch);
    SimpleQueue* queue = new SimpleQueue();
    m_shared_queues.push_back(queue);
    return queue;
}

void GarbageCollector::unregister(SimpleQueue* local, SimpleQueue* shared){
    util::WriteLatch lock(m_latch);
    if(shared != nullptr){
        uint64_t pos = 0;
        while(pos < m_shared_queues.size() && m_shared_queues[pos] != shared) pos++;
        assert(pos != m_shared_queues.size() && "Shared queue not registered");
        m_shared_queues.erase(m_shared_queues.begin() + pos);
        m_private_queues.push_back(shared);
    }

    if(local != nullptr){
        m_private_queues.push_back(local);
    }
}

void GarbageCollector::perform_gc_pass(uint64_t epoch, SimpleQueue* queue){
    profiler::ScopedTimer profiler { profiler::GC_PERFORM_GC_PASS, m_profiler };

    COUT_DEBUG("epoch: " << epoch << ", queue: " << queue);
    if(queue->empty()) return;

    bool success = true;
    int num_elts_removed = 0;
    int size = queue->size();
    while(num_elts_removed < size && success){
        success = queue->get(num_elts_removed).process_if(epoch);
        num_elts_removed += success;
    }
    queue->pop(num_elts_removed);
}

void GarbageCollector::execute(){
    profiler::ScopedTimer profiler { profiler::GC_EXECUTE, m_profiler };

    // current epoch
    auto epoch = m_global_context->min_epoch();
    COUT_DEBUG("epoch: " << epoch);

    util::WriteLatch lock(m_latch);

    // let's start with our internal queue
    { // restrict the scope for the profiler
        profiler::ScopedTimer profiler { profiler::GC_QUEUE_LOCAL, m_profiler };
        perform_gc_pass(epoch, m_local);
    }

    // shared queues
    { // restrict the scope for the profiler
        profiler::ScopedTimer profiler { profiler::GC_QUEUE_SHARED, m_profiler };
        for(uint64_t i =0, sz = m_shared_queues.size(); i < sz; i++){
            if(m_shared_queues[i]->full()){
                m_shared_queues[i]->resize();
            }
            perform_gc_pass(epoch, m_shared_queues[i]);
        }
    }

    // local queues
    // we proceed backwards as we are could erase some of the queues
    { // restrict the scope for the profiler
        profiler::ScopedTimer profiler { profiler::GC_QUEUE_PRIVATE, m_profiler };
        for(int64_t i = static_cast<int64_t>(m_private_queues.size()) -1; i >= 0; i--){
            perform_gc_pass(epoch, m_private_queues[i]);
            if(m_private_queues[i]->empty()){
                delete m_private_queues[i];
                m_private_queues[i] = nullptr;
                m_private_queues.erase(m_private_queues.begin() +i);
            }
        }
    }
}

void GarbageCollector::mark(void* pointer, void (*deleter)(void*)){
    COUT_DEBUG("pointer: " << pointer);

    util::WriteLatch lock(m_latch);
    if(m_local->full())
        m_local->resize();

    Item item { pointer, deleter };
#if !defined(NDEBUG)
    bool success = m_local->push(item);
    assert(success == true && "Item rejected");
#else
    m_local->push(item);
#endif
}

void GarbageCollector::set_profiler(profiler::EventThread* profiler){
    m_profiler = profiler;
}

void GarbageCollector::dump() const {
    cout << "[Garbage Collector]\n";
    cout << "Local queue: " << m_local << ", size: " << m_local->size() << "\n";
    cout << "Shared queues: \n";
    for(int i = 0, sz = m_shared_queues.size(); i < sz; i++){
        cout << "[" << i << "] address: " << m_shared_queues[i] << ", size: " << m_shared_queues[i]->size() << "\n";
    }
    cout << "Private queues: \n";
    for(int i = 0, sz = m_private_queues.size(); i < sz; i++){
        cout << "[" << i << "] address: " << m_private_queues[i] << ", size: " << m_private_queues[i]->size() << "\n";
    }
}

} // namespace
