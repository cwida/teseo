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

#include "teseo/context/garbage_collector.hpp"

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

#include "teseo/context/global_context.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/debug.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/thread.hpp"

using namespace std;

namespace teseo::context {

GarbageCollector::GarbageCollector(GlobalContext* global_context) : GarbageCollector(global_context, chrono::duration_cast<chrono::milliseconds>(chrono::seconds(1))) { }

GarbageCollector::GarbageCollector(GlobalContext* global_context, chrono::milliseconds timer_interval) :
        m_global_context(global_context), m_timer_interval(timer_interval) {
    m_thread_can_execute = false;
    COUT_DEBUG("Initialised");

    start();
}

GarbageCollector::~GarbageCollector() {
    stop();

    // clean up
    for(size_t i = 0, sz = m_items_to_delete.size(); i < sz; i++){
        auto descr = m_items_to_delete[i];
        descr->m_deleter->free(descr->m_pointer);
        delete descr; descr = nullptr;
    }

    COUT_DEBUG("Destroyed");
}

GarbageCollector::DeleteInterface::~DeleteInterface() { }

void GarbageCollector::start(){
    COUT_DEBUG("Starting...");
    unique_lock<mutex> lock(m_mutex);
    if(m_thread_can_execute) RAISE_EXCEPTION(Exception, "Invalid state. The background thread is already running");

    m_thread_can_execute = true;
    util::barrier();

    m_background_thread = thread(&GarbageCollector::run, this);

    m_condvar.wait(lock, [this](){ return m_thread_is_running; });
}

void GarbageCollector::stop(){
    COUT_DEBUG("Stopping...");
    m_thread_can_execute = false;
    util::barrier();
    if(m_background_thread.joinable())
        m_background_thread.join(); // wait for the thread to finish
}

// Background thread
void GarbageCollector::run(){
    COUT_DEBUG("Started");
    util::Thread::set_name("Teseo.GC");
#if defined(HAVE_PROFILER)
    m_global_context->register_thread();
#endif

    { // ensure that #notify is invoked only after m_thread_is_running == true
        scoped_lock<mutex> lock(m_mutex);
        m_thread_is_running = true;
    }
    m_condvar.notify_one();

    while(m_thread_can_execute){
        std::this_thread::sleep_for(m_timer_interval);
        perform_gc_pass();
    }

#if defined(HAVE_PROFILER)
    m_global_context->unregister_thread();
#endif

    m_thread_is_running = false;
    COUT_DEBUG("Stopped");
}

void GarbageCollector::perform_gc_pass(){
    COUT_DEBUG("Performing a pass of garbage collection...");
    profiler::ScopedTimer profiler { profiler::GC_PERFORM_GC_PASS };

    // current epoch
    auto epoch = m_global_context->min_epoch();
    vector<Item*> items;
    items.reserve(/* magic number */ 64);
    {  // restrict the scope
        profiler::ScopedTimer prof_gather_items { profiler::GC_GATHER_ITEMS };
        lock_guard<mutex> lock(m_mutex);
        for(uint64_t i = 0, sz = m_items_to_delete.size(); i < sz; i++){
            if(m_items_to_delete[0]->m_timestamp > epoch) break; // done
            items.push_back(m_items_to_delete[0]);
            m_items_to_delete.pop();
        }
    }

    // remove the objects identified for deletion
    profiler::ScopedTimer prof_delete_items { profiler::GC_DELETE_ITEMS };
    COUT_DEBUG("Min epoch: " << epoch);
    for(auto& item : items){
        COUT_DEBUG("Deallocating " << item->m_pointer << " (epoch: " << item->m_timestamp << ")");
        item->m_deleter->free(item->m_pointer);
        delete item; item = nullptr;
    }

    COUT_DEBUG("Pass finished");
}

void GarbageCollector::dump(std::ostream& out) const {
    auto current_epoch = m_global_context->min_epoch();

    scoped_lock<mutex> lock(m_mutex);
    out << "[GarbageCollector] min epoch: " << current_epoch << ", # items: " << m_items_to_delete.size();
    if(m_items_to_delete.empty()){
        out << " -- empty";
    } else {
        out << ": ";
        for(size_t i = 0; i < m_items_to_delete.size(); i++){
            if(i > 0) out << ", ";
            Item* item = m_items_to_delete[i];
            out << "{epoch: " << item->m_timestamp << ", pointer: " << item->m_pointer << "}";
        }
    }

    out << "\n";
}

void GarbageCollector::dump() const{
    dump(cout);
}

} // namespace
