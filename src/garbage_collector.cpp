/*
 * garbage_collector.cpp
 *
 *  Created on: 07 Feb 2019
 *      Author: Dean De Leo
 */
#include "garbage_collector.hpp"

#include <chrono>
#include <iostream>
#include <mutex>

#include "context.hpp"
#include "utility.hpp"

using namespace std;

namespace teseo::internal {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex g_debugging_mutex [[maybe_unused]]; // context.cpp
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[GarbageCollector::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Implementation                                                          *
 *                                                                           *
 *****************************************************************************/

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
    barrier();

    m_background_thread = thread(&GarbageCollector::run, this);

    m_condvar.wait(lock, [this](){ return m_thread_is_running; });
}

void GarbageCollector::stop(){
    COUT_DEBUG("Stopping...");
    m_thread_can_execute = false;
    barrier();
    if(m_background_thread.joinable())
        m_background_thread.join(); // wait for the thread to finish
}

// Background thread
void GarbageCollector::run(){
    COUT_DEBUG("Started");
    set_thread_name("Teseo.GC");

    { // ensure that #notify is invoked only after m_thread_is_running == true
        scoped_lock<mutex> lock(m_mutex);
        m_thread_is_running = true;
    }
    m_condvar.notify_one();

    while(m_thread_can_execute){
        std::this_thread::sleep_for(m_timer_interval);
        perform_gc_pass();
    }
    m_thread_is_running = false;
    COUT_DEBUG("Stopped");
}

void GarbageCollector::perform_gc_pass(){
    COUT_DEBUG("Performing a pass of garbage collection...");

    // current epoch
    auto epoch = m_global_context->min_epoch();
    vector<Item*> items;
    items.reserve(/* magic number */ 64);
    {  // restrict the scope
        lock_guard<mutex> lock(m_mutex);
        for(uint64_t i = 0, sz = m_items_to_delete.size(); i < sz; i++){
            if(m_items_to_delete[0]->m_timestamp > epoch) break; // done
            items.push_back(m_items_to_delete[0]);
            m_items_to_delete.pop();
        }
    }

    // remove the objects identified for deletion
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
