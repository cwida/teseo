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

#include "teseo/rebalance/merger_service.hpp"

#include <cassert>
#include <condition_variable>
#include <event2/event.h>
#include <future>
#include <mutex>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/rebalance/merge_operator.hpp"
#include "teseo/util/chrono.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/libevent.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::rebalance {

// synchronisation to start/stop the background thread
static mutex g_mutex;
static condition_variable g_condvar;

struct MergerCallbackData {
    MergerService* m_instance; // pointer to the background service
    promise<void>* m_producer; // pointer to the producer/consumer queue, only used for synchronous invocations through #execute_now()
};

MergerService::MergerService(memstore::Memstore* owner) : m_queue(nullptr), m_eventloop_exec(false), m_memstore(owner){
    if(m_memstore == nullptr) { throw std::invalid_argument("the sparse array instance is a nullptr"); }

    util::LibEvent::init();
    m_queue = event_base_new();
    if(m_queue == nullptr) ERROR("Cannot initialise the libevent queue");
}

MergerService::~MergerService() {
    stop();
    event_base_free(m_queue); m_queue = nullptr;
    util::LibEvent::shutdown();
}

void MergerService::start(){
    COUT_DEBUG("Starting...");
    unique_lock<mutex> lock(g_mutex);
    if(m_background_thread.joinable()) ERROR("Invalid state. The background thread is already running");
    m_eventloop_exec = false;

    auto timer = util::duration2timeval(0s); // fire the event immediately
    int rc = event_base_once(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, &MergerService::callback_start, /* argument */ this, &timer);
    if(rc != 0) ERROR("Cannot initialise the event loop");

    m_background_thread = thread(&MergerService::main_thread, this);

    // create the periodic event, to invoke the service every `m_time_interval' millisecs
    MergerCallbackData* event_payload = (MergerCallbackData*) malloc(sizeof(MergerCallbackData));
    if(event_payload == nullptr) throw std::bad_alloc{};
    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT | EV_PERSIST, callback_execute, event_payload);
    if(event == nullptr) throw std::bad_alloc{};
    event_payload->m_instance = this;
    event_payload->m_producer = nullptr;
    timer = util::duration2timeval(context::StaticConfiguration::merger_frequency);
    rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: MergerService::start, event_add failed");
        std::abort(); // not sure what we can do here
    }

    g_condvar.wait(lock, [this](){ return m_eventloop_exec; });
    COUT_DEBUG("Started");
}

void MergerService::stop(){
    scoped_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) return;
    COUT_DEBUG("Stopping...");

    int rc = event_base_loopbreak(m_queue);
    if(rc != 0) ERROR("event_base_loopbreak");
    m_background_thread.join();

    // remove all enqueued events still in the queue
    std::vector<struct event*> pending_events = util::LibEvent::get_pending_events(m_queue);
    COUT_DEBUG("Pending events to remove: " << pending_events.size());
    for(auto e : pending_events){
        free(event_get_callback_arg(e));
        event_free(e);
    }

    COUT_DEBUG("Stopped");
}

void MergerService::execute_now(){
    // This method is not completely thread safe, because other threads may still stop the service while
    // the invoker of this method is still waiting for the request to be accomplished. But, anyway, this
    // method is supposed to be used only for debugging purposes.
    unique_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) { ERROR("The service is not running"); }
    lock.unlock();

    // init the producer/consumer queue
    promise<void> producer;
    auto consumer = producer.get_future();

    // create the event
    MergerCallbackData* event_payload = (MergerCallbackData*) malloc(sizeof(MergerCallbackData));
    if(event_payload == nullptr) throw std::bad_alloc{};
    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_execute, event_payload);
    if(event == nullptr) throw std::bad_alloc{};
    event_payload->m_instance = this;
    event_payload->m_producer = &producer;
    // int rc = event_add(event, nullptr); // nullptr -> execute immediately // it didn't work!
    auto timer = util::duration2timeval(0s);
    int rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: MergerService::execute_now, event_add failed");
        std::abort(); // not sure what we can do here
    }

    // wait for the execution to complete
    consumer.get();
}

void MergerService::callback_start(int fd, short flags, void* event_argument){
    COUT_DEBUG("Event loop started");

    MergerService* instance = reinterpret_cast<MergerService*>(event_argument);
    {
        unique_lock<mutex> lock(g_mutex); // not really necessary, but it does silence tsan
        instance->m_eventloop_exec = true;
    }
    g_condvar.notify_all();
}

void MergerService::callback_execute(int fd, short flags, void* /* MergerCallbackData */ raw_callback_arg){
    MergerCallbackData* callback_arg = reinterpret_cast<MergerCallbackData*>(raw_callback_arg);

    MergerService* instance = callback_arg->m_instance;
    memstore::Memstore* memstore = instance->m_memstore;

    memstore::Context context { memstore };
    MergeOperator merge_operator { context };
    merge_operator.execute();

    if(callback_arg->m_producer != nullptr){ // this is not the persistent event
        callback_arg->m_producer->set_value();
        free(callback_arg);
        free(event_base_get_running_event(instance->m_queue));
    }
}

void MergerService::main_thread(){
    COUT_DEBUG("Service thread started");
    util::Thread::set_name("Teseo.Merger");
    m_memstore->global_context()->register_thread();

    // delegate libevent to run the loop
    int rc = event_base_loop(m_queue, EVLOOP_NO_EXIT_ON_EMPTY);
    if(rc != 0){ COUT_DEBUG_FORCE("event_base_loop rc: " << rc); }

    m_memstore->global_context()->unregister_thread();
    COUT_DEBUG("Service thread stopped");
}


} // namespace


