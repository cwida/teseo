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
#include "teseo/context/tctimer.hpp"

#include <cassert>
#include <condition_variable>
#include <event2/event.h>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/util/chrono.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/libevent.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::context {

// synchronisation to start/stop the background thread
static std::mutex g_mutex;
static std::condition_variable g_condvar;

// local exception type
#undef CURRENT_ERROR_TYPE
#define CURRENT_ERROR_TYPE InternalError

/*****************************************************************************
 *                                                                           *
 *   Init                                                                    *
 *                                                                           *
 *****************************************************************************/

TcTimer::TcTimer() : m_queue(nullptr), m_eventloop_exec(false) {
    util::LibEvent::init();
    m_queue = event_base_new();
    if(m_queue == nullptr) ERROR("Cannot initialise the libevent queue");

    start();
}

TcTimer::~TcTimer() {
    stop();
    event_base_free(m_queue); m_queue = nullptr;
    util::LibEvent::shutdown();
}

void TcTimer::start(){
    COUT_DEBUG("Starting...");
    unique_lock<mutex> lock(g_mutex);
    if(m_background_thread.joinable()) ERROR("Invalid state. The background thread is already running");

    auto timer = util::duration2timeval(0s); // fire the event immediately
    int rc = event_base_once(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, &TcTimer::callback_start, /* argument */ this, &timer);
    if(rc != 0) ERROR("Cannot initialise the event loop");

    m_background_thread = thread(&TcTimer::main_thread, this);

    g_condvar.wait(lock, [this](){ return m_eventloop_exec; });
    COUT_DEBUG("Started");
}

void TcTimer::stop(){
    COUT_DEBUG("Stopping...");
    scoped_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) return;
    int rc = event_base_loopbreak(m_queue);
    if(rc != 0) ERROR("event_base_loopbreak");
    m_background_thread.join();

    // remove all enqueued events still in the queue
    vector<struct event*> pending_events = util::LibEvent::get_pending_events(m_queue);
    COUT_DEBUG("Pending events to remove: " << pending_events.size());
    for(auto e : pending_events){
        auto callback_fn = event_get_callback(e);

        // Invoke the callback from here
        assert(callback_fn == &TcTimer::callback_invoke);
        callback_fn(-1, 0, event_get_callback_arg(e));
    }

    COUT_DEBUG("Stopped");
}

/*****************************************************************************
 *                                                                           *
 *   Background thread                                                       *
 *                                                                           *
 *****************************************************************************/

void TcTimer::main_thread(){
    COUT_DEBUG("Service thread started");
    util::Thread::set_name("Teseo.TcTimer");

    // delegate libevent to run the loop
    int rc = event_base_loop(m_queue, EVLOOP_NO_EXIT_ON_EMPTY);
    if(rc != 0){ COUT_DEBUG_FORCE("event_base_loop rc: " << rc); }

    COUT_DEBUG("Service thread stopped");
}


/*****************************************************************************
 *                                                                           *
 *   Callbacks                                                               *
 *                                                                           *
 *****************************************************************************/

void TcTimer::callback_start(int fd, short flags, void* event_argument){
    COUT_DEBUG("Event loop started");

    TcTimer* instance = reinterpret_cast<TcTimer*>(event_argument);
    {
        unique_lock<mutex> lock(g_mutex); // not really necessary, but it does silence tsan
        instance->m_eventloop_exec = true;
    }
    g_condvar.notify_all();
}

// static method, trampoline to `handle_callback'
void TcTimer::callback_invoke(evutil_socket_t /* fd == -1 */, short /* flags */, void* argument){
    assert(argument != nullptr && "Invalid pointer");
    Event* event = reinterpret_cast<Event*>(argument);

    // handle the callback
    event->m_thread_context->reset_cache_active_transactions();

    // Release the memory associated to the event
    event->m_thread_context.~shared_ptr<ThreadContext>();
    event_free(event->m_event); event->m_event = nullptr;
    free(event); event = nullptr;
}

void TcTimer::register_thread_context(shared_ptr<ThreadContext> thread_context){
    COUT_DEBUG("thread context: " << thread_context.get());

    assert(m_background_thread.joinable() && "The service is not running");
    Event* event_payload = (Event*) malloc(sizeof(Event));
    assert(event_payload != nullptr && "cannot allocate the timer event");
    if(event_payload == nullptr) throw std::bad_alloc{};

    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_invoke, event_payload);
    if(event == nullptr) throw std::bad_alloc{};

    // the payload to associated to the event
    event_payload->m_event = event;
    // explicitly invoke the ctor of the shared_ptr
    new (&(event_payload->m_thread_context)) shared_ptr<ThreadContext> (thread_context);

    // time when the event should be invoked
    struct timeval timer = util::duration2timeval(StaticConfiguration::tctimer_txnlist_lifetime);
    int rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: delay_rebalance, event_add failed");
        std::abort(); // not sure what we can do here
    }
}



} // namespace
