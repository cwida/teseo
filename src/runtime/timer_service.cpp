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
#include "teseo/runtime/timer_service.hpp"

#include <cassert>
#include <event2/event.h>
#include <teseo/runtime/queue.hpp>
#include <teseo/runtime/timer_service.hpp>
#include <stdexcept>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/gc/tc_queue.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/runtime/task.hpp"
#include "teseo/util/chrono.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/libevent.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::runtime {

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

TimerService::TimerService(runtime::Runtime* runtime) : m_queue(nullptr), m_runtime(runtime), m_eventloop_exec(false), m_gc_queue(nullptr) {
    util::LibEvent::init();
    m_queue = event_base_new();
    if(m_queue == nullptr) ERROR("Cannot initialise the libevent queue");

    start();
}

TimerService::~TimerService() {
    stop();

    remove_pending_events(); // avoid memory leaks

    event_base_free(m_queue); m_queue = nullptr;
    util::LibEvent::shutdown();
}

void TimerService::start(){
    COUT_DEBUG("Starting...");
    unique_lock<mutex> lock(g_mutex);
    if(m_background_thread.joinable()) ERROR("Invalid state. The background thread is already running");

    auto timer = util::duration2timeval(0s); // fire the event immediately
    int rc = event_base_once(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, &TimerService::callback_start, /* argument */ this, &timer);
    if(rc != 0) ERROR("Cannot initialise the event loop");
    m_background_thread = thread(&TimerService::main_thread, this);

    g_condvar.wait(lock, [this](){ return m_eventloop_exec; });
    COUT_DEBUG("Started");
}

void TimerService::stop(){
    scoped_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) return;
    COUT_DEBUG("Stopping...");
    int rc = event_base_loopbreak(m_queue);
    if(rc != 0) ERROR("event_base_loopbreak");
    m_background_thread.join();
    remove_pending_events();
    COUT_DEBUG("Stopped");
}

void TimerService::remove_pending_events(){
    vector<struct event*> pending_events = util::LibEvent::get_pending_events(m_queue);
    COUT_DEBUG("Pending events to remove: " << pending_events.size());
    for(auto e : pending_events){
        auto callback_fn = event_get_callback(e);

        // Invoke the callback from here
        if(callback_fn == &TimerService::callback_active_transactions){
            //callback_fn(-1, 0, event_get_callback_arg(e)); // the GC was removed
            EventActiveTransactions* at_event = reinterpret_cast<EventActiveTransactions*>(e);
            auto object = at_event->m_thread_context->reset_cache_active_transactions();
            context::ThreadContext::delete_transaction_sequence(object);
            at_event->m_thread_context->decr_ref_count();
            free(at_event);
            event_free(e);
        } else if(callback_fn == &TimerService::callback_runtime){ // ignore all invocations to the runtime
            auto runtime_event = reinterpret_cast<EventRuntime*>(event_get_callback_arg(e));
            Runtime::delete_task(runtime_event->m_task);
            free(runtime_event);
            event_free(e);
        } else {
            assert(0 && "Unknown event type");
        }

    }
}

/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/

void TimerService::refresh_active_transactions(){
    assert(m_background_thread.joinable() && "The service is not running");

    // retrieve the thread context
    auto thread_context = context::thread_context_if_exists();
    assert(thread_context != nullptr && "There should always be a registered thread context");

    EventActiveTransactions* event_payload = (EventActiveTransactions*) malloc(sizeof(EventActiveTransactions));
    assert(event_payload != nullptr && "cannot allocate the timer event");
    if(event_payload == nullptr) throw std::bad_alloc{};

    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_active_transactions, event_payload);
    if(event == nullptr) throw std::bad_alloc{};

    //  set the payload to associated to the event
    event_payload->m_event = event;
    event_payload->m_gc = m_gc_queue;
    thread_context->incr_ref_count();
    event_payload->m_thread_context = thread_context;

    // time when the event should be invoked
    struct timeval timer = util::duration2timeval(context::StaticConfiguration::runtime_txnlist_refresh);
    int rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: delay_rebalance, event_add failed");
        std::abort(); // not sure what we can do here
    }
}

void TimerService::schedule_task(Task task, int worker_id, chrono::milliseconds when){
    COUT_DEBUG("task: " << task << ", worker_id: " << worker_id);

    EventRuntime* event_payload = (EventRuntime*) malloc(sizeof(EventRuntime));
    assert(event_payload != nullptr && "cannot allocate the runtime event");
    if(event_payload == nullptr) throw std::bad_alloc{};

    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_runtime, event_payload);
    if(event == nullptr) throw std::bad_alloc{};

    //  set the payload to associated to the event
    event_payload->m_event = event;
    event_payload->m_runtime = m_runtime;
    event_payload->m_task = task;
    event_payload->m_worker_id = worker_id;

    struct timeval timeout = util::duration2timeval(when);
    int rc = event_add(event, &timeout);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: delay_rebalance, event_add failed");
        std::abort(); // not sure what we can do here
    }
}

/*****************************************************************************
 *                                                                           *
 *   Callbacks                                                               *
 *                                                                           *
 *****************************************************************************/

void TimerService::main_thread(){
    COUT_DEBUG("Service thread started");
    util::Thread::set_name("Teseo.Timer");
    m_gc_queue = new gc::TcQueue(m_runtime->next_gc()); // init the GC

    // delegate libevent to run the loop
    int rc = event_base_loop(m_queue, EVLOOP_NO_EXIT_ON_EMPTY);
    if(rc != 0){ COUT_DEBUG_FORCE("event_base_loop rc: " << rc); }

    delete m_gc_queue; m_gc_queue = nullptr;
    COUT_DEBUG("Service thread stopped");
}

void TimerService::callback_start(int fd, short flags, void* event_argument){
    COUT_DEBUG("Event loop started");

    TimerService* instance = reinterpret_cast<TimerService*>(event_argument);

    {
        unique_lock<mutex> lock(g_mutex); // not really necessary, but it does silence tsan
        instance->m_eventloop_exec = true;
    }
    g_condvar.notify_all();
}

void TimerService::callback_active_transactions(evutil_socket_t /* fd == -1 */, short /* flags */, void* argument){
    assert(argument != nullptr && "Invalid pointer");
    EventActiveTransactions* event = reinterpret_cast<EventActiveTransactions*>(argument);

    // handle the callback
    auto object = event->m_thread_context->reset_cache_active_transactions();
    event->m_gc->mark(object, &context::ThreadContext::delete_transaction_sequence);

    // release the memory associated to the event
    event->m_thread_context->decr_ref_count();
    event_free(event->m_event); event->m_event = nullptr;
    free(event); event = nullptr;
}

void TimerService::callback_runtime(int fd, short flags, void* event_argument){
    EventRuntime* payload = reinterpret_cast<EventRuntime*>(event_argument);
    Runtime* runtime = payload->m_runtime;
    runtime->execute(payload->m_task, payload->m_worker_id);
    event_free(payload->m_event); payload->m_event = nullptr;
    free(payload);
}


} // namespace


