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

#include "teseo/rebalance/async_service.hpp"

#include <cassert>
#include <condition_variable>
#include <event2/event.h>
#include <future>
#include <mutex>
#include <vector>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/util/chrono.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/libevent.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::rebalance {


/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/

AsyncService::AsyncService(context::GlobalContext* global_context) : m_global_context(global_context), m_num_threads_starting(0) {
    util::LibEvent::init();
    m_queue = event_base_new();
    if(m_queue == nullptr) ERROR("Cannot initialise the libevent queue");
}

AsyncService::~AsyncService(){
    stop();

    while(!m_requests.empty()){ delete m_requests[0]; m_requests.pop(); }
    remove_pending_events(); // in case the service was already stopped and new events were created in the meanwhile
    event_base_free(m_queue); m_queue = nullptr;
    util::LibEvent::shutdown();
}

AsyncService::Request::Request(AsyncService* instance, const memstore::Context& context, const memstore::Key& key) : m_instance(instance), m_context(context), m_key(key){ }

void AsyncService::start(){
    if( context::StaticConfiguration::async_num_threads == 0) {
        COUT_DEBUG_FORCE("Service disabled");
        return;
    }

    COUT_DEBUG("Starting...");

    m_mutex.lock();
    if(!m_workers.empty()){
        m_mutex.unlock();
        ERROR("Invalid state. The background thread is already running");
    }

    m_num_threads_starting = /* timer */ 1 + /* workers */ context::StaticConfiguration::async_num_threads;

    // Start the workers
    m_workers.reserve( context::StaticConfiguration::async_num_threads );
    for(int thread_id = 0, num_threads = context::StaticConfiguration::async_num_threads; thread_id < num_threads; thread_id ++){
        m_workers.emplace_back(&AsyncService::worker_thread, this, thread_id);
    }

    // Start the master/timer
    m_timer = thread(&AsyncService::master_thread, this);

    auto timer = util::duration2timeval(0s); // fire the event immediately
    int rc = event_base_once(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, &AsyncService::callback_master_start, /* argument */ this, &timer);
    if(rc != 0) ERROR("Cannot initialise the event loop");
    m_condvar.wait(m_mutex, [this](){ return m_num_threads_starting == 0; });

    m_mutex.unlock();

    COUT_DEBUG("Started");

}

void AsyncService::stop(){
    if( context::StaticConfiguration::async_num_threads == 0) {
        COUT_DEBUG_FORCE("Service disabled");
        return;
    }

    m_mutex.lock();
    if(m_workers.empty()){ m_mutex.unlock(); return; }
    COUT_DEBUG("Stopping...");

    // stop the timer
    int rc = event_base_loopbreak(m_queue);
    if(rc != 0) ERROR("event_base_loopbreak");
    m_timer.join();

    // remove all enqueued events still in the queue
    remove_pending_events();

    // stop the worker threads
    memstore::Context dummy { nullptr };
    for(int thread_id = 0, num_threads = m_workers.size(); thread_id < num_threads; thread_id ++ ){
        m_requests.prepend(new Request{ this, dummy, memstore::KEY_MAX } );
    }
    m_condvar.notify_all();

    m_mutex.unlock();

    for(auto& thread : m_workers){ thread.join(); }

    m_mutex.lock();
    m_workers.clear();
    // Remove the pending requests
    while(!m_requests.empty()){ delete m_requests[0]; m_requests.pop(); }
    m_mutex.unlock();

    COUT_DEBUG("Stopped");
}

void AsyncService::remove_pending_events(){
    std::vector<struct event*> pending_events = util::LibEvent::get_pending_events(m_queue);
    COUT_DEBUG("Pending events to remove: " << pending_events.size());
    for(auto e : pending_events){
        delete (Request*) event_get_callback_arg(e);
        event_free(e);
    }
}


void AsyncService::request(const memstore::Context& context){
    // create the event
    Request* payload = new Request { this, memstore::Context { context.m_tree }, memstore::Segment::get_lfkey(context) };
    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_master_request, payload);
    if(event == nullptr) throw std::bad_alloc{};
    COUT_DEBUG("allocate event: " << event);

    // activate the event
    auto timer = util::duration2timeval(context::StaticConfiguration::async_delay + 2ms);
    int rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: " << DEBUG_WHOAMI << ", event_add failed");
        std::abort(); // not sure what we can do here
    }
}

/*****************************************************************************
 *                                                                           *
 *   Timer (master)                                                          *
 *                                                                           *
 *****************************************************************************/

void AsyncService::master_thread(){
    COUT_DEBUG("Master started");
    util::Thread::set_name("Teseo.AsTimer");

    // delegate libevent to run the loop
    int rc = event_base_loop(m_queue, EVLOOP_NO_EXIT_ON_EMPTY);
    if(rc != 0){ COUT_DEBUG_FORCE("event_base_loop rc: " << rc); }

    COUT_DEBUG("Master stopped");
}

void AsyncService::callback_master_start(int fd, short flags, void* /* AsyncService instance */ event_argument) {
    COUT_DEBUG("Event loop started");
    auto instance = reinterpret_cast<AsyncService*>(event_argument);
    instance->m_num_threads_starting--;
    instance->m_condvar.notify_all();
}

void AsyncService::callback_master_request(evutil_socket_t /* fd == -1 */, short /* flags */, void* argument){
    assert(argument != nullptr && "Invalid pointer");
    auto request = reinterpret_cast<Request*>(argument);

    // handle the callback
    AsyncService* instance = request->m_instance;
    instance->handle_master_request(request);

    // deallocate the libevent event
    event* event = event_base_get_running_event(instance->m_queue);
    free(event);

    // don't free the `request', it's responsibility of the workers to do it
}

void AsyncService::handle_master_request(Request* request) {
    m_mutex.lock();
    m_requests.append(request);
    m_mutex.unlock();

    m_condvar.notify_one();
}

/*****************************************************************************
 *                                                                           *
 *   Rebalancer (worker)                                                     *
 *                                                                           *
 *****************************************************************************/
void AsyncService::worker_thread(int thread_id){
    COUT_DEBUG("Worker #" << thread_id << " started");
    set_thread_name(thread_id);

    // signal the caller this thread has started
    m_num_threads_starting--;
    m_condvar.notify_all();

    m_global_context->register_thread();

    while(true){
        // extract the next key to process
        m_mutex.lock();
        m_condvar.wait(m_mutex, [this]{ return !m_requests.empty(); });
        Request* request = m_requests[0];
        m_requests.pop();
        m_mutex.unlock();
        if(request->m_key == memstore::KEY_MAX){ delete request; break; }; // terminate the while loop

        handle_worker_request(*request);

        delete request;
    }

    m_global_context->unregister_thread();
    COUT_DEBUG("Worker #" << thread_id << " stopped");
}

void AsyncService::handle_worker_request(Request& request){
    profiler::ScopedTimer profiler { profiler::ARS_HANDLE_REQUEST };
    COUT_DEBUG("Key: " << request.m_key);
    memstore::Context& context = request.m_context;
    context::ScopedEpoch epoch; // protect from the GC

    try {
        context.writer_enter(request.m_key);

        if(memstore::Segment::get_lfkey(context) != request.m_key || !context.m_segment->need_async_rebalance()){
            // we're done, that was easy
            context.writer_exit();
            return;
        }

        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality_ub() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();

        // the crawler dtor will release the acquired segments ...

    } catch (Abort) {
        /* nop */
    } catch (RebalanceNotNecessary){
        /* nop */
    }
}

void AsyncService::set_thread_name(int thread_id){
    string thread_name = string("Teseo.Async #") + to_string(thread_id);
    util::Thread::set_name(thread_name);
}



} // namespace
