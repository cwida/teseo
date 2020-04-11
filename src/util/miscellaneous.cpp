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

#include "miscellaneous.hpp"

#include <cassert>
#include <cstring>
#include <event2/event.h> // libevent_global_shutdown
#include <event2/thread.h> // evthread_use_pthreads
#include <pthread.h>
#include <syscall.h>
#include <unistd.h>

#include "error.hpp"

using namespace std;
using namespace teseo::internal;

namespace teseo::internal::util {


int64_t get_thread_id(){
    auto tid = (int64_t) syscall(SYS_gettid);
    assert(tid > 0);
    return tid;
}

void set_thread_name(const std::string& name){
    pthread_t thread_id = pthread_self();
    string truncated_name = name.substr(0, 15);
    int rc = pthread_setname_np(thread_id, truncated_name.c_str());
    if(rc != 0){
        RAISE_EXCEPTION(InternalError, "[set_thread_name] error: " << strerror(errno) << " (" << errno << ")");
    }
}

/*********************************************************************************************************************
 *                                                                                                                   *
 *  libevent                                                                                                         *
 *                                                                                                                   *
 *********************************************************************************************************************/
static int g_libevent_active_clients = 0; // number of invocations libevent_init - invocations to libevent_shutdown
static mutex g_libevent_mutex; // concurrency protection for multiple invocations to libevent_init and libevent_shutdown

void libevent_init(){
    scoped_lock<mutex> lock(g_libevent_mutex);
    assert(g_libevent_active_clients >= 0 && "Negative counter");
    if(g_libevent_active_clients > 0){
        g_libevent_active_clients += 1;
    } else {
        int rc = evthread_use_pthreads();
        if(rc != 0) RAISE_EXCEPTION(InternalError, "[libevent_init] Cannot initialise the library");
        assert(g_libevent_active_clients == 0 && "Multiple inits of the same library?");
        g_libevent_active_clients = 1;
    }
}

void libevent_shutdown(){
    scoped_lock<mutex> lock(g_libevent_mutex);
    assert(g_libevent_active_clients >= 0 && "Negative counter");
    if(g_libevent_active_clients == 1){
       libevent_global_shutdown(); // it doesn't return anything
    }
    g_libevent_active_clients -= 1;
}

static int collect_events(const struct event_base*, const struct event* event, void*  /* vector<struct event*>* */ argument){
    // event_get_events: bad naming, it retrieves the flags associated to an event.
    // The guard should protect against static & expired events
    if(event_get_events(event) & EV_TIMEOUT){
        auto vector_elements = reinterpret_cast<vector<struct event*>*>(argument);
        vector_elements->push_back(const_cast<struct event*>(event));
    }
    return 0; // 0 => keep iterating
}

vector<struct event*> libevent_pending_events(struct event_base* queue) {
    vector<struct event*> pending_events;
    int rc = event_base_foreach_event(queue, collect_events, &pending_events); // thread safe
    if(rc != 0) ERROR("event_base_foreach_event");
    return pending_events;
}



} // namespace
