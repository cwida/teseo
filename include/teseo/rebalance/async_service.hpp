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

#pragma once

#include <condition_variable>
#include <thread>
#include <vector>

#include "teseo/memstore/context.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/util/circular_array.hpp"
#include "teseo/util/latch.hpp"

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.

namespace teseo::context { class GlobalContext; } // forward declaration

namespace teseo::rebalance {

/**
 * A bunch of background threads performing rebalances of the memstore upon request of writers
 */
class AsyncService {
    AsyncService(const AsyncService&) = delete;
    AsyncService& operator= (const AsyncService&) = delete;

    // An enqueued request for the workers
    struct Request {
        AsyncService* m_instance;
        memstore::Context m_context;
        memstore::Key m_key;

        Request(AsyncService* instance, const memstore::Context& context, const memstore::Key& key);
    };

    context::GlobalContext* const m_global_context; // the owner of this service
    struct event_base* m_queue; // libevent's queue
    util::SpinLock m_mutex; // synchronisation with the background threads
    std::condition_variable_any m_condvar; // synchronisation with the background thread
    util::CircularArray<Request*> m_requests; // the sequence of requests still to handle
    std::thread m_timer; // handle to the timer thread
    bool m_eventloop_exec; // true when the master/timer thread is running the event loop
    std::vector<std::thread> m_workers; // handle to the worker threads

    // Helper, set the name of this thread, for debugging purposes
    void set_thread_name(int thread_id);

    // Event loop for the master thread
    void master_thread();

    // Event loop for the worker threads
    void worker_thread(int thread_id);

    // Forward the request to one of the masters
    void handle_master_request(Request* request);

    // Notify the thread who started the service that the event loop is running
    static void callback_master_start(int fd, short flags, void* /* AsyncService instance */ event_argument);

    // Trampoline for libevent
    static void callback_master_request(int fd, short flags, void* /* Event */  event_argument);

    // Rebalance the gate identified by the given key
    void handle_worker_request(Request& request);

public:
    // Create the service
    AsyncService(context::GlobalContext* global_context);

    // Destructor
    ~AsyncService();

    // Start the service
    void start();

    // Stop the service
    void stop();

    // Request to asynchronously rebalance the segment identified by the given context
    void request(const memstore::Context& context);
};

}

