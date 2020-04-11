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

#include <memory>
#include <thread>

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.

namespace teseo::internal::context {

class ThreadContext; // forward decl.

/**
 * This service asynchronously removes the caches of active transactions in the
 * existing Thread Contexts.
 */
class TcTimer {
    struct event_base* m_queue; // libevent's queue
    std::thread m_background_thread; // handle to the background thread
    bool m_eventloop_exec; // true when the service thread is running the event loop

    // An enqueued event
    struct Event {
        struct event* m_event; // pointer to the libevent's event
        std::shared_ptr<ThreadContext> m_thread_context; // the thread context to clear
    };

    // Start the service / background thread
    void start();

    // Stop the service / background thread
    void stop();

    // Method executed by the background thread, it runs the event loop
    void main_thread();

    // Notify the thread who started the service that the event loop is running
    static void callback_start(int fd, short flags, void* /* TcTimer instance */ event_argument);

    // The callback invoked by the libevent event loop, trampoline to `handle_callback'
    static void callback_invoke(int fd, short flags, void* /* Event */  event_argument);

public:
    // Constructor. It implicitly starts the service.
    TcTimer();

    // Destructor. It implicitly stops the running service.
    ~TcTimer();

    // Track a thread context to process
    void register_thread_context(std::shared_ptr<ThreadContext> thread_context);
};

} // namespace
