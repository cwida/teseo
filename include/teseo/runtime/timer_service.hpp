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

#include <chrono>
#include <memory>
#include <thread>

#include "teseo/gc/tc_queue.hpp"
#include "teseo/runtime/task.hpp"

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.

namespace teseo::context { class GlobalContext; } // forward decl.
namespace teseo::context { class ThreadContext; } // forward decl.
namespace teseo::memstore { class Memstore; } // forward decl.
namespace teseo::runtime { class Runtime; } // forward decl.

namespace teseo::runtime {

class Runtime; // forward declaration

class TimerService {
    struct event_base* m_queue; // libevent's queue
    runtime::Runtime* const m_runtime; // owner of this instance
    std::thread m_background_thread; // handle to the background thread
    bool m_eventloop_exec; // true when the service thread is running the event loop
    gc::TcQueue* m_gc_queue; // internal GC queue

    // Start the service / background thread
    void start();


    // Event loop
    void main_thread();

    // Notify the thread who started the service that the event loop is running
    static void callback_start(int fd, short flags, void* /* TimerService instance */ event_argument);

    // Callback to reset the current cached transaction list inside a thread context
    static void callback_active_transactions(int fd, short flags, void* /* ActiveTransactionsEvent */  event_argument);
    struct EventActiveTransactions { struct event* m_event; gc::TcQueue* m_gc; context::ThreadContext* m_thread_context; };

    // Forward the event to the runtime
    static void callback_runtime(int fd, short flags, void* /* RuntimeEvent */ event_argument);
    struct EventRuntime { struct event* m_event; runtime::Runtime* m_runtime; Task m_task; int m_worker_id; };

    // Remove the pending events still in the queue
    void remove_pending_events();

public:
    // Constructor. It implicitly starts the service.
    TimerService(runtime::Runtime* runtime);

    // Destructor. It implicitly stops the running service.
    ~TimerService();

    // Stop the service / background thread
    void stop();

    // Request to asynchronously delete the cache of active transactions in the current thread context
    void refresh_active_transactions();

    // Request to schedule a rebalance for the given segment
    void schedule_task(Task task, int worker_id, std::chrono::milliseconds when);
};

} // namespace
