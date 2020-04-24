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
#include <thread>

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.
namespace teseo::memstore { class Memstore; } // forward declaration

namespace teseo::rebalance {

/**
 * This is a service attached to a single sparse array. It periodically runs
 * on a background thread. It prunes obsolete version/undo records from the
 * sparse array and, where possible, merges two smaller chunks into a single
 * large chunk.
 */
class MergerService {
    struct event_base* m_queue; // libevent's queue
    std::thread m_background_thread; // handle to the background thread
    bool m_eventloop_exec; // true when the service thread is running the event loop
    memstore::Memstore* const m_memstore; // the attached sparse array instance

    // Method executed by the background thread, it runs the event loop
    void main_thread();

    // Notify the thread who started the service that the event loop is running
    static void callback_start(int fd, short flags, void* /* MergerService instance */ event_argument);

    // Trampoline to invoke the actual Merger routine
    static void callback_execute(int fd, short flags, void* /* MergerCallbackData */ event_argument);

public:
    /**
     * Create a new instance of the service
     * @param owner pointer to the related Memstore instance
     */
    MergerService(memstore::Memstore* owner);

    /**
     * Destructor
     */
    ~MergerService();

    /**
     * Start the service
     */
    void start();

    /**
     * Stop the service
     */
    void stop();

    /**
     * Invoke the service synchronously, for debugging purposes
     */
    void execute_now();
};

} // namespace

