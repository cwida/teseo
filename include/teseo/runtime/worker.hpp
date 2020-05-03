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

#include <thread>


namespace teseo::gc { class GarbageCollector; }

namespace teseo::runtime {

class Queue; // forward declaration

/**
 * A background thread performing various maintenance tasks asynchronously, upon request
 * of the Master and the rest of the system.
 */
class Worker {
    Worker(Worker&) = delete;
    Worker& operator= (const Worker&) = delete;
    Queue* m_worker_pool; // the master service
    const int m_id; // the worker id
    gc::GarbageCollector* m_gc; // every worker has its own garbage collector!
    std::thread m_thread; // handle to the background thread

    // Helper, set the name of this thread, for debugging purposes
    static void set_thread_name(int worker_id);

    // Event loop for the background thread
    void main_thread();

public:
    // Create & start a worker
    Worker(Queue* pool, int worker_id);

    // Destructor, wait for the background thread to terminate
    ~Worker();

    // Handle to the garbage collector
    gc::GarbageCollector* gc();

    // Get the worker ID
    int worker_id() const;
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
gc::GarbageCollector* Worker::gc() {
    return m_gc;
}

}
