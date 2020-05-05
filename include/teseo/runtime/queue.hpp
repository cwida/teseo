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

#include "task.hpp"

#include <condition_variable>
#include <mutex>

#include "teseo/context/static_configuration.hpp"
#include "teseo/runtime/task.hpp"
#include "teseo/util/circular_array.hpp"

// forward declarations
namespace teseo::context { class GlobalContext; }
namespace teseo::gc { class GarbageCollector; }
namespace teseo::runtime { class Runtime; }
namespace teseo::runtime { class Worker; }

namespace teseo::runtime {

class Queue {
    const int m_num_workers; // total number of workers
    struct WState {
        Worker* m_worker;
        std::mutex m_mutex;
        std::condition_variable m_condvar;
        util::CircularArray<Task> m_queue;
    };
    WState* m_workers; // The pointer & the queue associated to each worker
    runtime::Runtime* const m_runtime; // Pointer to the owner of this instance

    // Start all the workers
    void start_workers();

    // Send a request to terminate all workers, wait for their threads to terminate
    void stop_workers();

public:
    // Constructor, start the workers
    Queue(runtime::Runtime* runtime);

    // Destructor, terminate the workers
    ~Queue();

    // Submit the task to a specific worker
    void submit(Task task, int worker_id);

    // Submit the given task to all workers
    void submit_all(Task task);

    // Fetch the runtime instance associated to this set of queues
    runtime::Runtime* runtime();

    // Retrieve one of the workers randomly
    int random_worker_id();

    // Retrieve a random worker
    Worker* random_worker();

    // Retrieve the worker associated to the given ID
    Worker* get_worker(int worker_id);

    // Total number of workers
    int num_workers() const;

    // Fetch the next task to process. This method is invoked by workers.
    Task fetch(int worker_id);
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
int Queue::num_workers() const {
    return m_num_workers;
}

} // namespace
