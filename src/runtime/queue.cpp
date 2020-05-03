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

#include <teseo/runtime/queue.hpp>
#include <cassert>
#include <future>
#include <random>

#include "teseo/context/global_context.hpp"
#include "teseo/runtime/worker.hpp"

using namespace std;

namespace teseo::runtime {

Queue::Queue(runtime::Runtime* runtime) : m_runtime(runtime) {
    start_workers();
}

Queue::~Queue(){
    stop_workers();
}

void Queue::start_workers(){
    for(int i = 0; i < NUM_WORKERS; i++){
        m_workers[i].m_worker = new Worker(this, i);
    }
}

void Queue::stop_workers(){
    for(int i = 0; i < NUM_WORKERS; i++){
        submit(Task{TaskType::TERMINATE, nullptr}, i);
        delete m_workers[i].m_worker; /* blocking call, it waits for the worker terminate */
        m_workers[i].m_worker = nullptr;
    }
}

runtime::Runtime* Queue::runtime() {
    return m_runtime;
}

void Queue::submit(Task task, int worker_id){
    assert(worker_id < NUM_WORKERS && "Invalid worker id");
    auto& winfo = m_workers[worker_id];
    winfo.m_mutex.lock();
    winfo.m_queue.append(task);
    winfo.m_mutex.unlock();
    winfo.m_condvar.notify_one();
}

void Queue::submit_all(Task task){
    for(int i = 0; i < NUM_WORKERS; i++){
        submit(task, i);
    }
}

Task Queue::fetch(int worker_id){
    assert(worker_id < NUM_WORKERS && "Invalid worker id");

    auto& winfo = m_workers[worker_id];
    auto& queue = winfo.m_queue;
    unique_lock<mutex> lock(winfo.m_mutex);
    winfo.m_condvar.wait(lock, [&queue]{ return !queue.empty(); });
    Task task = queue[0];
    queue.pop();
    return task;
}

int Queue::random_worker_id() {
    mt19937 random_generator { random_device{}() };
    return uniform_int_distribution<int>{0, NUM_WORKERS -1}(random_generator);
}

Worker* Queue::random_worker(){
    return m_workers[random_worker_id()].m_worker;
}

} // namespace

