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
#include "teseo/runtime/runtime.hpp"

#include <future>

#include "teseo/runtime/queue.hpp"
#include "teseo/runtime/task.hpp"
#include "teseo/runtime/timer_service.hpp"
#include "teseo/runtime/worker.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::runtime {

Runtime::Runtime(context::GlobalContext* global_context) : m_global_context(global_context), m_queue(this), m_timer_service(this) {
    // start performing the GC passes
    m_queue.submit_all(Task{TaskType::GC_RUN, nullptr});
}

Runtime::~Runtime(){

    // bugfix: as we`re going to terminate the async timer first, stop sending new GC events it from the workers
    for(int i = 0; i < m_queue.num_workers(); i++){
        promise<void> producer;
        future<void> consumer = producer.get_future();
        m_queue.submit(Task{TaskType::GC_STOP, &producer}, i);
        consumer.wait();
    }
}

context::GlobalContext* Runtime::global_context(){
    return m_global_context;
}

void Runtime::schedule_rebalance(const memstore::Context& context, const memstore::Key& key){
    Task task { TaskType::MEMSTORE_REBALANCE, new TaskRebalance{ context, key } };
    m_timer_service.schedule_task(task, /* anyone = */ -1, context::StaticConfiguration::runtime_delay_rebalance);
}

void Runtime::schedule_gc_pass(int worker_id){
    Task task { TaskType::GC_RUN, nullptr };
    m_timer_service.schedule_task(task, worker_id, context::StaticConfiguration::runtime_gc_frequency);
}

void Runtime::schedule_reset_active_transactions(){
    m_timer_service.refresh_active_transactions();
}

void Runtime::execute(Task task, int worker_id){
    if(worker_id < 0){ // pick a random worker
        m_queue.submit(task, m_queue.random_worker_id());
    } else {
        m_queue.submit(task, worker_id);
    }
}

gc::GarbageCollector* Runtime::gc() {
    return m_queue.random_worker()->gc();
}

void Runtime::register_thread_contexts(){
    for(int i = 0; i < m_queue.num_workers(); i++){
        promise<void> producer;
        future<void> consumer = producer.get_future();
        m_queue.submit(Task{TaskType::REGISTER_THREAD_CONTEXT, &producer}, i);
        consumer.wait();
    }
}

void Runtime::unregister_thread_contexts(){
    for(int i = 0; i < m_queue.num_workers(); i++){
        promise<void> producer;
        future<void> consumer = producer.get_future();
        m_queue.submit(Task{TaskType::UNREGISTER_THREAD_CONTEXT, &producer}, i);
        consumer.wait();
    }
}

void Runtime::enable_rebalance(){
    Task task{TaskType::MEMSTORE_ENABLE_REBALANCE, nullptr};
    m_queue.submit_all(task);
}

void Runtime::disable_rebalance(){
    Task task{TaskType::MEMSTORE_DISABLE_REBALANCE, nullptr};
    m_queue.submit_all(task);
}

void Runtime::delete_task(Task task){
    switch(task.type()){
    case TaskType::MEMSTORE_REBALANCE:{
        delete reinterpret_cast<TaskRebalance*>(task.payload());
    } break;
    default:
        ; /* nop */

    }
}

} // namespace
