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

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/runtime/queue.hpp"
#include "teseo/runtime/task.hpp"
#include "teseo/runtime/timer_service.hpp"
#include "teseo/runtime/worker.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::runtime {

Runtime::Runtime(context::GlobalContext* global_context) : m_global_context(global_context), m_queue(this), m_timer_service(this) {
    // periodic maintenance task of the transaction pools
    m_queue.submit_all(Task{TaskType::TXN_MEMPOOL_PASS, nullptr});

    // start performing the GC passes
    m_queue.submit_all(Task{TaskType::GC_RUN, nullptr});
}

Runtime::~Runtime(){

    // bugfix: as we`re going to terminate the async timer first, stop sending new GC events it from the workers
    for(int i = 0; i < m_queue.num_workers(); i++){
        promise<void> producer;
        future<void> consumer = producer.get_future();
        m_queue.submit(Task{TaskType::GC_TERMINATE, &producer}, i);
        consumer.wait();
    }
}

context::GlobalContext* Runtime::global_context(){
    return m_global_context;
}

void Runtime::rebalance_first_leaf() {
    rebalance_first_leaf(m_global_context->memstore(), 0);
}

void Runtime::rebalance_first_leaf(memstore::Memstore* memstore, uint64_t segment_id) {
    context::ScopedEpoch epoch; // protect from the GC
    memstore::Leaf* leaf = memstore->index()->find(0).leaf();
    memstore::Segment* segment = leaf->get_segment(segment_id);
    segment->set_flag_rebal_requested();
    rebalance_segment_sync(memstore, segment->m_fence_key);
}

void Runtime::rebalance_segment_sync(memstore::Memstore* memstore, const memstore::Key& key){
    promise<void> producer;
    future<void> consumer = producer.get_future();
    Task task { TaskType::MEMSTORE_REBALANCE_SYNC, new SyncTaskRebalance{ &producer, memstore, key } };
    m_queue.submit(task, m_queue.random_worker_id());
    consumer.wait();
}

void Runtime::aux_partial_result(const memstore::Context& context, aux::PartialResult* partial_result){
    int worker_id = next_worker_id();
    Task task { TaskType::AUX_PARTIAL_RESULT, new TaskAuxPartialResult{ context, partial_result } };
    m_queue.submit(task, worker_id);
}

void Runtime::schedule_rebalance(memstore::Memstore* memstore, const memstore::Key& key){
    Task task { TaskType::MEMSTORE_REBALANCE, new TaskRebalance{ memstore, key } };
    m_timer_service.schedule_task(task, /* anyone = */ -1, context::StaticConfiguration::runtime_delay_rebalance);
}

void Runtime::schedule_rebalance(const memstore::Context& context, const memstore::Key& key){
    schedule_rebalance(context.m_tree, key);
}

void Runtime::schedule_gc_pass(int worker_id){
    Task task { TaskType::GC_RUN, nullptr };
    m_timer_service.schedule_task(task, worker_id, context::StaticConfiguration::runtime_gc_frequency);
}

void Runtime::schedule_bp_pass() {
    Task task { TaskType::BP_PASS, nullptr };
    m_timer_service.schedule_task(task, /* anyone = */ -1, context::StaticConfiguration::runtime_bp_frequency);
}

void Runtime::schedule_txnpool_pass(int worker_id){
    Task task { TaskType::TXN_MEMPOOL_PASS, nullptr };
    m_timer_service.schedule_task(task, worker_id, context::StaticConfiguration::runtime_txnpool_frequency);
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

void Runtime::execute_sync(TaskType task_type){
    vector<promise<void>> producers;
    producers.reserve(m_queue.num_workers());
    vector<future<void>> consumers;
    consumers.reserve(m_queue.num_workers());

    for(int i = 0; i < m_queue.num_workers(); i++){
        producers.emplace_back();
        consumers.emplace_back(producers.back().get_future());
        auto ptr_producer = &(producers.back());
        m_queue.submit(Task{task_type, ptr_producer}, i);
    }

    for(auto& c : consumers){ c.get(); }
}

gc::GarbageCollector* Runtime::gc() {
    return m_queue.random_worker()->gc();
}

gc::GarbageCollector* Runtime::next_gc(){
    uint64_t next = m_gc_next_counter ++;
    uint64_t num_workers = m_queue.num_workers();
    return m_queue.get_worker(next % num_workers)->gc();
}

int Runtime::next_worker_id(){
    uint64_t next = m_rr_next_counter ++;
    uint64_t num_workers = m_queue.num_workers();
    return next % num_workers;
}

transaction::MemoryPoolList* Runtime::transaction_pool(){
    return m_queue.random_worker()->transaction_pool();
}

transaction::MemoryPoolList* Runtime::transaction_pool(int worker_id){
    return m_queue.get_worker(worker_id)->transaction_pool();
}

void Runtime::register_thread_contexts(){
    execute_sync(TaskType::REGISTER_THREAD_CONTEXT);
}

void Runtime::unregister_thread_contexts(){
    // temporarily stop the GC
    execute_sync(TaskType::GC_STOP);

    // terminate the TC
    execute_sync(TaskType::UNREGISTER_THREAD_CONTEXT);

    // resume the GC at full throttle to deallocate the unregistered thread contexts
    m_queue.submit_all(Task{TaskType::GC_RUN, nullptr});
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
    case TaskType::AUX_PARTIAL_RESULT: {
        delete reinterpret_cast<TaskAuxPartialResult*>(task.payload());
    } break;
    case TaskType::MEMSTORE_REBALANCE:{
        delete reinterpret_cast<TaskRebalance*>(task.payload());
    } break;
    case TaskType::MEMSTORE_REBALANCE_SYNC: {
        delete reinterpret_cast<SyncTaskRebalance*>(task.payload());
    } break;
    default:
        ; /* nop */
    }
}

void Runtime::stop_timer(){
    m_timer_service.stop();
}

} // namespace
