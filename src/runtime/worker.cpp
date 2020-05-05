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
#include "teseo/runtime/worker.hpp"

#include <future>
#include <iostream>
#include <string>

#include "teseo/context/global_context.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/rebalance/merge_operator.hpp"
#include "teseo/rebalance/rebalance.hpp"
#include "teseo/runtime/queue.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/runtime/task.hpp"
#include "teseo/transaction/memory_pool_list.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::runtime {

Worker::Worker(Queue* master, int worker_id) : m_worker_pool(master), m_id(worker_id),
        m_transaction_pool(new transaction::MemoryPoolList()),
        m_gc(new gc::GarbageCollector(m_worker_pool->runtime()->global_context())){
    // start the background thread
    m_thread = thread(&Worker::main_thread, this);
}

Worker::~Worker(){
    m_thread.join();
    delete m_gc; m_gc = nullptr;
    delete m_transaction_pool; m_transaction_pool = nullptr;
}

void Worker::main_thread(){
    COUT_DEBUG("Worker #" << worker_id() << " started");
    set_thread_name(worker_id());

    // event loop
    bool rebal_enabled = false;
    bool gc_enabled = true;
    bool terminate = false;
    while(!terminate){
        Task task = m_worker_pool->fetch(worker_id());
        switch(task.type()){
        case TaskType::NOP: {
            assert(0 && "Nop");
        } break;
        case TaskType::REGISTER_THREAD_CONTEXT: {
            auto producer = reinterpret_cast<promise<void>*>(task.payload());
            m_worker_pool->runtime()->global_context()->register_thread();
            m_gc->set_profiler(context::thread_context()->profiler_events());
            producer->set_value();
            rebal_enabled = true;
        } break;
        case TaskType::UNREGISTER_THREAD_CONTEXT: {
            m_gc->set_profiler(nullptr);
            auto producer = reinterpret_cast<promise<void>*>(task.payload());
            m_worker_pool->runtime()->global_context()->unregister_thread();
            producer->set_value();
            rebal_enabled = false;
        } break;
        case TaskType::GC_RUN: {
            if(gc_enabled){
                m_gc->execute();
                m_worker_pool->runtime()->schedule_gc_pass(worker_id());
            }
        } break;
        case TaskType::GC_TERMINATE:
            delete m_gc; m_gc = nullptr; // fall through
        case TaskType::GC_STOP: {
            auto producer = reinterpret_cast<promise<void>*>(task.payload());
            producer->set_value();
            gc_enabled = false;
        } break;
        case TaskType::TXN_MEMPOOL_PASS: {
            transaction_pool()->cleanup();
            m_worker_pool->runtime()->schedule_txnpool_pass(worker_id());
        } break;
        case TaskType::MEMSTORE_ENABLE_REBALANCE: {
            rebal_enabled = true;
        } break;
        case TaskType::MEMSTORE_DISABLE_REBALANCE: {
            rebal_enabled = false;
        } break;
        case TaskType::MEMSTORE_REBALANCE: {
            auto task_rebal = reinterpret_cast<TaskRebalance*>(task.payload());
            if(rebal_enabled){
                rebalance::handle_rebalance(task_rebal->m_context, task_rebal->m_key);
            } else {
                COUT_DEBUG("Request ignored, context: " << task_rebal->m_context << ", key: " << task_rebal->m_key);
            }
        } break;
//        case TaskType::MEMSTORE_MERGE_LEAVES: {
//            if(rebal_enabled){
//                auto memstore = reinterpret_cast<memstore::Memstore*>(task.payload());
//                rebalance::handle_merge(memstore);
//            } else {
//                COUT_DEBUG("Request ignored, merge leaves");
//            }
//        } break;
        case TaskType::TERMINATE: {
            terminate = true;
        } break;
        }

        Runtime::delete_task(task);
    }


    COUT_DEBUG("Worker #" << worker_id() << " stopped");
}

int Worker::worker_id() const{
    return m_id;
}

void Worker::set_thread_name(int worker_id){
    string thread_name = string("Teseo.Worker #") + to_string(worker_id);
    util::Thread::set_name(thread_name);
}

} // namespace

