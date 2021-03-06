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

#include <cinttypes>
#include <future>
#include <ostream>
#include <string>

#include "teseo/memstore/context.hpp"

namespace teseo::aux { class PartialResult; } // forward declaration

namespace teseo::runtime {

/**
 * The type of tasks that get be performed by workers
 */
enum class TaskType : uint8_t {
    NOP, // ignore
    // Thread context
    REGISTER_THREAD_CONTEXT, // payload => future
    UNREGISTER_THREAD_CONTEXT, // payload => future
    // Execute a single pass of the garbage collector
    GC_RUN, // payload => nullptr
    GC_STOP, // payload => future
    GC_TERMINATE, // payload => future
    // Rebuild the free list of the transaction pools
    TXN_MEMPOOL_PASS, // payload => nulltpr,
    // Rebalance
    MEMSTORE_ENABLE_REBALANCE, // payload => nullptr
    MEMSTORE_DISABLE_REBALANCE, // payload => nullptr
    MEMSTORE_REBALANCE, // payload => ptr TaskRebalance
    MEMSTORE_REBALANCE_SYNC, // payload => ptr SyncTaskRebalance
    //MEMSTORE_MERGE_LEAVES, // payload, ptr to the memstore
    // Auxiliary view
    AUX_PARTIAL_RESULT, // payload => ptr to TaskAuxPartialResult
    // Terminate the worker
    TERMINATE // payload => nullptr

};

/**
 * A single task sent from the master to the workers
 */
class Task {
    uint64_t m_type:8; // the type of the task
    uint64_t m_pointer:48; // the payload

public:
    // Create a dummy task
    Task();

    // Create a new task
    Task(TaskType type, void* payload);

    // Get the type of this task
    TaskType type() const;

    // Get the payload of this task
    void* payload() const;

    // Get a string representation of this task
    std::string to_string() const;
};

// Print to the stdout this task
std::ostream& operator<<(std::ostream& out, const Task& task);


struct TaskRebalance {
    memstore::Memstore* m_memstore;
    memstore::Key m_key;

    TaskRebalance(memstore::Memstore* memstore, const memstore::Key& key);
};

struct SyncTaskRebalance {
    std::promise<void>* m_producer;
    memstore::Memstore* m_memstore;
    memstore::Key m_key;

    SyncTaskRebalance(std::promise<void>* producer, memstore::Memstore* memstore, const memstore::Key& key);
};

struct TaskAuxPartialResult {
    memstore::Context m_context;
    aux::PartialResult* m_partial_result;

    TaskAuxPartialResult(const memstore::Context& context, aux::PartialResult* partial_result);
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
Task::Task() : Task(TaskType::NOP, nullptr) { }

inline
Task::Task(TaskType type, void* payload) : m_type((uint8_t) type), m_pointer(reinterpret_cast<uint64_t>(payload)){

}

inline
TaskType Task::type() const {
    return (TaskType) m_type;
}

inline
void* Task::payload() const {
    return reinterpret_cast<void*>(m_pointer);
}

inline
TaskRebalance::TaskRebalance(memstore::Memstore* memstore, const memstore::Key& key) : m_memstore(memstore), m_key(key){

}

inline
SyncTaskRebalance::SyncTaskRebalance(std::promise<void>* producer, memstore::Memstore* memstore, const memstore::Key& key) : m_producer(producer), m_memstore(memstore), m_key(key) {

}

inline
TaskAuxPartialResult::TaskAuxPartialResult(const memstore::Context& context, aux::PartialResult* partial_result) : m_context(context), m_partial_result(partial_result) {

}

} // namespace
