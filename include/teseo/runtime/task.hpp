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
#include <ostream>
#include <string>

#include "teseo/memstore/context.hpp"

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
    // Rebuild the free list of the buffer pool
    BP_PASS,
    // Rebalance
    MEMSTORE_ENABLE_REBALANCE, // payload => nullptr
    MEMSTORE_DISABLE_REBALANCE, // payload => nullptr
    MEMSTORE_REBALANCE, // payload => ptr TaskRebalance
    //MEMSTORE_MERGE_LEAVES, // payload, ptr to the memstore
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
    memstore::Context m_context;
    memstore::Key m_key;

    TaskRebalance(const memstore::Context& context, const memstore::Key& key);
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
TaskRebalance::TaskRebalance(const memstore::Context& context, const memstore::Key& key) : m_context(context), m_key(key){

}

}
