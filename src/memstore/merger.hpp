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
#include <cinttypes>
#include <thread>

#include "sparse_array.hpp"

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.

namespace teseo::internal::memstore {

class RebalancerScratchPad; // forward decl.

/**
 * This class traverses the chunks in a sparse array, pruning obsolete records
 * and merging together consecutive chunks, where possible.
 */
class Merger {
    SparseArray* const m_sparse_array; // ptr to the Sparse Array instance
    RebalancerScratchPad* m_scratchpad; // working area, used to merge chunks together

    /**
     * Visit the current chunk, prune all the old records and return and estimate of the
     * number of slots in use
     */
    uint64_t visit_and_prune(SparseArray::Chunk* chunk);

    /**
     * Acquire a writer lock to the given gate
     */
    void xlock(Gate* gate);

    /**
     * Release the writer lock to the given gate
     */
    void xunlock(Gate* gate);

    /**
     * Acquire a merge lock for the whole chunk. Return the actual used space in the chunk.
     */
    uint64_t xlock(SparseArray::Chunk* chunk);

    /**
     * Release the merge lock for the given chunk
     */
    void xunlock(SparseArray::Chunk* chunk, bool invalidate = false);

    /**
     * Merge the content of `current' into `previous' and return the amount of used space
     */
    uint64_t merge(SparseArray::Chunk* previous, SparseArray::Chunk* current);

public:
    Merger(SparseArray* sparse_array);

    /**
     * Destructor
     */
    ~Merger();

    void execute();
};

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
    SparseArray* const m_sparse_array; // the attached sparse array instance
    const std::chrono::milliseconds m_time_interval; // how often to invoke the service

    // Method executed by the background thread, it runs the event loop
    void main_thread();

    // Notify the thread who started the service that the event loop is running
    static void callback_start(int fd, short flags, void* /* MergerService instance */ event_argument);

    // Trampoline to invoke the actual Merger routine
    static void callback_execute(int fd, short flags, void* /* MergerCallbackData */ event_argument);

public:
    /**
     * Create a new instance of the service
     * @param instance pointer to the related sparse array instance
     * @param interval the time interval defining how often the service should be executed
     */
    MergerService(SparseArray* sparse_array, std::chrono::milliseconds interval);

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
