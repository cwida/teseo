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

#include <vector>

#include "teseo/util/latch.hpp"

namespace teseo::context { class GlobalContext; } // forward declaration
namespace teseo::profiler { class EventThread; } // forward declaration

namespace teseo::gc {

class SimpleQueue; // forward declaration

/**
 * An instance of the garbage collector. Each Async/Worker thread owns an instance of
 * a garbage collector, to periodically release from memory unused objects. This
 * implementation is epoch based.
 *
 * This class is thread safe.
 */
class GarbageCollector {
    context::GlobalContext* m_global_context; // owner
    SimpleQueue* m_local; // internal queue, to provide the services of the GC even when a thread context is not available
    std::vector<SimpleQueue*> m_shared_queues; // queues shared between the GC and a single thread context
    std::vector<SimpleQueue*> m_private_queues; // queues released by the thread contexts
    util::Latch m_latch; // better safe than sorry
    profiler::EventThread* m_profiler; // record the GC usage

    // Remove all objects in the given queue
    void remove_all(SimpleQueue* queue);

    // Perform a GC pass over the given queue
    void perform_gc_pass(uint64_t epoch, SimpleQueue* queue);

public:
    /**
     * Create a new instance of the Garbage Collector
     */
    GarbageCollector(context::GlobalContext* instance);

    /**
     * Destructor
     */
    ~GarbageCollector();

    /**
     * Execute a single pass of the garbage collector
     */
    void execute();

    /**
     * Create a new queue shared between this garbage collector and a thread context
     * @param capacity the initial capacity of the queue (0 = automatic)
     */
    SimpleQueue* create_shared_queue();

    /**
     * Return to the GC (unshare) the queues used by a thread context
     */
    void unregister(SimpleQueue* local, SimpleQueue* shared);

    /**
     * Mark the given object for deletion
     */
    void mark(void* pointer, void (*deleter)(void*));

    /**
     * Reset the internal profiler
     */
    void set_profiler(profiler::EventThread* profiler);

    /**
     * Dump the addresses of the held queues to stdout, for debugging purposes
     */
    void dump() const;
};

} // namespace
