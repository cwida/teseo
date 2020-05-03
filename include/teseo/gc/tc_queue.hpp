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

#include "teseo/util/latch.hpp"

namespace teseo::gc {

class GarbageCollector; // forward declaration
class SimpleQueue; // forward declaration

/**
 * The queue shared by a thread context a GC, used to collect the objects that need to be
 * deleted by the garbage collector.
 */
class TcQueue {
    TcQueue(const TcQueue&) = delete;
    TcQueue& operator=(const TcQueue&) = delete;

    SimpleQueue* m_local; // private queue
    SimpleQueue* m_shared; // queue shared with the garbage collector
    GarbageCollector* m_gc; // the garbage collector owner of the queue m_shared
#if !defined(NDEBUG)
    const int m_thread_id; // the ID of the thread owning this queue
#endif

public:
    /**
     * Create a new thread context queue
     */
    TcQueue(GarbageCollector* gc);

    /**
     * Destructor
     */
    ~TcQueue();

    /**
     * Insert a new element to delete in the queue.
     * This method is not thread safe.
     */
    void mark(void* pointer, void (*deleter)(void*));

    /**
     * Release the internal queues. No new objects can be marked for GC afterwards.
     */
    void release();
};


} // namespace

