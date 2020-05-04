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

#include "teseo/util/circular_array.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::profiler { class EventThread; } // forward declaration

namespace teseo::transaction {

class MemoryPool; // forward declaration

/**
 * A cache to reuse "almost empty" memory pools to create new transactions.
 *
 * This class is thread safe.
 */
class MemoryPoolList {
    MemoryPoolList(const MemoryPoolList&) = delete;
    MemoryPoolList& operator=(const MemoryPoolList&) = delete;

    util::SpinLock m_latch; // to ensure thread safety
    util::CircularArray<MemoryPool*> m_ready; // memory pools that are ready to be reused
    util::CircularArray<MemoryPool*> m_idle; // memory pools that are still filled
    profiler::EventThread* m_profiler; // internal profiler

    // Helper
    void dump_queue(const char* name, const util::CircularArray<MemoryPool*>& queue) const;

public:
    /**
     * Init the object
     */
    MemoryPoolList();

    /**
     * Destructor
     */
    ~MemoryPoolList();

    /**
     * Acquire a new memory pool
     */
    MemoryPool* acquire();

    /**
     * Release the `old' memory pool, and acquire a new one
     */
    MemoryPool* exchange(MemoryPool* old);

    /**
     * Release the given memory pool
     */
    void release(MemoryPool* mempool);

    /**
     * Remove from the cache the memory pools that are completely empty
     */
    void cleanup();

    /**
     * Reset the internal profiler
     */
    void set_profiler(profiler::EventThread* profiler);

    /**
     * Dump the content of this class, for debugging purposes
     */
    void dump() const;
};

} // namespace
