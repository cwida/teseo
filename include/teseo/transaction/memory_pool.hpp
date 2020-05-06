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

namespace teseo::context { class GlobalContext; } // forward declaration

namespace teseo::transaction {

class TransactionImpl; // forward declaration

/**
 * To ease the pressure on the memory, every thread reuses a memory pool to allocate
 * new transactions. An entry in the memory pool actually contains both the space for
 * a transaction and a small undo buffer.
 * A memory pool belongs to a single thread context, but because the span of transaction
 * can last after it terminated, due to versions still active in the undo buffer, deallocations
 * can be invoked by different threads.
 *
 * To ensure a lock-free data structure, memory deallocations do not alter the `free list'. Rather
 * the owner of this memory pool should periodically invoke #rebuild_free_list() to find the new
 * slots available.
 */
class MemoryPool {
    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;
    MemoryPool(); // private ctor, use the static function create to allocate a new memory pool
    ~MemoryPool(); // private dtor, use the static function destroy to deallocate a memory pool

    // The free list consists of three sections: [A, B, C]
    // A are the free slots, sorted in the order order, e.g. [1, 3, 4, 8]
    // B are the slots occupied since the array was used by a worker, still sorted in ascending order, e.g. [9, 11, 14]
    // C are the slots occupied since the free list was lastly built, this time sorted in decreasing order, e.g. [13, 12, 10, 9, 7, 6, 5, 2, 0]
    // the pointers m_last refers to the end of section A, and m_sorted to the end of section B
    uint32_t m_next; // last slot allocated
    uint32_t m_sorted; // last slot sorted

    // access the memory area where transactions are stored
    uint8_t* buffer();
    uint32_t* array_free_slots();

    // merge the section B and C together, sorting in decreasing order
    void sort(uint32_t* __restrict scratchpad);

    // Remove the given transaction
    void do_destroy_transaction(uint64_t* slot_address);

public:
    // Allocate a new transaction from the memory pool. Returns nullptr if there are no more available slots in the thread pool
    TransactionImpl* create_transaction(context::GlobalContext* global_context, bool read_only = false);

    // Destroy (deallocate) the given transaction
    static void destroy_transaction(TransactionImpl* transaction);

    // Rebuild the free list
    void rebuild_free_list(uint32_t* __restrict scratchpad);

    // Check whether the memory pool is full
    bool is_full() const;

    // Check whether the memory pool is empty
    bool is_empty() const;

    // Retrieve the fill ratio of this memory pool, in [0, 1]
    double fill_factor() const;

    // The capacity of the memory pool, in terms of number of transactions
    static uint64_t capacity();

    // Get the size of each entry in the memory pool
    static uint64_t entry_size();

    // Create a new memory pool
    static MemoryPool* create();

    // Destroy the given memory pool
    static void destroy(MemoryPool* mempool);

    // Internal dump to stdout, for debugging purposes
    void dump() const;

};

} // namespace
