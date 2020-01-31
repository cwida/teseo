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
#include <memory>

#include "latch.hpp"

namespace teseo::internal {

// forward declarations
class GarbageCollector;
class GlobalContext;
class ThreadContext;
class TransactionContext;

class GlobalContext {
    GlobalContext(const GlobalContext&) = delete;
    GlobalContext& operator=(const GlobalContext& ) = delete;

    ThreadContext* m_tc_head {nullptr}; // linked list of the registered contexts
    OptimisticLatch<0> m_tc_latch; // latch for the head of registered contexts
    uint64_t m_txn_global_counter = 0; // global counter, where the startTime and commitTime for transactions are drawn
    uint64_t m_txn_quick_update = 0; // transaction ID used by the quick/short transactions (txns that perform a single update stmt)
    GarbageCollector* m_garbage_collector { nullptr }; // centralised garbage collector

public:

    GlobalContext(); // ctor

    /**
     * Destructor
     */
    ~GlobalContext();

    void register_thread();

    void unregister_thread();

    uint64_t min_epoch() const;

    /**
     * Retrieve the current global context
     */
    static GlobalContext* context();

    /**
     * Instance to the garbage collector
     */
    GarbageCollector* gc() const noexcept;
};

class ThreadContext {
    friend class GlobalContext;

    GlobalContext* m_global_context;
    uint64_t m_epoch;
    OptimisticLatch<0> m_latch;
    ThreadContext* m_next; // next thread context in the chain
    TransactionContext* m_transaction;


public:
    ThreadContext(GlobalContext* global_context);

    void epoch_enter();


    void epoch_exit();


    uint64_t epoch() const;


    /**
     * Retrieve the global context associated to the given local context
     */
    GlobalContext* global_context() const noexcept;

    /**
     * Retrieve the current local context
     */
    static ThreadContext* context();
};


class UndoLog {
    TransactionContext* m_transaction;
    ThreadContext* m_context; // pointer to the thread context this undo log belongs
    UndoLog* m_next; // pointer to the next undo log in the chain
    constexpr static uint64_t BUFFER_SZ = 264192; // 256kb
    uint64_t m_space_left = BUFFER_SZ;
    char m_buffer[BUFFER_SZ];

};


enum class UndoType : uint32_t {
    VERTEX_COUNT
};

class UndoEntry {
    TransactionContext* m_transaction;
    UndoEntry* m_next; // linked list of undo entries
    UndoType m_type; // the type of entry
    uint32_t m_length; // the length of the payload, excluding the length of the class UndoEntry

public:
    TransactionContext* transaction();
    uint64_t transaction_id();

    static void set_entry_vertex_count(UndoEntry** pointer, uint64_t previous_value);
};

class TransactionContext {
    UndoLog m_undo_log; // first undo log in the chain
};





}
