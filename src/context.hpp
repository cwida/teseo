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

class ThreadContext;
class GlobalContext;
class TransactionContext;

class GlobalContext {
    GlobalContext(const GlobalContext&) = delete;
    GlobalContext& operator=(const GlobalContext& ) = delete;

    static thread_local ThreadContext* m_thread_context;
    ThreadContext* m_tc_head; // linked list of the registered contexts
    OptimisticLatch<0> m_tc_latch; // latch for the head of registered contexts
    uint64_t m_txn_global_counter = 0; // global counter, where the startTime and commitTime for transactions are drawn
    uint64_t m_txn_quick_update = 0; // transaction ID used by the quick/short transactions (txns that perform a single update stmt)
    std::atomic<uint64_t> m_epoch = 0; // current global epoch, updated from time to time and used by the centralised garbage collector

public:

    void register_thread();

    void unregister_thread();

    uint64_t min_epoch() const;

    // Return the current epoch
    uint64_t epoch() const;


    ThreadContext* context();
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

};


class UndoLog {
    TransactionContext* m_transaction;
    ThreadContext* m_context; // pointer to the thread context this undo log belongs
    UndoLog* m_next; // pointer to the next undo log in the chain
    constexpr static uint64_t BUFFER_SZ = 264192; // 256kb
    uint64_t m_space_left = BUFFER_SZ;
    char m_buffer[BUFFER_SZ];

};

class TransactionContext {
    UndoLog m_undo_log; // first undo log in the chain
};



}
