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

namespace teseo::internal::context {

class GlobalContext; // forward decl.
class Transaction; // forward decl.

class ThreadContext {
    friend class GlobalContext;

    GlobalContext* m_global_context; // pointer to the instance of the database
    uint64_t m_epoch; // current epoch of the thread
    OptimisticLatch<0> m_latch; // latch, used to manage the linked list of thread contexts
    ThreadContext* m_next; // next thread context in the chain

#if !defined(NDEBUG) // thread contexts are always associated to a single logical thread, keep thrack of its ID for debugging purposes
    const int64_t m_thread_id;
#endif


public:
    /**
     * Create a new thread context, associated to the given database instance
     */
    ThreadContext(GlobalContext* global_context);

    /**
     * Enter a new epoch in the current context
     */
    void epoch_enter();

    /**
     * Exit the epoch in the current context
     */
    void epoch_exit();

    /**
     * Retrieve the current epoch for this context
     */
    uint64_t epoch() const;

    /**
     * Retrieve the global context associated to the given local context
     */
    GlobalContext* global_context() noexcept;
    const GlobalContext* global_context() const noexcept;

    /**
     * Dump the content of this context to stdout, for debugging purposes
     */
    void dump() const;
};

/**
 * Retrieve the current thread context
 */
ThreadContext* thread_context();

} // namespace
