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

namespace teseo::context {

class GlobalContext; // forward decl.
class ThreadContext; // forward decl.

/**
 * The list of all thread contexts registered within a global context
 */
class TcList {
    friend class GlobalContext;
    TcList(const TcList&) = delete;
    TcList& operator=(const TcList&) = delete;

    GlobalContext* const m_global_context; // owner of this list
    mutable util::OptimisticLatch<0> m_latch; // provide thread safety
    ThreadContext** m_list; // the actual list of elements
    volatile uint32_t m_size; // number of elements in the list
    uint32_t m_capacity; // number of slots in the list

    // Resize the list to a new capacity
    void resize();

    // The current capacity of the list
    uint32_t capacity() const;

public:
    // Constructor
    TcList(GlobalContext* global_context);

    // Destructor
    ~TcList();

    // Register a new thread context in the list
    void insert(ThreadContext* thread_context);

    // Remove the given thread context from the list
    void remove(ThreadContext* thread_context);

    // Retrieve the current list
    ThreadContext** list() const;

    // Check whether the list is empty
    bool empty() const;

    // Read & validate the internal optimistic latch
    uint64_t read_version() const;
    void validate_version(uint64_t version) const;
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
ThreadContext** TcList::list() const {
    return m_list;
}

inline
uint64_t TcList::read_version() const {
    return m_latch.read_version();
}

inline
void TcList::validate_version(uint64_t version) const {
    m_latch.validate_version(version);
}

inline
bool TcList::empty() const {
    return m_size == 0;
}


} // namespace
