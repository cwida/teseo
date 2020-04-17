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

namespace teseo::transaction {

class MemoryPool; // forward decl.

/**
 * Memory space for multiple undo records
 */
class UndoBuffer {
    UndoBuffer(const UndoBuffer&) = delete;
    UndoBuffer& operator=(const UndoBuffer&) = delete;
    friend class MemoryPool;

    // Init the buffer
    UndoBuffer(uint64_t total_space);

    // Destructor, limit the visibility
    ~UndoBuffer();

public:
    uint32_t m_space_left; // amount of space left in the buffer, in bytes
    const uint32_t m_space_total; // total amount of space in the buffer, in bytes
    UndoBuffer* m_next {nullptr}; // pointer to the next undo log in the chain

    // Access the underlying buffer
    const uint8_t* buffer() const;
    uint8_t* buffer();

    // Retrieve how much space is occupied by an UndoBuffer of the given size
    static uint64_t undobuffer_sz(uint32_t buffer_sz);

    // Create a new buffer of the given size
    static UndoBuffer* allocate(uint32_t buffer_sz);

    // Deallocate the given buffer
    static void deallocate(UndoBuffer* undobuffer);
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
UndoBuffer::UndoBuffer(uint64_t total_space) : m_space_left(total_space), m_space_total(total_space) { }

inline
UndoBuffer::~UndoBuffer(){ }

inline
const uint8_t* UndoBuffer::buffer() const{
    return reinterpret_cast<const uint8_t*>(this +1);
}

inline
uint8_t* UndoBuffer::buffer() {
    return reinterpret_cast<uint8_t*>(this +1);
}

inline
uint64_t UndoBuffer::undobuffer_sz(uint32_t buffer_sz){
    return sizeof(UndoBuffer) + buffer_sz;
}

} // namespace
