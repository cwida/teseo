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
#include <ostream>
#include <string>

#include "teseo/memstore/context.hpp"
#include "teseo/memstore/direct_pointer.hpp"
#include "teseo/memstore/key.hpp"

namespace teseo::memstore {

/**
 * This is the saved state of an iterator. It can be eventually reloaded to resume the
 * scan from its last saved position.
 *
 * This instance can only be used with regular (non optimistic) readers on sparse files. When a reader
 * saves its state, it doesn't release the held latch to the segment. The latch must be eventually
 * released by invoking the method #close() on this class.
 */
class CursorState {
    Key m_key; // The next key to read in the cursor
    DirectPointer m_position; // The last position of the cursor

public:
    /**
     * Create a new (empty) instance
     */
    CursorState();

    /**
     * Destroy the instance
     */
    ~CursorState();

    /**
     * Invalidate the state, but do not release the held latch
     */
    void invalidate() noexcept;

    /**
     * Invalidate the state and the release the held latch
     */
    void close() noexcept;

    /**
     * Retrieve the key associated to this cursor
     */
    Key& key();
    const Key& key() const;


    /**
     * Check if the current instance is still valid, that is, it has not been invalidated or closed.
     */
    bool is_valid() const;

    /**
     * Retrieve the direct pointer associated to this cursor
     */
    DirectPointer& position();
    const DirectPointer& position() const;

    /**
     * Retrieve a string representation of the state, for debugging purposes
     */
    std::string to_string() const;

    /**
     * Dump the content of the cursor to the output stream, for debugging purposes
     */
    void dump() const;
};

/**
 * Print a string representation of the cursor.
 */
std::ostream& operator<<(std::ostream& out, const CursorState& bookmark);


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
Key& CursorState::key() {
    return m_key;
}

inline
const Key& CursorState::key() const {
    return m_key;
}

inline
DirectPointer& CursorState::position() {
    return m_position;
}

inline
const DirectPointer& CursorState::position() const {
    return m_position;
}

inline
bool CursorState::is_valid() const {
    return m_key != KEY_MIN;
}

} // namespace
