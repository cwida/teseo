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
#include "teseo/memstore/key.hpp"

namespace teseo::memstore {

/**
 * This is the saved state of an iterator. It can be eventually reloaded to resume the
 * scan from its last saved position.
 *
 * This istance can only be used with regular (non optimistic) readers on sparse files. When a reader
 * saves its state, it doesn't release the held latch to the segment. The latch must be eventually
 * released by invoking the method #close() on this class.
 */
class CursorState {
    Context m_context; // Path to the memstore - leaf - segment, together with the latch being held
    Key m_key; // The next key to read in the cursor
    uint64_t m_pos_vertex; // The saved position in the cursor for the vertex
    uint64_t m_pos_edge; // The saved position in the cursor for the edge
    uint64_t m_pos_backptr; // The saved position in the cursor for the backptr (the MVCC version)

public:
    /**
     * Create a new (empty) instance
     */
    CursorState(const Context& context);

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
     * Retrieve the context associated
     */
    Context context() const;

    /**
     * Retrieve the key associated to this cursor
     */
    Key key() const;

    /**
     * Retrieve the vertex/edge/backptr position in the cursor
     */
    uint64_t pos_vertex() const;
    uint64_t pos_edge() const;
    uint64_t pos_backptr() const;

    /**
     * Check if the current instance is still valid, that is, it has not been invalidated or closed.
     */
    bool is_valid() const;

    /**
     * Save the state of the cursor with an exact position in a sparse file.
     */
    void save(const Context& context, Key key, uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr) noexcept;

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
Context CursorState::context() const {
    return m_context;
}

inline
Key CursorState::key() const {
    return m_key;
}

inline
uint64_t CursorState::pos_vertex() const {
    return m_pos_vertex;
}

inline
uint64_t CursorState::pos_edge() const {
    return m_pos_edge;
}

inline
uint64_t CursorState::pos_backptr() const {
    return m_pos_backptr;
}

inline
bool CursorState::is_valid() const {
    return m_key != KEY_MIN;
}

} // namespace
