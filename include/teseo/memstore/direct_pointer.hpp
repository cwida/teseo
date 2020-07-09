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

#include <cstdint>
#include <ostream>
#include <string>

namespace teseo::memstore {

// forward declaration
class Context;
class Leaf;
class Segment;

/**
 * It's the value component of an element in the vertex table, representing a pointer to a position in
 * a _sparse file_ as a triple:
 * 1- leaf pointer
 * 2- segment + version (epoch)
 * 3- file position: vertex ptr, edge pointer, back pointer + misc flags
 */
class DirectPointer {
    Leaf* m_leaf; // pointer to the leaf
    uint64_t m_segment; // combination of the offset (16 bits) and version (48 bits)
    uint64_t m_filepos; // file position, as [vertex, edge, backptr, flags]

    // masks
    constexpr static uint64_t MASK_SEGMENT_VERSION = (1ull << 48) -1; // Least significant 48 bits
    constexpr static uint64_t MASK_SEGMENT_OFFSET = ~MASK_SEGMENT_VERSION;// Most significant 16 bits
    constexpr static uint64_t MASK_FILEPOS_FLAGS = (1ull << 16) -1; // Least significant 16 bits
    constexpr static uint64_t MASK_FILEPOS_VERTEX = MASK_FILEPOS_FLAGS << 48; // Most significant 16 bits
    constexpr static uint64_t MASK_FILEPOS_EDGE = MASK_FILEPOS_FLAGS << 32; // 16 bits
    constexpr static uint64_t MASK_FILEPOS_BACKPTR = MASK_FILEPOS_FLAGS << 16; // 16 bits

    // flags
    constexpr static uint16_t FLAG_HAS_FILEPOS = 0x1; // 1 iff a position in the sparse file is set

    // Retrieve the value associated to the given flag
    static bool get_flag(uint64_t field, uint16_t flag);

    // Set the given flag
    static void set_flag(uint64_t& field, uint16_t flag, int value);

public:

    /**
     * Create an empty, invalid direct pointer. This is analogous to a nullptr
     */
    DirectPointer();

    /**
     * Copy constructor
     */
    DirectPointer(const DirectPointer& ptr);

    /**
     * Initialise the pointer with a leaf and segment, but without a position in the sparse file
     */
    DirectPointer(const Context& context);

    /**
     * Initialise the pointer with a leaf, a segment and position in the sparse file
     */
    DirectPointer(const Context& context, uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr);

    /**
     * Copy assignment
     */
    DirectPointer& operator=(const DirectPointer& ptr);

    /**
     * Set the position in the sparse file
     */
    void set_filepos(uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr);

    /**
     * Retrieve the leaf's set
     */
    Leaf* leaf() const noexcept;

    /**
     * Set the leaf
     */
    void set_leaf(Leaf* leaf) noexcept;

    /**
     * Unset the leaf
     */
    void unset_leaf() noexcept;

    /**
     * Retrieve the latch's version of the segment
     */
    uint64_t get_segment_version() const noexcept;

    /**
     * Retrieve the segment ID (offset)
     */
    uint64_t get_segment_id() const noexcept;

    /**
     * Retrieve the segment set
     */
    Segment* segment() const noexcept;

    /**
     * Reset the field `segment'
     */
    void set_segment(uint64_t offset, uint64_t version) noexcept;

    /**
     * Unset the field segment
     */
    void unset_segment() noexcept;

    /**
     * Restore the position in the sparse file from the pointer
     */
    void get_filepos(uint64_t* out_pos_vertex, uint64_t* out_pos_edge, uint64_t* out_pos_backptr) const;

    /**
     * Check whether a file position is set
     */
    bool has_filepos() const noexcept;

    /**
     * Reset (unset) the file position
     */
    void unset_filepos() noexcept;

    /**
     * Unset the pointer. Similar to assigning an invalid pointer.
     */
    void unset() noexcept;

    /**
     * Get a string representation of the direct pointer, for debugging purposes
     */
    std::string to_string() const;

    /**
     * Dump the content of the direct pointer to stdout, for debugging purposes
     */
    void dump() const;
};

/**
 * Print the content of the pointer to the given output stream, for debugging purposes
 */
std::ostream& operator<<(std::ostream& out, const DirectPointer& ptr);

} // namespace
