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
class DirectPointer;
class Leaf;
class Segment;
class VertexTable;

/**
 * A compressed representation of a direct pointer, stored in the vertex table
 */
class CompressedDirectPointer {
    friend class DirectPointer;
    friend class VertexTable;

    unsigned __int128 m_scalar; // the wrapped & compressed value
};

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
    constexpr static uint64_t MASK_SEGMENT_OFFSET = ~MASK_SEGMENT_VERSION; // Most significant 16 bits
    constexpr static uint64_t MASK_FILEPOS_FLAGS = (1ull << 16) -1; // Least significant 16 bits
    constexpr static uint64_t MASK_FILEPOS_VERTEX = MASK_FILEPOS_FLAGS << 48; // Most significant 16 bits
    constexpr static uint64_t MASK_FILEPOS_EDGE = MASK_FILEPOS_FLAGS << 32; // 16 bits
    constexpr static uint64_t MASK_FILEPOS_BACKPTR = MASK_FILEPOS_FLAGS << 16; // 16 bits

    // flags
    constexpr static uint16_t FLAG_HAS_FILEPOS = 0x1; // 1 iff a position in the sparse file is set
    constexpr static uint16_t FLAG_LATCH_HELD = 0x2; //1 iff the thread context is holding a reader latch (only used by a cursor state)

    // compressed representation of the direct pointer
    constexpr static unsigned __int128 MASK_COMPRESS_LEAF = ((static_cast<unsigned __int128>(1) << 45) -1) << 83; // MSB 45 bits
    constexpr static unsigned __int128 MASK_COMPRESS_SEGMENT = ((static_cast<unsigned __int128>(1) << 12) -1) << 71; // 12 bits
    constexpr static unsigned __int128 MASK_COMPRESS_VERSION = ((static_cast<unsigned __int128>(1) << 48) -1) << 23; // 48 bits
    constexpr static unsigned __int128 MASK_COMPRESS_FILEPOS = static_cast<unsigned __int128>(1) << 22;  // 1 bit, bit at position 22, starting from 0
    constexpr static unsigned __int128 MASK_COMPRESS_VERTEX = ((static_cast<unsigned __int128>(1) << 11) -1) << 11; // 11 bits
    constexpr static unsigned __int128 MASK_COMPRESS_BACKPTR = ((static_cast<unsigned __int128>(1) << 11) -1) << 0; // 11 bits

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
     * Decompress a pointer
     */
    DirectPointer(CompressedDirectPointer cdptr);

    /**
     * Copy assignment
     */
    DirectPointer& operator=(const DirectPointer& ptr);
    DirectPointer& operator=(CompressedDirectPointer cdptr);

    /**
     * Set the leaf & the segment of the pointer
     */
    void set_context(const Context& context) noexcept;

    /**
     * Load the context from the direct pointer
     */
    void restore_context(Context* context) noexcept;

    /**
     * Set the position in the sparse file
     */
    void set_filepos(uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr) noexcept;

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
     * Check whether a reader latch is held
     */
    bool has_latch() const noexcept;

    /**
     * Set the flag for the reader latch
     */
    void set_latch(bool value) noexcept;

    /**
     * Get a compressed representation of this pointer
     */
    CompressedDirectPointer compress() const noexcept;

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


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
bool DirectPointer::get_flag(uint64_t flags, uint16_t flag) {
    return (flags & flag) >> __builtin_ctz(flag) != 0;
}

inline
void DirectPointer::set_flag(uint64_t& flags, uint16_t flag, int value){
    flags = (flags & ~flag) | (value << __builtin_ctz(flag));
}

inline
bool DirectPointer::has_filepos() const noexcept {
    return get_flag(m_filepos, FLAG_HAS_FILEPOS);
}

inline
bool DirectPointer::has_latch() const noexcept {
    return get_flag(m_filepos, FLAG_LATCH_HELD);
}

inline
Leaf* DirectPointer::leaf() const noexcept {
    return m_leaf;
}

} // namespace
