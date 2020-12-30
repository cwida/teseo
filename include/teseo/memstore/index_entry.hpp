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

#include <cassert>
#include <cinttypes>
#include <iostream>

#include "leaf.hpp"

namespace teseo::memstore {

class Leaf; // forward declaration
class Segment; // forward declaration

/**
 * A single entry retrieved from the index
 */
class IndexEntry {
    uint64_t m_segment_id:16;
    uint64_t m_leaf_addr:48;

public:
    /**
     * Create an empty instance
     */
    IndexEntry();

    /**
     * Create a new instance for the given leaf and segment
     */
    IndexEntry(Leaf* leaf, uint64_t segment_id);

    /**
     * Retrieve an invalid entry
     */
    static IndexEntry INVALID;

    /**
     * Check whether the returned entry is invalid. Invalid entries are returned by the index when a
     * search keys was not found.
     */
    bool is_invalid() const;

    /**
     * Retrieve the leaf associated to this entry
     */
    Leaf* leaf() const;

    /**
     * Retrieve the segment_id associated to this entry
     */
    uint64_t segment_id() const;

    /**
     * Retrieve the segment associated to this entry
     */
    Segment* segment() const;

    /**
     * Equality operators
     */
    bool operator==(const IndexEntry& other) const;
    bool operator!=(const IndexEntry& other) const;

    /**
     * Dump the content of this instance to stdout, for debugging purposes
     */
    void dump() const;
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
static_assert(sizeof(IndexEntry) == sizeof(uint64_t), "Expected to be one Qword");

inline
IndexEntry::IndexEntry() : m_segment_id(0), m_leaf_addr(0) { }

inline
IndexEntry::IndexEntry(Leaf* leaf, uint64_t segment_id){
    assert(segment_id <= ((1ull<<16) -1) && "We only have 16 bits to store the segment_id, the given value is greater than that");
    m_segment_id = segment_id;
    m_leaf_addr = reinterpret_cast<uint64_t>(leaf);
}

inline
bool IndexEntry::is_invalid() const {
    return m_leaf_addr == 0;
}

inline
Leaf* IndexEntry::leaf() const {
    return reinterpret_cast<Leaf*>(m_leaf_addr);
}

inline
uint64_t IndexEntry::segment_id() const {
    return m_segment_id;
}

inline
Segment* IndexEntry::segment() const {
    return leaf()->get_segment(segment_id());
}

inline
bool IndexEntry::operator==(const IndexEntry& other) const {
    return m_leaf_addr == other.m_leaf_addr && m_segment_id == other.m_segment_id;
}

inline
bool IndexEntry::operator!=(const IndexEntry& other) const {
    return !(*this == other);
}

inline
IndexEntry IndexEntry::INVALID = IndexEntry(nullptr, 0);

inline
std::ostream& operator<<(std::ostream& out, const IndexEntry& entry){
    out << "leaf: " << entry.leaf() << ", segment_id: " << entry.segment_id();
    return out;
}

}
