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

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <ostream>

#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/transaction/undo.hpp"

namespace teseo::memstore {

/**
 * A static vertex entry in the segment
 */
class Vertex {
public:
    uint64_t m_vertex_id; // the id of the vertex
    uint64_t m_first :1; // whether this is the first vertex with this ID stored in a segment
    uint64_t m_lock :1; // vertex locked by a remover, to avoid phantom writes (new edge insertions) while progressing
    uint64_t m_count :62; // number of static edges following the static vertex
};

/**
 * A static edge entry in the segment
 */
class Edge {
public:
    uint64_t m_destination; // the destination id of the given edge
    double m_weight; // the weight associated to the edge
};

/**
 * An element in the segment can be either a vertex or an edge
 */
union Element {
    Vertex m_vertex;
    Edge m_edge;
};

constexpr static uint64_t OFFSET_ELEMENT = sizeof(Element) / 8; // => 2 words

/**
 * The version in the memstore is simply the head of an undo chain
 */
class Version {
public:
    uint64_t m_insdel:1; // 0 = insert, 1 = delete
    uint64_t m_undo_length:3; // length of the version chain: 0, ..., 6; 7 => length >= 7
    uint64_t m_backptr:12; // offset to the content
    uint64_t m_version:48; // ptr to the transaction version

    // Check whether the record refers to an insertion
    bool is_insert() const;

    // Check whether the records refers to a deletion
    bool is_remove() const;

    // Retrieve the head of the Undo chain
    transaction::Undo* get_undo() const;

    /**
     * Retrieve the currently set back pointer to the element
     */
    uint64_t get_backptr() const;


    // Reset the content of the field
    void reset();

    /**
     * Set the type of the operation (insert or deletion) for the head of history
     */
    void set_type(bool true_if_insert_or_false_if_remove);

    /**
     * Set the type of the operation depending on the update record
     */
    void set_type(const Update& update);

    /**
     * Reset the back pointer to the given value
     */
    void set_backptr(uint64_t offset);

    /**
     * Reset the head of the Undo chain
     */
    void set_undo(transaction::Undo* undo);

    /**
     * Remove the head of the Undo chain and set it to the next item
     */
    void unset_undo(transaction::Undo* undo);

    /**
     * Prune the undo records
     */
    void prune();

    /**
     * Prune the undo records only iff the length of the history reached its max
     */
    void prune_on_write();
};

constexpr static uint64_t MAX_UNDO_LENGTH = (1ull<< /* num bits in m_undo_length */ 3) -1; // => 7

constexpr static uint64_t OFFSET_VERSION = sizeof(Version) / 8; // => 1 word

/**
 * A data item stored in the dense file
 */
class DataItem {
public:
    Update m_update;
    Version m_version;

    /**
     * Check whether this item has been set
     */
    bool is_empty() const;

    /**
     * Check whether this item has a version set
     */
    bool has_version() const;
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
// Vertex, Edge and Version offsets, as multiples of 8 bytes (qwords)
static_assert(sizeof(Vertex) % 8 == 0);
static_assert(sizeof(Edge) % 8 == 0);
static_assert(sizeof(Version) % 8 == 0);
static_assert(sizeof(Vertex) == sizeof(Edge), "We further expect they share they same size, so we can memcpy chunks of a segment");
static_assert(sizeof(Element) == 2 * sizeof(uint64_t), "Expected to be two Qwords");
static_assert(sizeof(Version) == sizeof(uint64_t), "Expected to be one Qword");

inline
bool Version::is_insert() const {
    return m_insdel == 0;
}

inline
bool Version::is_remove() const {
    return m_insdel;
}

inline
void Version::reset() {
    reinterpret_cast<uint64_t*>(this)[0] = 0ull;
}

inline
transaction::Undo* Version::get_undo() const {
    return reinterpret_cast<transaction::Undo*>(m_version);
}

inline
uint64_t Version::get_backptr() const {
    return m_backptr;
}

inline
void Version::set_type(bool value){
    /* 0 = insert, 1 = remove */
    m_insdel = !value;
}

inline
void Version::set_type(const Update& update){
    set_type(update.is_insert());
}

inline
void Version::set_backptr(uint64_t offset){
    m_backptr = offset;
}

inline
void Version::set_undo(transaction::Undo* undo){
    if(undo == nullptr){
        m_undo_length = 0;
    } else if(m_undo_length < MAX_UNDO_LENGTH){
        m_undo_length ++;
    }
    m_version = reinterpret_cast<uint64_t>(undo);
}

inline
void Version::unset_undo(transaction::Undo* undo){
    assert(m_undo_length > 0 && "The are no versions associated to this version");
    assert(undo != nullptr && "Just remove the record all together from the sparse array");

    if(m_undo_length < MAX_UNDO_LENGTH){
        m_undo_length--;
        assert(m_undo_length > 0 && "Well, we assume that the given `undo' was the pointer to the previous head => length >= 2");
    }
    m_version = reinterpret_cast<uint64_t>(undo);
}

inline
void Version::prune_on_write(){
    if(m_undo_length >= MAX_UNDO_LENGTH) prune();
}

inline
void Version::prune(){
    auto result = transaction::Undo::prune(get_undo(), context::thread_context()->all_active_transactions());
    m_version = reinterpret_cast<uint64_t>(result.first);
    m_undo_length = std::min(result.second, MAX_UNDO_LENGTH);
}

inline
bool DataItem::is_empty() const {
    return m_update.is_empty();
}

inline
bool DataItem::has_version() const {
    return m_version.get_undo() != nullptr;
}


} // namespace
