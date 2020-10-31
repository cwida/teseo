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
#include <string>

#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/transaction/undo.hpp"

namespace teseo::memstore {


// forward declarations
class DataItem;
class Edge;
class Version;
class Vertex;

/**
 * A static vertex entry in the segment
 */
class Vertex {
public:
    uint64_t m_vertex_id; // the id of the vertex
    uint64_t m_first :1; // whether this is the first vertex with this ID stored in a segment
    uint64_t m_lock :1; // vertex locked by a remover, to avoid phantom writes (new edge insertions) while progressing
    uint64_t m_count :62; // number of static edges following the static vertex

    // Retrieve a string representation of the item, for debugging purposes
    std::string to_string(const Version* version = nullptr) const;

    // Validate the content of the vertex
    void validate(const Version* version) const; // only iff NDEBUG is not defined
    void do_validate(const Version* version) const; // always
};

/**
 * A static edge entry in the sparse file
 */
class Edge {
    Edge() = delete; // edges cannot be created, but only casted from a sparse array
    Edge(const Edge&) = delete;
    Edge& operator= (const Edge& e);
public:
    uint64_t m_destination; // the destination id of the given edge

    // Retrieve the weight associated to this edge
    double get_weight() const;

    // Retrieve the pointer where the weight is associated
    const double* get_weight_ptr() const;

    // Set the weight associated to this edge
    void set_weight(double value);

    // Retrieve a string representation of the item, for debugging purposes
    std::string to_string(const Vertex* source, const Version* version = nullptr) const;

    // Validate the content of the vertex
    void validate(const Vertex* source, const Version* version) const; // only iff NDEBUG is not defined
    void do_validate(const Vertex* source, const Version* version) const; // always
};


/**
 * The version in the memstore is simply the head of an undo chain
 */
class Version {
public:
    uint64_t m_insdel:1; // 0 = insert, 1 = delete
    uint64_t m_undo_length:3; // length of the version chain: 0, ..., 6; 7 => length >= 7
    uint64_t m_backptr:12; // offset to the content
    uint64_t m_version:48; // ptr to the transaction version

    // Create an empty version
    Version();

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

    /**
     * Retrieve a string representation of the item, for debugging purposes
     */
    std::string to_string() const;
};

// Space occupied by a vertex in the sparse file
constexpr uint64_t OFFSET_VERTEX = sizeof(Vertex) / sizeof(uint64_t); // => 2 qwords

// Space occupied by an edge in the sparse file
constexpr static uint64_t OFFSET_EDGE = sizeof(Edge) / sizeof(uint64_t); // => 1 qword

// Space occupied by a version ptr in the sparse file
constexpr static uint64_t OFFSET_VERSION = sizeof(Version) / sizeof(uint64_t); // => 1 qword

// Max value that can be stored into the version's counter m_undo_length, relative to the total number of existing versions of a data item
constexpr static uint64_t MAX_UNDO_LENGTH = (1ull<< /* num bits in m_undo_length */ 3) -1; // => 7

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

    /**
     * Retrieve a string representation of the item, for debugging purposes
     */
    std::string to_string() const;
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
static_assert(sizeof(Version) == sizeof(uint64_t), "Expected to be one Qword");

inline
void Vertex::validate(const Version* version) const {
#if !defined(NDEBUG)
    do_validate(version);
#endif
}

inline
double Edge::get_weight() const {
    return * get_weight_ptr();
}

inline
const double* Edge::get_weight_ptr() const {
    return reinterpret_cast<const double*>(this) + Leaf::section_size_qwords();
}

inline
void Edge::set_weight(double value){
    *( reinterpret_cast<double*>(this) + Leaf::section_size_qwords() ) = value;
}

inline
void Edge::validate(const Vertex* source, const Version* version) const {
#if !defined(NDEBUG)
    do_validate(source, version);
#endif
}

inline
Version::Version() {
    reset();
}

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
void Version::set_type(bool true_if_insert_or_false_if_remove){
    /* 0 = insert, 1 = remove */
    m_insdel = !true_if_insert_or_false_if_remove;
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
