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
#include <ostream>

#include "key.hpp"

namespace teseo::transaction{ class Undo; } // forward decl.

namespace teseo :: memstore {

// Forward declarations
class Context;
class DataItem;
class Edge;
class Version;
class Vertex;

/**
 * The data associated to a single update
 */
class Update {
    constexpr static uint32_t FLAG_ENTRY_TYPE = 0x1;
    constexpr static uint32_t FLAG_UPDATE_TYPE = 0x2;
    constexpr static uint32_t FLAG_EMPTY = 0x4;
    uint32_t m_flags = 0;

    //enum { Vertex, Edge } m_entry_type;
    //enum { Insert, Remove } m_update_type;
    Key m_key = KEY_MIN; // either a vertex or a pair <source, destination> for an edge
    double m_weight = 0.0;

    Update(); // private ctor

    // Retrieve the value associated to the given flag
    uint32_t get_flag(uint32_t flag) const;

    // Set the given flag
    void set_flag(uint32_t flag, uint32_t value);

    // Mark the update as an insert
    void set_insert();

    // Mark the update as a deletion
    void set_remove();

    // Mark the update for a vertex
    void set_vertex();

    // Mark the update related to an edge
    void set_edge();

    // Retrieve the update readable by the current transaction for the given delta record
    static Update read_delta_locked(Context& context, const memstore::DataItem* data_item);
    static Update read_delta_locked(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version);
    static Update read_delta_optimistic(Context& context, const memstore::DataItem* data_item);
    static Update read_delta_optimistic(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version);
    static Update read_delta_impl(const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version, bool txn_response, Update* txn_payload);
    static Update read_simple(const memstore::Vertex* vertex, const memstore::Edge* edge); // when there is not a version

public:
    /**
     * Create an instance
     */
    Update(bool is_vertex, bool is_insert, Key key, double weight = 0.0);

    /**
     * Retrieve the source vertex of the update
     */
    uint64_t source() const;

    /**
     * Retrieve the destination vertex of the update
     */
    uint64_t destination() const;

    /**
     * Retrieve the pair <source, destination> as a key
     */
    Key key() const;

    /**
     * Swap source & destination of the vertex
     */
    void swap();

    /**
     * Retrieve the weight associated to this update
     */
    double weight() const;

    /**
     * Check whether the update refers to an insertion
     */
    bool is_insert() const;

    /**
     * Check whether the updates refers to a deletion
     */
    bool is_remove() const;

    /**
     * Check whether the update refers to a vertex
     */
    bool is_vertex() const;

    /**
     * Check whether the update refers to an edge
     */
    bool is_edge() const;

    /**
     * Flip the type of the operation: insert -> remove, remove -> insert
     */
    void flip();

    /**
     * Check if the update has been invalidated due to a rollback
     */
    bool is_empty() const;

    /**
     * Invalidate the given update, mark it as empty
     */
    void set_empty(bool value = true);

    /**
     * Change the weight for the record
     */
    void set_weight(double value);

    // Retrieve the update readable by the current transaction for the given delta record
    // @return true if the record is visible by the transaction, false otherwise
    static Update read_delta(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version);
    static Update read_delta(Context& context, const memstore::DataItem* data_item);


    // Dump to stdout the content of this update
    void dump() const;

    // Retrieve a string representation of this update
    std::string to_string() const;
};

std::ostream& operator<<(std::ostream& out, const Update& update);

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
Update::Update() {
    set_empty();
};

inline
Update::Update(bool is_vertex, bool is_insert, Key key, double weight) {
    if(is_vertex){
        set_vertex();
    } else {
        set_edge();
    }
    if(is_insert){
        set_insert();
    } else {
        set_remove();
    }
    m_key = key;
    m_weight = weight;

    set_empty(false);
}

inline
uint32_t Update::get_flag(uint32_t flag) const {
    return static_cast<int>((m_flags & flag) >> __builtin_ctz(flag));
}

inline
void Update::set_flag(uint32_t flag, uint32_t value){
    m_flags = (m_flags & ~flag) | (value << __builtin_ctz(flag));
}

inline
uint64_t Update::source() const {
    return m_key.source();
}

inline
uint64_t Update::destination() const {
    assert(is_edge() && "This record refers to a vertex");
    return m_key.destination();
}

inline
Key Update::key() const {
    return m_key;
}

inline
void Update::swap(){
    m_key.set(m_key.destination(), m_key.source());
}

inline
double Update::weight() const {
    assert(is_edge() && "This record refers to a vertex");
    return m_weight;
}

inline
bool Update::is_insert() const {
    return get_flag(FLAG_UPDATE_TYPE) == 0; // 0 => insert, 1 => remove
}

inline
bool Update::is_remove() const {
    return get_flag(FLAG_UPDATE_TYPE) == 1; // 0 => insert, 1 => remove
}

inline
void Update::set_insert() {
    set_flag(FLAG_UPDATE_TYPE, 0); // 0 => insert, 1 => remove
}

inline
void Update::set_remove() {
    set_flag(FLAG_UPDATE_TYPE, 1); // 0 => insert, 1 => remove
}

inline
bool Update::is_vertex() const {
    return get_flag(FLAG_ENTRY_TYPE) == 0; // 0 => vertex, 1 => edge
}

inline
bool Update::is_edge() const {
    return get_flag(FLAG_ENTRY_TYPE) == 1; // 0 => vertex, 1 => edge
}

inline
void Update::set_vertex() {
    set_flag(FLAG_ENTRY_TYPE, 0); // 0 => vertex, 1 => edge
}

inline
void Update::set_edge() {
    set_flag(FLAG_ENTRY_TYPE, 1); // 0 => vertex, 1 => edge
}

inline
bool Update::is_empty() const {
    return get_flag(FLAG_EMPTY) == 0;
}

inline
void Update::set_empty(bool value) {
    set_flag(FLAG_EMPTY, !value); // 0 => empty, 1 => set
}

inline
void Update::flip() {
    set_flag(FLAG_UPDATE_TYPE, !get_flag(FLAG_UPDATE_TYPE));
}


inline
void Update::set_weight(double value){
    m_weight = value;
}



} // namespace
