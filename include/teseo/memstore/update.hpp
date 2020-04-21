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
    enum { Vertex, Edge } m_entry_type;
    enum { Insert, Remove } m_update_type;
    Key m_key = KEY_MIN; // either a vertex or a pair <source, destination> for an edge
    double m_weight = 0.0;

    Update(); // private ctor

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
    void set_empty();

    /**
     * Change the weight for the record
     */
    void set_weight(double value);

    // Retrieve the update readable by the current transaction for the given delta record
    // @return true if the record is visible by the transaction, false otherwise
    static Update read_delta(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version);
    static Update read_delta(Context& context, const memstore::DataItem* data_item);
    static Update read_delta_optimistic(Context& context, const memstore::DataItem* data_item);
    static Update read_delta_optimistic(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version);
    static Update read_delta_impl(const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version, bool txn_response, Update* txn_payload);
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
Update::Update() : m_entry_type(Vertex), m_update_type(Insert) { }; // just to silent the compiter warnings

inline
Update::Update(bool is_vertex, bool is_insert, Key key, double weight) {
    m_entry_type = (is_vertex) ? Vertex : Edge;
    m_update_type = (is_insert) ? Insert : Remove;
    m_key = key;
    m_weight = weight;
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
double Update::weight() const {
    assert(is_edge() && "This record refers to a vertex");
    return m_weight;
}

inline
bool Update::is_insert() const {
    return m_update_type == Update::Insert;
}

inline
bool Update::is_remove() const {
    return m_update_type == Update::Remove;
}

inline
bool Update::is_vertex() const {
    return m_entry_type == Update::Vertex;
}

inline
bool Update::is_edge() const {
    return m_entry_type == Update::Edge;
}

inline
bool Update::is_empty() const {
    return m_key.source() == 0; // all updates must have a source vertex > 0
}

inline
void Update::flip() {
    m_update_type = (m_update_type == Update::Insert) ? Update::Remove : Update::Insert;
}

inline
void Update::set_empty() {
    m_key = Key { 0 } ;
}

inline
void Update::set_weight(double value){
    m_weight = value;
}

inline
std::ostream& operator<<(std::ostream& out, const Update& update){
    if(update.is_insert()){
        out << "Insert ";
    } else {
        out << "Remove ";
    }
    if(update.is_vertex()){
        out << "vertex " << update.source();
    } else {
        out << "edge " << update.source() << " -> " << update.destination() << " (weight: " << update.weight() << ")";
    }
    return out;
}


} // namespace
