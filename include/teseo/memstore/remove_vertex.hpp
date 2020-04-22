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
#include <vector>

#include "key.hpp"

namespace teseo::memstore {

// forward declarations
class Context;
class DataItem;
class Edge;

/**
 * Wrapper class to remove a vertex and its attached edges from the memstore
 */
class RemoveVertex {
    Context& m_context; // current memstore->leaf->segment traversal
    const uint64_t m_vertex_id; // the vertex to remove
    std::vector<uint64_t>* m_outgoing_edges; // the list of outgoing edges removed
    bool m_owns_outgoing_edges; // whether the memory of `m_outgoing_edges' has been allocated by this instance
    uint64_t* m_scratchpad; // Temporary scratchpad, used to copy & move the versions in a sparse file
public:
    bool m_unlock_required = false; // Whether we need a further step to unlock the vertices
    uint64_t m_num_items_removed = 0; // Number of items removed so far
    Key m_key; // next edge to remove

    // Ascending visit, remove the vertex and its edges. If the edges span multiple segments, we need to lock the vertex
    // in the visited segments to avoid having other transactions being able to attach new edges in the meanwhile.
    void step1_lock_and_remove();

    // Unlock the vertices
    void step2_unlock();

public:
    /**
     * Initialise the object
     */
    RemoveVertex(Context& context, uint64_t vertex_id, std::vector<uint64_t>* out_outgoing_edges);

    /**
     * Destructor
     */
    ~RemoveVertex();

    /**
     * Execute the operation, that is remove the `vertex_id' and its attached edges.
     * Return the number of edges removed.
     */
    uint64_t operator()();

    /**
     * Retrieve the current context
     */
    Context& context();

    /**
     * Retrieve the vertex id that needs to be removed
     */
    uint64_t vertex_id() const;

    /**
     * Retrieve the internal scratchpad
     */
    uint64_t* scratchpad();

    /**
     * Signal the end of the visit, that is all outgoing edges have been removed
     */
    void set_done();

    /**
     * Check whether the operation was completed
     */
    bool done() const;

    /**
     * Keep track of a removed edge
     */
    void record_removed_edge(Edge* edge);
    void record_removed_edge(DataItem* data_item);
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
Context& RemoveVertex::context() {
    return m_context;
}

inline
uint64_t RemoveVertex::vertex_id() const {
    return m_vertex_id;
}

inline
uint64_t* RemoveVertex::scratchpad(){
    return m_scratchpad;
}

inline
void RemoveVertex::set_done(){
    m_key = Key { 0 };
}

inline
bool RemoveVertex::done() const {
    return m_key.source() == 0;
}


} // namespace
