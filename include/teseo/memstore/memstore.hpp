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

#include "teseo/transaction/rollback_interface.hpp"

namespace teseo::context { class GlobalContext; } // forward declaration
namespace teseo::transaction { class TransactionImpl; } // forward declaration

namespace teseo::memstore {

class Context; // forward declaration
class Index; // forward declaration
class Update; // forward declaration

/**
 * A tree having as index a trie (ART) and large leaves in the form of sparse arrays
 */
class Memstore : public transaction::RollbackInterface {
    Memstore(const Memstore&) = delete;
    Memstore& operator=(const Memstore&) = delete;

    const bool m_is_directed; // whether the semantic of the edge updates is for directed or undirected graphs. Note this flag only affects edge_insert and edge_remove
    Index* m_index; // the actual implementation of the memstore
    context::GlobalContext* m_global_context; // owner of this instance

    // Perform the given update to the tree
    void write(Context& context, const Update& update, bool has_source_vertex = true);

public:
    /**
     * Create a new instance
     * @param global_context the database owning this data structure
     * @param directed whether the underlying graph is directed
     */
    Memstore(context::GlobalContext* global_context, bool directed);

    /**
     * Destructor
     */
    ~Memstore();

    /**
     * Insert the given vertex in the sparse array
     */
    void insert_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id);

    /**
     * Check whether the semantic of edge updates tailors directed graphs
     */
    bool is_directed() const;

    /**
     * Check whether the semantic of edge updates tailors undirected graphs
     */
    bool is_undirected() const;

    /**
     * Remove all chunks of the sparse array. Invoked by the global instance before releasing the
     * object, to avoid memory leaks
     */
    void clear();

    /**
     * Retrieve the global context associated to this sparse array
     */
    context::GlobalContext* global_context();

    /**
     * Retrieve the index of the tree
     */
    Index* index(){ return m_index; }

};

} // namespace

