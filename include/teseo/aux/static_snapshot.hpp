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

#include <unordered_map>


#include "teseo/aux/auxiliary_snapshot.hpp"

namespace teseo::memstore { class Memstore; } // forward declaration
namespace teseo::transaction { class TransactionImpl; } // forward declaration

namespace teseo::aux {

class ItemUndirected; // forward declaration

/**
 * A mapping between logical IDs and vertex IDs, including their degree.
 *
 * This class is not thread safe
 */
class StaticSnapshot : public AuxiliarySnapshot {
    StaticSnapshot(const StaticSnapshot&) = delete;
    StaticSnapshot& operator=(const StaticSnapshot&) = delete;

    const uint64_t m_num_vertices; // total number of vertices in the snapshot, also the size of the degree vector
    const ItemUndirected* m_degree_vector; // Map a logical ID to its vertex_id and its degree
    std::unordered_map<uint64_t, uint64_t> m_vertex_ids; // Mapping to a vertex id to its logical Id

    // Build the dictionary for the vertex IDs. Invoked at initialisation
    void create_vertex_id_mapping();

public:
    // Create the snapshot
    StaticSnapshot(uint64_t num_vertices, const ItemUndirected* degree_vector);

    // Destructor
    ~StaticSnapshot();

    // Retrieve the actual vertex ID associated to the logical ID
    // Return NOT_FOUND if the logical_id does not exist
    uint64_t vertex_id(uint64_t logical_id) const noexcept override;

    // Retrieve the logical ID associated to the vertex ID
    // Return NOT_FOUND if vertex_id does not exist
    uint64_t logical_id(uint64_t vertex_id) const noexcept override;

    // Retrieve the degree associated to the given vertex
    // Return NOT_FOUND if the vertex does not exist
    uint64_t degree(uint64_t id, bool is_logical) const noexcept override;

    // Retrieve the total number of vertices
    uint64_t num_vertices() const noexcept override;

    // Retrieve the underlying degree vector
    const ItemUndirected* degree_vector() const;

    // Create a snapshot for the given transaction
    static StaticSnapshot* create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction);

    // Dump the content of the snapshot to stdout, for debugging purposes
    void dump() const;
};

} // namespace
