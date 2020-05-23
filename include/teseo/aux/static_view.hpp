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

#include "teseo/aux/item.hpp"
#include "teseo/aux/view.hpp"

namespace teseo::memstore { class Memstore; } // forward declaration
namespace teseo::transaction { class TransactionImpl; } // forward declaration

namespace teseo::aux {

class ItemUndirected; // forward declaration

/**
 * A mapping between logical IDs and vertex IDs, including their degree.
 *
 * This class is not thread safe
 */
class StaticView : public View {
    StaticView(const StaticView&) = delete;
    StaticView& operator=(const StaticView&) = delete;

    const uint64_t m_num_vertices; // total number of vertices in the view, also the size of the degree vector
    const ItemUndirected* m_degree_vector; // Map a logical ID to its vertex_id and its degree
    const bool m_hash_direct; // whether the hash table is an array for direct access
    const uint64_t m_hash_capacity; // the size of the dictionary to map the vertex ids to their logical IDs
    const uint64_t m_hash_const; // hash constant to compute the hash function
    uint64_t* m_hash_array; // the actual dictionary used to translate the vertex IDs into logical IDs

    // Build the dictionary for the vertex IDs. Invoked at initialisation
    void create_vertex_id_mapping();

    struct HashParams {
        bool m_direct; // use a direct table rather than an hash table?
        uint64_t m_capacity; // the capacity of the dictionary array
        uint64_t m_const; // first hash key

        HashParams(uint64_t max_vertex_id, uint64_t num_vertices);
    };

    // Actual init
    StaticView(uint64_t num_vertices, const ItemUndirected* degree_vector, const HashParams& hash);

    // Compute the hash the given vertex id
    uint64_t hash(uint64_t vertex_id) const noexcept;

    // Profile the amount of collisions in the hashmap `m_hash_array'
    void profile_collisions() const;

public:
    // Create the view
    StaticView(uint64_t num_vertices, const ItemUndirected* degree_vector);

    // Destructor
    ~StaticView();

    // Retrieve the actual vertex ID associated to the logical ID
    // Return NOT_FOUND if the logical_id does not exist
    uint64_t vertex_id(uint64_t logical_id) const noexcept;

    // Retrieve the logical ID associated to the vertex ID
    // Return NOT_FOUND if vertex_id does not exist
    uint64_t logical_id(uint64_t vertex_id) const noexcept;

    // Retrieve the degree associated to the given vertex
    // Return NOT_FOUND if the vertex does not exist
    uint64_t degree(uint64_t id, bool is_logical) const noexcept;

    // Retrieve the total number of vertices
    uint64_t num_vertices() const noexcept;

    // Retrieve the underlying degree vector
    const ItemUndirected* degree_vector() const;

    // Create a view for the given transaction
    static StaticView* create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction);

    // Dump the content of the view to stdout, for debugging purposes
    void dump() const;
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/

inline
uint64_t StaticView::hash(uint64_t vertex_id) const noexcept {
    return vertex_id & m_hash_const;
}

// This method is so critical in scans, that it could be beneficial to inline it altogether
inline
uint64_t StaticView::logical_id(uint64_t vertex_id) const noexcept  {
    if(m_hash_direct){
        if(vertex_id >= m_hash_capacity){
            return aux::NOT_FOUND;
        } else {
            return m_hash_array[vertex_id];
        }
    } else {
        const uint64_t* __restrict A = m_hash_array;
        const ItemUndirected* __restrict DV = m_degree_vector;
        uint64_t slot = hash(vertex_id);

        while(A[slot] != aux::NOT_FOUND){
            if(DV[ A[slot] ].m_vertex_id == vertex_id ){
                return A[slot];
            }
            slot = ((slot + 1) & m_hash_const);
        }

        return aux::NOT_FOUND;
    }
}

} // namespace
