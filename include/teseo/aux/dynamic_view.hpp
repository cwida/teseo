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

#include "teseo/aux/counting_tree.hpp"
#include "teseo/aux/view.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::memstore { class Memstore; } // forward declaration
namespace teseo::transaction { class TransactionImpl; } // forward declaration

namespace teseo::aux {

/**
 * A dynamic mapping between logical IDs and vertex IDs, including their degrees. The view can be
 * altered by creating new vertices, removing the existing vertices or modifying the degrees.
 *
 * This class is thread safe.
 */
class DynamicView : public View {
    DynamicView(const DynamicView&) = delete;
    DynamicView& operator=(const DynamicView&) = delete;

    CountingTree m_tree; // mapping between vertex ID and logical IDs
    mutable util::OptimisticLatch<0> m_latch; // to ensure thread-safety

    // Create a new instance of the view
    DynamicView(CountingTree&& tree);

public:
    // Destructor
    ~DynamicView();

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

    // Insert a new vertex in the view. Assume its degree to be 0
    void insert_vertex(uint64_t vertex_id);

    // Remove a vertex from the view
    void remove_vertex(uint64_t vertex_id);

    // Change the degree of the given vertex
    void change_degree(uint64_t vertex_id, int64_t diff);

    // Create a view for the given transaction
    static DynamicView* create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction);

    // Dump the content of the view to stdout, for debugging purposes
    void dump() const;
};


}
