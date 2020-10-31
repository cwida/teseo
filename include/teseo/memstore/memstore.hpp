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

#include "teseo/transaction/rollback_interface.hpp"

namespace teseo::aux { class Builder; } // forward declaration
namespace teseo::aux { class PartialResult; } // forward declaration
namespace teseo::aux { class View; } // forward declaration
namespace teseo::context { class GlobalContext; } // forward declaration
namespace teseo::rebalance { class MergerService; } // forward declaration
namespace teseo::transaction { class TransactionImpl; } // forward declaration
namespace teseo::transaction { class Undo; } // forward declaration

namespace teseo::memstore {

class Context; // forward declaration
class CursorState; // forward declaration
class Index; // forward declaration
class Key; // forward declaration
class Update; // forward declaration
class VertexTable; // forward declaration

/**
 * A tree having as index a trie (ART) and large leaves in the form of sparse arrays
 */
class Memstore : public transaction::RollbackInterface {
    Memstore(const Memstore&) = delete;
    Memstore& operator=(const Memstore&) = delete;

    const bool m_is_directed; // whether the semantic of the edge updates is for directed or undirected graphs. Note this flag only affects edge_insert and edge_remove
    Index* m_index; // primary index to the memory store
    VertexTable* m_vertex_table;  // secondary index to the memory store
    context::GlobalContext* m_global_context; // owner of this instance
    rebalance::MergerService* m_merger; // maintenance service for the leaves


    // Perform the given insertion, taking care of the consistency. That is, it ensures that the source vertex (but not the destination vertex) actually exists
    void do_insert_edge(Context& context, const Update& update);

    // Perform the given update to the tree
    void write(Context& context, const Update& update, bool has_source_vertex = true);

    // Check whether the given element exists
    bool has_item(Context& context, const Key& key, bool is_unlocked = false) const;

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
    virtual ~Memstore();

    /**
     * Insert the given vertex in the data structure
     */
    void insert_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id);

    /**
     * Check whether the given vertex is present
     */
    bool has_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id) const;

    /**
     * Retrieve the number of outgoing edges attached to the given vertex
     */
    uint64_t get_degree(transaction::TransactionImpl* transaction, uint64_t vertex_id) const; // lock the segments on the way
    uint64_t get_degree_nolock(transaction::TransactionImpl* transaction, uint64_t vertex_id) const; // optimistic readers

    /**
     * Scan all elements stored in the file, that are equal or greater than the given pair <source, destination>
     * The expected signature for the callback is:
     *     bool fn(uint64_t source, uint64_t destination, double weight);
     * The elements are forwarded to the callback in sorted order. The callback can discriminate between a vertex and
     * an edge as destination == 0 for the vertices and destination != 0 for the edges.
     * The scan ends either when the callback returns false or the are no more elements to retrieve.
     * The parameter `has_weight' determines if the weights also need to be retrieved for the edges
     */
    template<bool has_weight, typename Callback>
    void scan(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, Callback&& callback) const; // lock the segments on the way
    template< bool has_weight, typename Callback>
    void scan(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, CursorState* cs, Callback&& callback) const; // lock the segments on the way
    template<bool has_weight, typename Callback>
    void scan_nolock(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, Callback&& callback) const; // optimistic readers

    /**
     * Remove the given vertex and all its attached edges from the data structure.
     * @return the outdegree of the vertex removed
     */
    uint64_t remove_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id, std::vector<uint64_t>* out_edges = nullptr);

    /**
     * Insert the given edge in the data structure
     */
    void insert_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, double weight);

    /**
     * Check whether the given edge exists
     */
    bool has_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination) const;

    /**
     * Get the weight associated to the given edge
     */
    double get_weight(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination) const;

    /**
     * Remove the given edge from the data structure
     */
    void remove_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination);
    void remove_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, bool directed_only);

    /**
     * Check whether the semantic of edge updates tailors directed graphs
     */
    bool is_directed() const;

    /**
     * Check whether the semantic of edge updates tailors undirected graphs
     */
    bool is_undirected() const;

    /**
     * Process an undo record
     */
    void do_rollback(void* object, transaction::Undo* next) override;

    /**
     * Create the partial results for the auxiliary view
     */
    void aux_view(transaction::TransactionImpl* transaction, aux::Builder* builder);

    /**
     * Process a partial result (intermediate) to create the aux view.
     * This method is invoked by the worker threads of the runtime.
     */
    void aux_partial_result(transaction::TransactionImpl* transaction, aux::PartialResult* partial_result);

    /**
     * Retrieve the global context associated to this sparse array
     */
    context::GlobalContext* global_context();

    /**
     * Retrieve the attached merger service
     */
    rebalance::MergerService* merger();

    /**
     * Retrieve the index of the tree
     */
    Index* index(){ return m_index; }
    const Index* index() const { return m_index; }

    /**
     * Retrieve the vertex table (secondary index)
     */
    VertexTable* vertex_table(){ return m_vertex_table; }
    const VertexTable* vertex_table() const { return m_vertex_table; }

    /**
     * Retrieve a string representation of an undo record, for debugging purposes
     */
    std::string str_undo_payload(const void* object) const override;

    /**
     * Remove all chunks of the sparse array. Invoked by the global instance before releasing the
     * object, to avoid memory leaks
     */
    void clear();

    /**
     * Dump the content of this instance to stdout, for debugging purposes
     */
    void dump() const;
};

} // namespace

