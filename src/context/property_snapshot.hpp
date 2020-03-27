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

#include "latch.hpp"

namespace teseo::internal::context {

class GlobalContext; // forward declaration
class TransactionSequence; // forward declaration

/**
 * The global properties attached to a graph
 */
struct GraphProperty {
    int64_t m_vertex_count = 0; // number of vertices in the graph
    int64_t m_edge_count = 0; // number of edges in the graph

    // Sum up with the properties of the given argument
    void operator +=(const GraphProperty& p2);

    // Check whether there have been local changes
    operator bool() const;
};

/**
 * A snapshot of the global properties attached to a graph
 */
struct PropertySnapshot{
    uint64_t m_transaction_id; // commit time fo the transaction which updated the property
    GraphProperty m_property; // the underlying property
};

/**
 * Store a sequence of snapshot properties
 */
class PropertySnapshotList {
    PropertySnapshotList(const PropertySnapshotList&) = delete;
    PropertySnapshotList& operator=(const PropertySnapshotList&) = delete;

    constexpr static uint64_t MIN_CAPACITY = 4; // the minimum capacity of the array m_list
    constexpr static uint64_t AUTO_PRUNE_SIZE = 4; // invoke prune if the list contains more than 16 entries
    PropertySnapshot* m_list; // the list of committed properties
    uint64_t m_capacity; // the current capacity of the array m_list
    uint64_t m_size; // the current number of elements stored in the array m_list
    mutable OptimisticLatch<1> m_latch; // protect against multiple accesses

    /**
     * Change the capacity of the underlying list
     */
    void resize(uint64_t new_capacity);

    /**
     * Prune the property list
     */
    void prune0(const TransactionSequence* txseq);
    void prune0(uint64_t high_water_mark);

public:

    /**
     * Constructor
     */
    PropertySnapshotList();

    /**
     * Destructor
     */
    ~PropertySnapshotList();

    /**
     * Insert a new property in the list.
     * Optionally provide a transaction list to prune the property list of unaccessible snapshots when its
     * size becomes bigger than the predefined threshold AUTO_PRUNE_SIZE
     */
    void insert(const PropertySnapshot& property, const TransactionSequence* txseq = nullptr);

    /**
     * Prune the property list according to the active transaction list
     */
    void prune(const TransactionSequence* txseq);

    /**
     * Prune the property list according to the given high water mark
     */
    void prune(uint64_t high_water_mark);

    /**
     * Merge the two property lists together
     * @param gctxt pass the global context explicitly as this method may be invoked by an unregistering thread_context
     */
    void acquire(GlobalContext* gcntxt, PropertySnapshotList& list);

    /*
     * Retrieve the snapshot with visible by the given transaction id
     */
    GraphProperty snapshot(uint64_t transaction_id) const;

    /**
     * The underlying version of this list
     */
    uint64_t version() const;

    /**
     * Current size of the list
     */
    uint64_t size() const;
};

/**
 * Implementation details
 */
inline
void GraphProperty::operator+=(const GraphProperty& p2){
    m_vertex_count += p2.m_vertex_count;
    m_edge_count += p2.m_edge_count;
}

inline
GraphProperty::operator bool() const {
    return m_vertex_count != 0 || m_edge_count != 0;
}

inline
GraphProperty operator+(const GraphProperty& p1, const GraphProperty& p2){
    GraphProperty res = p1;
    res += p2;
    return res;
}

} // namespace
