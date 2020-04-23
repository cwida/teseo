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
#include <string>

// Forward declarations
namespace teseo::memstore {
    class Leaf;
}

namespace teseo::rebalance {

/**
 * Determine what kind of rebalance to perform. It can be either:
 * 1- a spread, that is,  move the element among a sequence of segment inside the same leaf
 * 2- a split, that splitting a leaf into two or more leaves
 * 3- a merge, that is merging multiple leafs into one
 */
class Plan {
    Plan(); // use the factory methods: create_spread(), create_merge() and create_split()

    memstore::Leaf* m_leaf1; // the leaf to operate
    memstore::Leaf* m_leaf2; // the last leaf to merge
    int32_t m_window_start; // the first segment in m_leaf1 to rebalance (inclusive)
    int32_t m_window_end; // the last segment in m_leaf1 to rebalance (exclusive)
    int64_t m_num_output_segments; // total number of segment in the output
    uint64_t m_cardinality; // the total number of elements to be copied

public:
    /**
     * Copy constructor
     */
    Plan(const Plan&) = default;

    /**
     * Assignment operator
     */
    Plan& operator=(const Plan&) = default;

    /**
     * Factory method, create a new plan for a split
     * @param cardinality an upper bound on the total number of elements to be copied
     * @param leaf the leaf to split
     * @param num_segments the total number of segments that needs to be used in the output. It affects the final
     *        number of leaves created
     */
    static Plan create_split(uint64_t cardinality, memstore::Leaf* leaf, uint64_t num_out_segments);

    /**
     * Factory method, create a `spread' plan
     * @param cardinality an upper bound on the total number of elements to be copied
     * @param leaf the leaf to operate
     * @param window_start the first segment of the window (inclusive)
     * @param window_end the last segment of the window (exclusive)
     */
    static Plan create_spread(uint64_t cardinality, memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end);

    /**
     * Factory method, create a new plan to merge all leaves from leaf1 all the way up leaf2
     * @param cardinality an upper bound on the total number of elements to be copied
     * @param leaf1 the first leaf to merge together
     * @param leaf2 the last leaf to merge together
     */
    static Plan create_merge(uint64_t cardinality, memstore::Leaf* leaf1, memstore::Leaf* leaf2);

    /**
     * Check the kind of action requested
     */
    bool is_spread() const;
    bool is_merge() const;
    bool is_split() const;

    /**
     * Retrieve the first segment of the window to rebalance, inclusive
     */
    uint64_t window_start() const;

    /**
     * Retrieve the number of segments to copy
     */
    uint64_t window_length() const;

    /**
     * Retrieve the last segment of the window to rebalance, exclusive
     */
    uint64_t window_end() const;

    /**
     * Retrieve the total number of output segments to produce
     */
    uint64_t num_output_segments() const;

    /**
     * Retrieve the total number of elements that will be in a spread
     */
    uint64_t cardinality() const;

    /**
     * Retrieve the first and last leaves of the plan
     */
    memstore::Leaf* leaf() const;
    memstore::Leaf* first_leaf() const;
    memstore::Leaf* last_leaf() const;

    /**
     * Reset the number of output segments to the given value
     */
    void set_num_output_segments(uint64_t value);

    /**
     * Get a string representation of this plan, for debugging purposes
     */
    std::string to_string() const;

    /**
     * Dump to the output stream this plan, for debugging purposes
     */
    void dump() const;
};


} // namespace
