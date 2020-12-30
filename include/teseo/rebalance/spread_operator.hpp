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

#include <vector>
#include <utility>

#include "teseo/memstore/context.hpp"
#include "teseo/profiler/rebal_profiler.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/rebalanced_leaf.hpp"

namespace teseo::memstore { class Leaf; } // Forward declarations

namespace teseo::rebalance {

// Forward declarations
class ScratchPad;

/**
 * Spread the content in the sparse array
 */
class SpreadOperator {
    SpreadOperator(const SpreadOperator&) = delete;
    SpreadOperator& operator=(const SpreadOperator&) = delete;
    memstore::Context m_context;
    ScratchPad& m_scratchpad;
    Plan& m_plan;
    std::vector<RebalancedLeaf> m_rebalanced_leaves; // keep track of the leaves used in this rebalance
    profiler::RebalanceProfiler m_profiler;

    uint64_t m_space_required = 0; // total amount of space required, in qwords

    // Load the elements from the involved segments into the scratch pad
    void load();
    void load(memstore::Leaf* leaf);
    void load(memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end);

    // Prune the undo records for the elements loaded in the scratch pad
    void prune();

    // Readjust the number of output segments after a pruning step
    void tune_plan();

    // Save the elements from the scratchpad back to the segments
    void save();
    void save(memstore::Leaf* leaf, int64_t window_start, int64_t window_end, uint64_t num_filled_segments, uint64_t& num_segments_saved, uint64_t& budget_achieved, int64_t& pos_vertex, int64_t& pos_element);

    // Resolve the fence keys and search keys in the index for the interval rebalanced
    void update_fence_keys();

    // Create a new leaf
    memstore::Leaf* create_leaf(uint64_t num_segments);

    // Debug only. Validate that the content of the loaded segments matches the content in the scratchpad
    void validate_load() const;

    // Debug only. Validate that during the pruning we didn't skip any element
    void validate_prune(const ScratchPad& copy) const;

    // Debug only. Check that the high fence keys of the leaves always point to the next leaf in the index
    void validate_leaf_traversals();

    // Debug only. Check that the whole content of the scratchpad has been copied in the fat tree
    void validate_save() const;

public:
    /**
     * Create a new instance
     */
    SpreadOperator(const memstore::Context& context, ScratchPad& scratchpad, /* in/out */ Plan& plan);

    /**
     * Destructor. Release all acquired locks
     */
    ~SpreadOperator();

    /**
     * Execute the Spread operation
     * @param out_rebalanced_leaves if given, a vector with the leaves rebalanced and the space, in qwords, filled
     */
    void operator()();

    /**
     * Get the last leaf of the interval
     */
    memstore::Leaf* last_leaf() const;
};

} // namespace
