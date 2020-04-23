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

#include "teseo/profiler/rebal_profiler.hpp"
#include "teseo/rebalance/plan.hpp"


// Forward declarations
namespace teseo::memstore {
class Context;
class Leaf;
}

namespace teseo::rebalance {

// Forward declarations
class ScratchPad;

/**
 * Spread the content in the sparse array
 */
class SpreadOperator {
    SpreadOperator(const SpreadOperator&) = delete;
    SpreadOperator& operator=(const SpreadOperator&) = delete;
    memstore::Context& m_context;
    ScratchPad& m_scratchpad;
    Plan m_plan;
    profiler::RebalanceProfiler m_profiler;

    uint64_t m_space_required = 0; // total amount of space required, in qwords

    // Load the elements from the involved segments into the scratch pad
    void load();
    void load(memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end);

    // Prune the undo records for the elements loaded in the scratch pad
    void prune();

    // Readjust the number of output segments after a pruning step
    void tune_plan();

    // Save the elements from the scratchpad back to the segments
    void save();
    void save(memstore::Leaf* leaf, int64_t window_start, int64_t window_end, uint64_t num_output_segments, uint64_t& num_segments_saved, uint64_t& budget_achieved, int64_t& pos_vertex, int64_t& pos_element);

    void update_fence_keys(memstore::Leaf* leaf, int64_t window_start, int64_t window_end);

public:
    /**
     * Create a new instance
     */
    SpreadOperator(memstore::Context& context, ScratchPad& scratchpad, const Plan& plan);

    /**
     * Execute the Spread operation
     */
    void operator()();
};

} // namespace
