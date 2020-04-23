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
#include <future>
#include <vector>

#include "teseo/rebalance/plan.hpp"

namespace teseo::memstore {
class Context;
}

namespace teseo::rebalance {

// Exception thrown by a crawler when it gets overtaken by another competing crawler
struct RebalanceNotNecessary { };

/**
 * Explore the leaf to find a proper window to rebalance
 */
class Crawler {
    Crawler(const Crawler&) = delete;
    Crawler& operator=(const Crawler&) = delete;

    memstore::Context& m_context; // path memstore -> leaf -> segment
    bool m_can_continue; // whether this crawler is required to continue the rebalancing
    bool m_can_be_stopped; // whether this crawler is already executing the spread/split operation
    bool m_invalidate_upon_release; // whether to invalidate the acquired segments upon release
    int32_t m_window_start; // the first segment to rebalance (inclusive)
    int32_t m_window_end; // the last segment to rebalance (exclusive)
    int64_t m_used_space = 0; // total amount of words in the window
    std::vector<std::promise<void>*> m_threads2wait; // the threads we need to wait operating before we can proceed with the spread/split

    // Get & release the exclusive lock to the leaf's latch
    void leaf_xlock();
    void leaf_xunlock();

    // Acquire & release the segments in REBAL mode
    void acquire_segment(int64_t& segment_id, bool is_right_direction);
    void release_segment(int64_t segment_id, bool invalidate);

    // The height of the calibrator tree
    int64_t get_cb_height_per_chunk() const;

    // Get the minimum and maximum amount of space allowed by the density thresholds in the calibrator tree
    std::pair<int64_t, int64_t> get_thresholds(int height) const;

public:
    /**
     * Init the crawler
     * @param context information regarding the memstore, leaf and segment to rebalance
     * @param can_be_overtaken true if this crawler can be overtaken by other competing crawlers
     */
    Crawler(memstore::Context& context);

    /**
     * Destructor
     */
    ~Crawler();

    /**
     * Create a plan on the window to rebalance. Acquire the involved segments in rebalance mode along the ways
     * @throws RebalanceNotNecessary if the crawler has been overtaken by another crawler
     */
    Plan make_plan();

    /**
     * Lock all segments in the segment. This is the first step before merging two leaves together.
     */
    void lock2merge();

    /**
     * Retrieve the cardinality of the acquired window
     */
    uint64_t cardinality() const;

    /**
     * Retrieve the space usage in the acquired window, in terms of qwords
     */
    uint64_t used_space() const;

    /**
     * Invalidate the segments upon release
     */
    void invalidate();

    /**
     * Retrieve a string representation of this instance, for debugging purposes
     */
    std::string to_string() const;

    /**
     * Dump to stdout the content of this instance, for debugging purposes
     */
    void dump() const;
};


} // namespace
