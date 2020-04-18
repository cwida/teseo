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
#include <limits>
#include <ostream>
#include <vector>

#include "rebal_time_unit.hpp"

namespace teseo::profiler {

/**
 * The type of the rebalance operation
 */
enum class RebalanceType : int { REBALANCE, SPLIT, MERGE };

/**
 * The profiling recorded for a single execution
 */
struct RebalanceRecordedStats {
    RebalanceType m_type; // type of rebalancing
    uint64_t m_window_length = 0; // the window, in terms of number of segments, being rebalanced
    RebalanceTimeUnit m_total_time = RebalanceTimeUnit::zero(); // total rebalancing time
    RebalanceTimeUnit m_load_time = RebalanceTimeUnit::zero(); // total time to load all elements
    RebalanceTimeUnit m_write_time = RebalanceTimeUnit::zero(); // total time to save all elements
    RebalanceTimeUnit m_prune_time = RebalanceTimeUnit::zero(); // time spent pruning old versions
    int64_t m_in_num_qwords = 0; // total amount of qwords read
    int64_t m_in_num_elts = 0; // total number of elements at load time
    int64_t m_in_num_vertices = 0;
    int64_t m_in_num_edges = 0;
    int64_t m_out_num_qwords = 0; // total amount of qwords stored
    int64_t m_out_num_elts = 0; // total number of elements at save time
    int64_t m_out_num_vertices = 0;
    int64_t m_out_num_edges = 0;

};


/**
 * Statistics associated to a single field of RebalancingStatistics
 */
struct RebalanceFieldStatistics{
    int64_t m_count =0; // number of elements counted
    int64_t m_sum =0; // sum of the times, in microsecs
    int64_t m_sum_sq = 0; // the squared sum of times
    int64_t m_average =0; // average, in microsecs
    int64_t m_min = 0; // minimum, in microsecs
    int64_t m_max = 0; // maximum, in microsecs
    int64_t m_stddev =0; // standard deviation, in microsecs
    int64_t m_median =-1; // median, in microsecs

    RebalanceFieldStatistics() : m_min(std::numeric_limits<int64_t>::max()){ }

    void to_json(std::ostream& out);
};

/**
 * Statistics associated to a single window
 */
struct RebalanceWindowStatistics {
    int64_t m_window_length; // the length of the window being rebalanced
    int64_t m_count = 0; // the number of entries associated to this window

    RebalanceFieldStatistics m_total_time; // total rebalancing time
    RebalanceFieldStatistics m_load_time; // total time to load all elements
    RebalanceFieldStatistics m_write_time; // total time to save all elements
    RebalanceFieldStatistics m_prune_time; // time spent pruning old versions
    RebalanceFieldStatistics m_in_num_qwords; // counter, number of qwords visited
    RebalanceFieldStatistics m_in_num_elts; // counter, number of elements loaded
    RebalanceFieldStatistics m_in_num_vertices;
    RebalanceFieldStatistics m_in_num_edges;
    RebalanceFieldStatistics m_out_num_qwords; // counter, number of qwords written
    RebalanceFieldStatistics m_out_num_elts; // counter, number of elements stored
    RebalanceFieldStatistics m_out_num_vertices;
    RebalanceFieldStatistics m_out_num_edges;

    RebalanceWindowStatistics(int64_t window_length) : m_window_length(window_length) { }

    void to_json(std::ostream& out);
};

/**
 * The complete statistics computed for the vector of statistics retrieved
 */
struct RebalanceCompleteStatistics {
    int64_t m_count = 0; // total amount of rebalancings performed
    RebalanceFieldStatistics m_total_time; // total rebalancing time
    RebalanceFieldStatistics m_load_time; // total time to load all elements
    RebalanceFieldStatistics m_write_time; // total time to save all elements

    std::vector<RebalanceWindowStatistics> m_rebalances; // rebalances
    std::vector<RebalanceWindowStatistics> m_merges; // increase the capacity
    std::vector<RebalanceWindowStatistics> m_splits; // decrease the capacity
};

} // namespace
