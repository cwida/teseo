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

#include "rebal_stats.hpp"

namespace teseo::profiler {

/**
 * Thread type
 */
enum class ThreadType : int { WORKER, MERGER, ASYNC, AUTO /* determine it by the thread name */ };


/**
 * A sequence of recordings
 */
class RebalanceList {
    std::vector<RebalanceRecordedStats> m_list; // the recordings saved so far
    ThreadType m_thread_type; // the type of thread associated to this statistics

public:
    /**
     * Constructor
     */
    RebalanceList(ThreadType type = ThreadType::AUTO);

    /**
     * The type of thread associated to this statistics
     */
    ThreadType thread_type() const;

    /**
     * Save the given recordings
     */
    void insert(const RebalanceRecordedStats& stats);

    /**
     * Merge the rebalancing list
     */
    void insert(const RebalanceList* list);

    /**
     * Compute the statistics for the saved recordings
     */
    RebalanceCompleteStatistics statistics();
};


} // namespace
