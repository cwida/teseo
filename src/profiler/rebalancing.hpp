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

#include <array>
#include <chrono>
#include <ostream>
#include <string>
#include <vector>
#include "util/timer.hpp"

namespace teseo::internal::profiler {

/**
 * The internal granularity of the timer
 */
using RebalancingTimeUnit = std::chrono::nanoseconds;

#if defined(HAVE_PROFILER)
/**
 * Simple timer to account the duration of a task inside a rebalance
 */
class RebalancingTimer {
    RebalancingTimer(const RebalancingTimer&) = delete;
    RebalancingTimer& operator=(const RebalancingTimer&) = delete;


    RebalancingTimeUnit* m_counter; // pointer to the event data
    util::Timer m_timer; // internal timer

public:
    // Create a timer for the given counter
    RebalancingTimer(RebalancingTimeUnit* counter, bool start_immediately = true) : m_counter(counter) {
        if(start_immediately) start();
    }

    // Destructor
    ~RebalancingTimer(){
        stop();
        *m_counter += m_timer.duration<RebalancingTimeUnit>();
    }

    // Restart the timer
    void start(){ m_timer.resume(); }

    // Stop the timer
    void stop() { m_timer.stop(); }
};

#else
// Dummy class
class RebalancingTimer {
public:
    RebalancingTimer(){ };
    void start(){ }
    void stop(){ }
};
#endif

/**
 * The type of the rebalancing operation
 */
enum class RebalancingType : int { REBALANCE, SPLIT, MERGE };

/**
 * The profiling recorded for a single execution
 */
struct RebalancingRecordedStats {
    RebalancingType m_type; // type of rebalancing
    uint64_t m_window_length = 0; // the window, in terms of number of segments, being rebalanced
    RebalancingTimeUnit m_total_time = RebalancingTimeUnit::zero(); // total rebalancing time
    RebalancingTimeUnit m_load_time = RebalancingTimeUnit::zero(); // total time to load all elements
    RebalancingTimeUnit m_write_time = RebalancingTimeUnit::zero(); // total time to save all elements
    RebalancingTimeUnit m_prune_time = RebalancingTimeUnit::zero(); // time spent pruning old versions
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
 * Single statistics attached to a single rebalancing task
 */
#if defined(HAVE_PROFILER)

class RebalancingProfiler {
    std::chrono::time_point<std::chrono::steady_clock> m_time_created; // when this instance was created
    RebalancingRecordedStats m_fields; // remaining stats

public:
    RebalancingProfiler(int64_t num_segments_input, int64_t num_segments_output);

    // Destructor
    ~RebalancingProfiler();

    RebalancingTimer profile_load_time(){
        return RebalancingTimer(&m_fields.m_load_time);
    }

    RebalancingTimer profile_write_time(){
        return RebalancingTimer(&m_fields.m_write_time);
    }

    RebalancingTimer profile_prune_time(bool start_immediately){
        return RebalancingTimer(&m_fields.m_prune_time);
    }

    void incr_count_in_num_qwords(int64_t v) { m_fields.m_in_num_qwords += v; }
    void incr_count_in_num_elts(){ m_fields.m_in_num_elts++; }
    void incr_count_in_num_vertices(){ m_fields.m_in_num_vertices++; }
    void incr_count_in_num_edges(){ m_fields.m_in_num_edges++; }
    void incr_count_out_num_qwords(int64_t v) { m_fields.m_out_num_qwords += v; }
    void incr_count_out_num_elts(int64_t v = 1){ m_fields.m_out_num_elts += v; }
    void incr_count_out_num_vertices(int64_t v = 1){ m_fields.m_out_num_vertices += v; }
    void incr_count_out_num_edges(int64_t v =1){ m_fields.m_out_num_vertices += v; }

};

#else

// Dummy class
class RebalancingProfiler {
public:
    RebalancingProfiler(int64_t num_segments_input, int64_t num_segments_output){ } ;
    RebalancingTimer profile_load_time(){ return RebalancingTimer(); }
    RebalancingTimer profile_write_time(){ return RebalancingTimer(); }
    RebalancingTimer profile_prune_time(bool start_immediately){ return RebalancingTimer(); }
    void incr_count_in_num_qwords(int64_t v) { }
    void incr_count_in_num_elts(){ }
    void incr_count_in_num_vertices(){ }
    void incr_count_in_num_edges(){ }
    void incr_count_out_num_qwords(int64_t v) { }
    void incr_count_out_num_elts(int64_t v = 1){ }
    void incr_count_out_num_vertices(int64_t v = 1){ }
    void incr_count_out_num_edges(int64_t v =1){ }
};

#endif


/**
 * Statistics associated to a single field of RebalancingStatistics
 */
struct RebalancingFieldStatistics{
    int64_t m_count =0; // number of elements counted
    int64_t m_sum =0; // sum of the times, in microsecs
    int64_t m_sum_sq = 0; // the squared sum of times
    int64_t m_average =0; // average, in microsecs
    int64_t m_min = 0; // minimum, in microsecs
    int64_t m_max = 0; // maximum, in microsecs
    int64_t m_stddev =0; // standard deviation, in microsecs
    int64_t m_median =-1; // median, in microsecs

    RebalancingFieldStatistics() : m_min(std::numeric_limits<int64_t>::max()){ }

    void to_json(std::ostream& out);
};

/**
 * Statistics associated to a single window
 */
struct RebalancingWindowStatistics {
    int64_t m_window_length; // the length of the window being rebalanced
    int64_t m_count = 0; // the number of entries associated to this window

    RebalancingFieldStatistics m_total_time; // total rebalancing time
    RebalancingFieldStatistics m_load_time; // total time to load all elements
    RebalancingFieldStatistics m_write_time; // total time to save all elements
    RebalancingFieldStatistics m_prune_time; // time spent pruning old versions
    RebalancingFieldStatistics m_in_num_qwords; // counter, number of qwords visited
    RebalancingFieldStatistics m_in_num_elts; // counter, number of elements loaded
    RebalancingFieldStatistics m_in_num_vertices;
    RebalancingFieldStatistics m_in_num_edges;
    RebalancingFieldStatistics m_out_num_qwords; // counter, number of qwords written
    RebalancingFieldStatistics m_out_num_elts; // counter, number of elements stored
    RebalancingFieldStatistics m_out_num_vertices;
    RebalancingFieldStatistics m_out_num_edges;

    RebalancingWindowStatistics(int64_t window_length) : m_window_length(window_length) { }

    void to_json(std::ostream& out);
};

/**
 * The complete statistics computed for the vector of statistics retrieved
 */
struct RebalancingCompleteStatistics {
    int64_t m_count = 0; // total amount of rebalancings performed
    RebalancingFieldStatistics m_total_time; // total rebalancing time
    RebalancingFieldStatistics m_load_time; // total time to load all elements
    RebalancingFieldStatistics m_write_time; // total time to save all elements

    std::vector<RebalancingWindowStatistics> m_rebalances; // rebalances
    std::vector<RebalancingWindowStatistics> m_merges; // increase the capacity
    std::vector<RebalancingWindowStatistics> m_splits; // decrease the capacity
};


/**
 * Thread type
 */
enum class ThreadType : int { WORKER, MERGER, ASYNC, AUTO /* determine it by the thread name */ };


/**
 * A sequence of recordings
 */
class RebalancingList {
    std::vector<RebalancingRecordedStats> m_list; // the recordings saved so far
    ThreadType m_thread_type; // the type of thread associated to this statistics

public:
    /**
     * Constructor
     */
    RebalancingList(ThreadType type = ThreadType::AUTO);

    /**
     * The type of thread associated to this statistics
     */
    ThreadType thread_type() const;

    /**
     * Save the given recordings
     */
    void insert(const RebalancingRecordedStats& stats);

    /**
     * Merge the rebalancing list
     */
    void insert(const RebalancingList* list);

    /**
     * Compute the statistics for the saved recordings
     */
    RebalancingCompleteStatistics statistics();
};

/**
 * The sequence of recordings, but maintain a sequence for each thread type
 */
class GlobalRebalancingList {
    std::array<RebalancingList, 3> m_lists; // recordings for each thread type
    std::array<uint64_t, 3> m_num_threads; // number of registered threads in each list

public:
    /**
     * Constructor
     */
    GlobalRebalancingList();

    /**
     * Save the given recordings
     */
    void insert(const RebalancingList* list);

    /**
     * Dump the recording into a json
     */
    void to_json(std::ostream& out);
};


} // namespace
