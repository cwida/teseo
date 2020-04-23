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
#include "rebal_timer.hpp"

// Forward declaration
namespace teseo::rebalance { class Plan;  }

namespace teseo::profiler {

/**
 * Single statistics attached to a single rebalancing task
 */
#if defined(HAVE_PROFILER)

class RebalanceProfiler {
    std::chrono::time_point<std::chrono::steady_clock> m_time_created; // when this instance was created
    RebalanceRecordedStats m_fields; // remaining stats

public:
    RebalanceProfiler(const rebalance::Plan& plan);

    // Destructor
    ~RebalanceProfiler();

    RebalanceTimer profile_load_time(){
        return RebalanceTimer(&m_fields.m_load_time);
    }

    RebalanceTimer profile_write_time(){
        return RebalanceTimer(&m_fields.m_write_time);
    }

    RebalanceTimer profile_prune_time(bool start_immediately){
        return RebalanceTimer(&m_fields.m_prune_time);
    }

    void incr_count_in_num_qwords(int64_t v = 1) { m_fields.m_in_num_qwords += v; }
    void incr_count_in_num_elts(int64_t v = 1){ m_fields.m_in_num_elts += v; }
    void incr_count_in_num_vertices(int64_t v = 1){ m_fields.m_in_num_vertices += v; }
    void incr_count_in_num_edges(int64_t v = 1){ m_fields.m_in_num_edges += v; }
    void incr_count_out_num_qwords(int64_t v = 1) { m_fields.m_out_num_qwords += v; }
    void incr_count_out_num_elts(int64_t v = 1){ m_fields.m_out_num_elts += v; }
    void incr_count_out_num_vertices(int64_t v = 1){ m_fields.m_out_num_vertices += v; }
    void incr_count_out_num_edges(int64_t v =1){ m_fields.m_out_num_edges += v; }

};

#else

// Dummy class
class RebalanceProfiler {
public:
    RebalanceProfiler(const rebalance::Plan& plan){ } ;
    RebalanceTimer profile_load_time(){ return RebalanceTimer(); }
    RebalanceTimer profile_write_time(){ return RebalanceTimer(); }
    RebalanceTimer profile_prune_time(bool start_immediately){ return RebalanceTimer(); }
    void incr_count_in_num_qwords(int64_t v = 1) { }
    void incr_count_in_num_elts(int64_t v = 1){ }
    void incr_count_in_num_vertices(int64_t v = 1){ }
    void incr_count_in_num_edges(int64_t v = 1){ }
    void incr_count_out_num_qwords(int64_t v = 1) { }
    void incr_count_out_num_elts(int64_t v = 1){ }
    void incr_count_out_num_vertices(int64_t v = 1){ }
    void incr_count_out_num_edges(int64_t v =1){ }
};

#endif

} // namespace
