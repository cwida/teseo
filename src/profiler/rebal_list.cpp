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
#include "teseo/profiler/rebal_list.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <string>
#include <vector>

#include "teseo/util/thread.hpp"

using namespace std;

namespace teseo::profiler {

RebalanceList::RebalanceList(ThreadType type) : m_thread_type(type) {
    if(type == ThreadType::AUTO){
        string thread_name = util::Thread::get_name();
        if(thread_name == "Teseo.Merger"){
            m_thread_type = ThreadType::MERGER;
        } else if (thread_name == "Teseo.Async"){
            m_thread_type = ThreadType::ASYNC;
        } else {
            m_thread_type = ThreadType::WORKER;
        }
    }
}

ThreadType RebalanceList::thread_type() const {
    return m_thread_type;
}

void RebalanceList::insert(const RebalanceRecordedStats& stats){
    m_list.push_back(stats);
}

void RebalanceList::insert(const RebalanceList* list){
    m_list.reserve(m_list.size() + list->m_list.size());
    for(const auto& e : list->m_list){
        m_list.push_back(e);
    }
}


[[maybe_unused]] static void add_stat(RebalanceFieldStatistics& field, int64_t value){
    field.m_count++;
    field.m_sum += value;
    field.m_sum_sq += value*value;
    field.m_min = min(value, field.m_min);
    field.m_max = max(value, field.m_max);
}

[[maybe_unused]] static int64_t to_int64_t(int64_t value){
    return value;
}

static int64_t to_int64_t(RebalanceTimeUnit time){
    return std::chrono::duration_cast<std::chrono::microseconds>(time).count();
}

[[maybe_unused]] static void add_stat(RebalanceFieldStatistics& field, RebalanceTimeUnit time){
    int64_t microsecs = to_int64_t(time);
    field.m_count++;
    field.m_sum += microsecs;
    field.m_sum_sq += microsecs*microsecs;
    field.m_min = min(microsecs, field.m_min);
    field.m_max = max(microsecs, field.m_max);
}

static void compute_avg_stddev(RebalanceFieldStatistics& field){
    if(field.m_count > 0) {
        field.m_average = field.m_sum / field.m_count;
        field.m_stddev = (static_cast<double>(field.m_sum_sq) / field.m_count) - pow(field.m_average, 2.0);
    }
    if(field.m_min == numeric_limits<int64_t>::max()){
        field.m_min = 0;
    }
}

#define finalize_stat(field_name) \
    if(index_start < index_end) { \
        auto& field = window.field_name; \
        assert(field.m_count == index_end - index_start && "Invalid count"); \
        std::sort(begin(profiles) + index_start, begin(profiles) + index_end, [](const RebalanceRecordedStats& stat1, RebalanceRecordedStats& stat2){ \
            return stat1.field_name < stat2.field_name; \
        }); \
        if(field.m_count % 2 == 1){ \
            field.m_median = to_int64_t( profiles[(index_start + index_end) /2].field_name ); \
        } else { \
            size_t d1 = (index_start + index_end) /2; \
            size_t d0 = d1 - 1; \
            field.m_median = to_int64_t(profiles[d0].field_name + profiles[d1].field_name) / 2; \
        } \
        compute_avg_stddev(field); \
    }


RebalanceCompleteStatistics RebalanceList::statistics(){
    RebalanceCompleteStatistics result;
    if(m_list.empty()) return result;

    auto& profiles = m_list;
    std::sort(begin(profiles), end(profiles), [](const RebalanceRecordedStats& stat1, RebalanceRecordedStats& stat2){
        // return true if stat1 < stat2, damn C++ interface

        // sort by type
        int type1 = static_cast<int>(stat1.m_type);
        int type2 = static_cast<int>(stat2.m_type);
        if(type1 < type2)
            return true;
        else if (type1 > type2)
            return false;

        // sort by the window length
        else
            return (stat1.m_window_length < stat2.m_window_length);
    });

    int64_t index_start = 0;
    auto do_compute_statistics = [&result, &profiles, &index_start](RebalanceType type, vector<RebalanceWindowStatistics>& container){
        while(index_start < (int64_t) profiles.size() && profiles[index_start].m_type == type){
            int64_t window_length = profiles[index_start].m_window_length;
            int64_t index_end = index_start; // excl
            RebalanceWindowStatistics window { window_length };
            while(index_end < (int64_t) profiles.size() && profiles[index_end].m_window_length == window_length && profiles[index_end].m_type == type){
                result.m_count++;
                window.m_count++;

                // global stats
                add_stat(result.m_total_time, profiles[index_end].m_total_time);
                add_stat(result.m_load_time, profiles[index_end].m_load_time);
                add_stat(result.m_write_time, profiles[index_end].m_write_time);

                // window stats
                add_stat(window.m_total_time, profiles[index_end].m_total_time);
                add_stat(window.m_load_time, profiles[index_end].m_load_time);
                add_stat(window.m_write_time, profiles[index_end].m_write_time);
                add_stat(window.m_prune_time, profiles[index_end].m_prune_time);
                add_stat(window.m_in_num_qwords, profiles[index_end].m_in_num_qwords);
                add_stat(window.m_in_num_elts, profiles[index_end].m_in_num_elts);
                add_stat(window.m_in_num_vertices, profiles[index_end].m_in_num_vertices);
                add_stat(window.m_in_num_edges, profiles[index_end].m_in_num_edges);
                add_stat(window.m_out_num_qwords, profiles[index_end].m_out_num_qwords);
                add_stat(window.m_out_num_elts, profiles[index_end].m_out_num_elts);
                add_stat(window.m_out_num_vertices, profiles[index_end].m_out_num_vertices);
                add_stat(window.m_out_num_edges, profiles[index_end].m_out_num_edges);

                index_end++;
            }

            finalize_stat(m_total_time);
            finalize_stat(m_load_time);
            finalize_stat(m_write_time);
            finalize_stat(m_prune_time);
            finalize_stat(m_in_num_qwords);
            finalize_stat(m_in_num_elts);
            finalize_stat(m_in_num_vertices);
            finalize_stat(m_in_num_edges);
            finalize_stat(m_out_num_qwords);
            finalize_stat(m_out_num_elts);
            finalize_stat(m_out_num_vertices);
            finalize_stat(m_out_num_edges);

            container.push_back(window);

            index_start = index_end; // next iteration
        }
    };

    // fire!
    do_compute_statistics(RebalanceType::REBALANCE, result.m_rebalances);
    do_compute_statistics(RebalanceType::SPLIT, result.m_splits);
    do_compute_statistics(RebalanceType::MERGE, result.m_merges);


    // global stats
    compute_avg_stddev(result.m_total_time);
    compute_avg_stddev(result.m_load_time);
    compute_avg_stddev(result.m_write_time);

    return result;
}

} // namespace
