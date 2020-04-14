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

#include "rebalancing.hpp"

#include <algorithm>
#include <cmath>

#include "context/thread_context.hpp"
#include "third-party/magic_enum.hpp"
#include "util/miscellaneous.hpp"


using namespace std;
using namespace std::chrono;

namespace teseo::internal::profiler {

#if defined(HAVE_PROFILER)

RebalancingProfiler::RebalancingProfiler(int64_t num_segments_input, int64_t num_segments_output) : m_time_created(steady_clock::now()) {
    m_fields.m_window_length = num_segments_output;
    if(num_segments_input < num_segments_output){
        m_fields.m_type = RebalancingType::MERGE;
    } else if(num_segments_input == num_segments_output){
        m_fields.m_type = RebalancingType::REBALANCE;
    } else { // num_segments_input > num_segments_output
        m_fields.m_type = RebalancingType::MERGE;
    }

    assert(duration_cast<microseconds>(m_fields.m_load_time).count() < 10000);
}

RebalancingProfiler::~RebalancingProfiler(){
    m_fields.m_total_time = steady_clock::now() - m_time_created;
    context::thread_context()->rebalances()->insert(m_fields);
}

#endif

void RebalancingFieldStatistics::to_json(ostream& out) {
    out << "{";
    out << "\"count\": " << m_count << ", ";
    out << "\"sum\": " << m_sum << ", ";
    out << "\"mean\": " << m_average << ", ";
    out << "\"median\": " << m_median << ", ";
    out << "\"min\": " << m_min << ", ";
    out << "\"max\": " << m_max << ", ";
    out << "\"stddev\": " << m_stddev;
    out << "}";
}

void RebalancingWindowStatistics::to_json(std::ostream& out){
    out << "{";
    out << "\"window_length\": " << m_window_length << ", ";
    out << "\"count\": " << m_count << ", ";
    out << "\"total_time\": "; m_total_time.to_json(out); out << ", ";
    out << "\"load_time\": "; m_load_time.to_json(out); out << ", ";
    out << "\"write_time\": "; m_write_time.to_json(out); out << ", ";
    out << "\"prune_time\": "; m_prune_time.to_json(out); out << ", ";
    out << "\"in_num_qwords\": "; m_in_num_qwords.to_json(out); out << ", ";
    out << "\"in_num_elts\": "; m_in_num_elts.to_json(out); out << ", ";
    out << "\"in_num_vertices\": "; m_in_num_vertices.to_json(out); out << ", ";
    out << "\"in_num_edges\": "; m_in_num_edges.to_json(out); out << ", ";
    out << "\"out_num_qwords\": "; m_out_num_qwords.to_json(out); out << ", ";
    out << "\"out_num_elts\": "; m_out_num_elts.to_json(out); out << ", ";
    out << "\"out_num_vertices\": "; m_out_num_vertices.to_json(out); out << ", ";
    out << "\"out_num_edges\": "; m_out_num_edges.to_json(out); //out << ", ";
    out << "}";
}

RebalancingList::RebalancingList(ThreadType type) : m_thread_type(type) {
    if(type == ThreadType::AUTO){
        string thread_name = util::get_thread_name();
        if(thread_name == "Teseo.Merger"){
            m_thread_type = ThreadType::MERGER;
        } else if (thread_name == "Teseo.Async"){
            m_thread_type = ThreadType::ASYNC;
        } else {
            m_thread_type = ThreadType::WORKER;
        }
    }

}

ThreadType RebalancingList::thread_type() const {
    return m_thread_type;
}

void RebalancingList::insert(const RebalancingRecordedStats& stats){
    m_list.push_back(stats);
}

void RebalancingList::insert(const RebalancingList* list){
    m_list.reserve(m_list.size() + list->m_list.size());
    for(const auto& e : list->m_list){
        m_list.push_back(e);
    }
}

GlobalRebalancingList::GlobalRebalancingList() : m_lists(), m_num_threads() {

}

void GlobalRebalancingList::insert(const RebalancingList* list){
    if(list == nullptr) return;
    m_num_threads[ (int) list->thread_type() ]++;
    m_lists[ (int) list->thread_type() ].insert(list);
}

static void to_json0(ostream& out, std::vector<RebalancingWindowStatistics>& vect){
    for(uint64_t i = 0; i < vect.size(); i++){
        if (i > 0) out << ", ";
        vect[i].to_json(out);
    }
}

void GlobalRebalancingList::to_json(std::ostream& out){
    out << "[";
    for(uint64_t i = 0; i < m_lists.size(); i++){
        RebalancingCompleteStatistics statistics = m_lists[i].statistics();

        if(i > 0) out << ", ";
        out << "{";

        ThreadType type = (ThreadType) i;
        out << "\"role\": \"" << magic_enum::enum_name(type) << "\", ";
        out << "\"num_threads\": " << m_num_threads[i] << ", ";
        out << "\"count\":"  << statistics.m_count << ", ";
        out << "\"total_time\":"; statistics.m_total_time.to_json(out); out << ", ";
        out << "\"load_time\":"; statistics.m_load_time.to_json(out); out << ", ";
        out << "\"write_time\":"; statistics.m_write_time.to_json(out); out << ", ";

        out << "\"rebalances\": ["; to_json0(out, statistics.m_rebalances); out << "], ";
        out << "\"splits\": ["; to_json0(out, statistics.m_splits); out << "], ";
        out << "\"merges\": ["; to_json0(out, statistics.m_merges); out << "] ";

        out << "}";
    }

    out << "]";
};


[[maybe_unused]] static void add_stat(RebalancingFieldStatistics& field, int64_t value){
    field.m_count++;
    field.m_sum += value;
    field.m_sum_sq += value*value;
    field.m_min = min(value, field.m_min);
    field.m_max = max(value, field.m_max);
}

[[maybe_unused]] static int64_t to_int64_t(int64_t value){
    return value;
}

static int64_t to_int64_t(RebalancingTimeUnit time){
    return duration_cast<std::chrono::microseconds>(time).count();
}

[[maybe_unused]] static void add_stat(RebalancingFieldStatistics& field, RebalancingTimeUnit time){
    int64_t microsecs = to_int64_t(time);
    field.m_count++;
    field.m_sum += microsecs;
    field.m_sum_sq += microsecs*microsecs;
    field.m_min = min(microsecs, field.m_min);
    field.m_max = max(microsecs, field.m_max);
}

static void compute_avg_stddev(RebalancingFieldStatistics& field){
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
        std::sort(begin(profiles) + index_start, begin(profiles) + index_end, [](const RebalancingRecordedStats& stat1, RebalancingRecordedStats& stat2){ \
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


RebalancingCompleteStatistics RebalancingList::statistics(){
    RebalancingCompleteStatistics result;
    if(m_list.empty()) return result;

    auto& profiles = m_list;
    std::sort(begin(profiles), end(profiles), [](const RebalancingRecordedStats& stat1, RebalancingRecordedStats& stat2){
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
    auto do_compute_statistics = [&result, &profiles, &index_start](RebalancingType type, vector<RebalancingWindowStatistics>& container){
        while(index_start < profiles.size() && profiles[index_start].m_type == type){
            int64_t window_length = profiles[index_start].m_window_length;
            int64_t index_end = index_start; // excl
            RebalancingWindowStatistics window { window_length };
            while(index_end < profiles.size() && profiles[index_end].m_window_length == window_length && profiles[index_end].m_type == type){
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
                add_stat(window.m_in_num_edges, profiles[index_end].m_in_num_vertices);
                add_stat(window.m_out_num_qwords, profiles[index_end].m_out_num_qwords);
                add_stat(window.m_out_num_elts, profiles[index_end].m_out_num_elts);
                add_stat(window.m_out_num_vertices, profiles[index_end].m_out_num_vertices);
                add_stat(window.m_out_num_edges, profiles[index_end].m_out_num_vertices);

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
    do_compute_statistics(RebalancingType::REBALANCE, result.m_rebalances);
    do_compute_statistics(RebalancingType::SPLIT, result.m_splits);
    do_compute_statistics(RebalancingType::MERGE, result.m_merges);


    // global stats
    compute_avg_stddev(result.m_total_time);
    compute_avg_stddev(result.m_load_time);
    compute_avg_stddev(result.m_write_time);

    return result;
}

} // namespace


