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

#include "teseo/profiler/rebal_global_list.hpp"

#include <ostream>
#include <string>
#include <vector>

#include "teseo/profiler/rebal_list.hpp"
#include "teseo/third-party/magic_enum.hpp"

using namespace std;

namespace teseo::profiler {


GlobalRebalanceList::GlobalRebalanceList() : m_lists(), m_num_threads() {

}

void GlobalRebalanceList::insert(const RebalanceList* list){
    if(list == nullptr) return;
    m_num_threads[ (int) list->thread_type() ]++;
    m_lists[ (int) list->thread_type() ].insert(list);
}

static void to_json0(ostream& out, vector<RebalanceWindowStatistics>& vect){
    for(uint64_t i = 0; i < vect.size(); i++){
        if (i > 0) out << ", ";
        vect[i].to_json(out);
    }
}

void GlobalRebalanceList::to_json(std::ostream& out){
    out << "[";
    for(uint64_t i = 0; i < m_lists.size(); i++){
        RebalanceCompleteStatistics statistics = m_lists[i].statistics();

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

} // namespace
