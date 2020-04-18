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

#include "teseo/profiler/rebal_stats.hpp"

#include <ostream>

using namespace std;

namespace teseo::profiler {

void RebalanceFieldStatistics::to_json(ostream& out) {
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

void RebalanceWindowStatistics::to_json(ostream& out){
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

} // namespace

