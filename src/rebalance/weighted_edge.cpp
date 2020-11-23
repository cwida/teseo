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

#include "teseo/rebalance/weighted_edge.hpp"

#include <cassert>
#include <sstream>
#include <string>

#include "teseo/memstore/data_item.hpp"

using namespace std;

namespace teseo::rebalance {

string WeightedEdge::to_string(const memstore::Vertex* source, const memstore::Version* version) const {
    stringstream ss;
    ss << "Edge " << source->m_vertex_id << " -> " << m_destination << ", weight: " << m_weight;
    if(version != nullptr){
        ss << ", " << version->to_string();
    }
    return ss.str();
}

} // namespace
