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


#include "teseo/memstore/data_item.hpp"

#include <sstream>
#include <string>


using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Vertex                                                                  *
 *                                                                           *
 *****************************************************************************/

string Vertex::to_string(const Version* version) const {
    stringstream ss;
    ss << "Vertex " << m_vertex_id;
    if(m_first == 1){ ss << " [first]"; };
    if(m_lock == 1){ ss << " [lock]"; }
    ss << ", edge count: " << m_count;
    if(version != nullptr){
        ss << ", " << version->to_string();
    }
    return ss.str();
}


/*****************************************************************************
 *                                                                           *
 *   Edge                                                                    *
 *                                                                           *
 *****************************************************************************/

string Edge::to_string(const Vertex* source, const Version* version) const{
    stringstream ss;
    ss << "Edge " << source->m_vertex_id << " -> " << m_destination << ", weight: " << m_weight;
    if(version != nullptr){
        ss << ", " << version->to_string();
    }
    return ss.str();
}

/*****************************************************************************
 *                                                                           *
 *   Version                                                                 *
 *                                                                           *
 *****************************************************************************/

string Version::to_string() const {
    stringstream ss;
    ss << "[version present] ";
    ss << "type: " << (is_insert() ? "insert" : "remove") << ", ";
    ss << "back pointer: " << get_backptr() << ", ";
    ss << "chain length: ";
    if(m_undo_length == MAX_UNDO_LENGTH){
        ss << ">= " << MAX_UNDO_LENGTH << ", ";
    } else {
        ss << m_undo_length << ", ";
    }
    ss << "undo pointer: " << get_undo();
    return ss.str();
}

/*****************************************************************************
 *                                                                           *
 *   Data item                                                               *
 *                                                                           *
 *****************************************************************************/
string DataItem::to_string() const {
    stringstream ss;

    if(is_empty()){
        ss << "empty/unset";
    } else {
        ss << m_update;
        if(has_version()){
            ss << ", " << m_version.to_string();
        }
    }

    return ss.str();
}

} // namespace
