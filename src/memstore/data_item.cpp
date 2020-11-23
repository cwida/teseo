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

#include "teseo/memstore/update.hpp"
#include "teseo/transaction/undo.hpp"

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

void Vertex::do_validate(const Version* version) const {
#if !defined(NDEBUG)
    if(version == nullptr) return; // skip
    assert(m_first == 1 && "Dummy vertices cannot have a version");
    const transaction::Undo* undo = version->get_undo();
    assert(undo != nullptr && "Missing undo record");
    assert(version->m_undo_length > 0 && "Undo length set to zero, but an undo record is at least present");
    const Update* update = reinterpret_cast<Update*>(undo->payload());
    assert(update != nullptr && "No update stored");
    assert(update->is_vertex() && "Incorrect type, expected a vertex");
    assert(m_vertex_id == update->source() && "Vertex mismatch");
    assert(update->key().destination() == 0 && "Expected set to zero, because this is a vertex");
#endif
}

/*****************************************************************************
 *                                                                           *
 *   Edge                                                                    *
 *                                                                           *
 *****************************************************************************/
string Edge::to_string(const Vertex* source, const Version* version, const Leaf* leaf) const{
    stringstream ss;
    ss << "Edge " << source->m_vertex_id << " -> " << m_destination << ", weight: ";

    if(leaf != nullptr){
        ss << get_weight(leaf);
    } else {
        ss << "n/a";
    }

    if(version != nullptr){
        ss << ", " << version->to_string();
    }
    return ss.str();
}

void Edge::do_validate(const Vertex* source, const Version* version) const {
#if !defined(NDEBUG)
    if(version == nullptr) return; // skip
    assert(source != nullptr && "vertex nullptr");
    const transaction::Undo* undo = version->get_undo();
    assert(undo != nullptr && "Missing undo record");
    assert(version->m_undo_length > 0 && "Undo length set to zero, but an undo record is at least present");
    const Update* update = reinterpret_cast<Update*>(undo->payload());
    assert(update != nullptr && "No update stored");
    assert(update->is_edge() && "Incorrect type, expected an edge");
    assert(source->m_vertex_id == update->source() && "Source mismatch");
    assert(m_destination == update->destination() && "Destination mismatch");
#endif
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
