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

#include "teseo/memstore/update.hpp"

#include <iostream>
#include <sstream>
#include <string>

#include "teseo/memstore/context.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"

using namespace std;

namespace teseo::memstore {


/*****************************************************************************
 *                                                                           *
 *   read_delta                                                              *
 *                                                                           *
 *****************************************************************************/

// For the sparse file
Update Update::read_delta(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version){
    if(version == nullptr){ // missing delta
        Update result = read_simple(context, vertex, edge);
        if(context.has_version()){ context.validate_version(); }
        return result;
    } else if(context.has_version()){ // optimistic reader
        return read_delta_optimistic(context, vertex, edge, version);
    } else { // // locked reader, with a version
        return read_delta_locked(context, vertex, edge, version);
    }
}

Update Update::read_delta_locked(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version) {
    Update* ptr_undo_update = nullptr;

    bool response = context.m_transaction->can_read(version->get_undo(), (void**) &ptr_undo_update);

    return read_delta_impl(context, vertex, edge, version, response, ptr_undo_update);
}

Update Update::read_delta_optimistic(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version) {
    // is the pointer we just read still valid ?
    assert(context.m_version != numeric_limits<uint64_t>::max() && "No version set");
    auto undo = version->get_undo();
    context.validate_version(); // throws Abort{}

    Update* ptr_undo_update = nullptr;
    bool response = context.m_transaction->can_read_optimistic(undo, (void**) &ptr_undo_update, context);

    Update result = read_delta_impl(context, vertex, edge, version, response, ptr_undo_update);

    context.validate_version(); // throws Abort{}
    return result;
}

Update Update::read_delta_impl(const Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version, bool txn_response, Update* txn_payload){
    Update result;
    if(txn_response == true){ // fetch from the storage

        if(version->is_insert()){
            result.set_insert();
        } else {
            result.set_remove();
        }

        if(edge == nullptr){ // this is a vertex
            result.set_vertex();
            result.m_key = Key (vertex->m_vertex_id );
            result.set_weight(0);
        } else { // this is an edge
            result.set_edge();
            result.m_key = Key ( vertex->m_vertex_id, edge->m_destination );

            const bool is_optimistic = context.has_version();
            if(is_optimistic){ // set the actual weight
                result.set_weight( edge->get_weight(context) );
            } else { // lazy assignment
                result.set_weight_ptr( edge->get_weight_ptr(context) );
            }
        }

    } else { // fetch from the undo log
        assert(txn_payload != nullptr && "A living version of this record must exist");
        result = *txn_payload; // copy the update
        // the key pair src -> dst of the undo record must be equal to the one retrieved from the undo record
        assert(result.source() == vertex->m_vertex_id && "Source mismatch");
        assert((edge == nullptr || (edge->m_destination == result.destination())) && "Destination mismatch");
    }

    return result;
}

Update Update::read_simple(const memstore::Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge){
    Update update;
    update.set_insert();
    if(edge == nullptr){ // this is a vertex;
        update.set_vertex();
        update.m_key = Key (vertex->m_vertex_id );
        update.set_weight(0);
    } else { // this is an edge
        update.set_edge();
        update.m_key = Key ( vertex->m_vertex_id, edge->m_destination );
        const bool is_optimistic = context.has_version();
        if(is_optimistic){ // set the actual weight
            update.set_weight( edge->get_weight(context) );
        } else { // lazy assignment
            update.set_weight_ptr( edge->get_weight_ptr(context) );
        }
    }
    return update;
}

// For the dense file
Update Update::read_delta(Context& context, const memstore::DataItem* data_item) {
    if(!context.has_version()){ // locked reader
        return read_delta_locked(context, data_item);
    } else { // optimistic reader
        return read_delta_optimistic(context, data_item);
    }
}

Update Update::read_delta_locked(Context& context, const memstore::DataItem* data_item){
    Update* ptr_undo_update = nullptr;
    transaction::Undo* undo = data_item->m_version.get_undo();

    Update result;
    if(undo == nullptr || context.m_transaction->can_read(undo, (void**) &ptr_undo_update)){ // fetch from the store
        result = data_item->m_update;
    } else { // fetch from the undo log
        result = *ptr_undo_update; // copy the update
    }

    return result;
}

Update Update::read_delta_optimistic(Context& context, const memstore::DataItem* data_item){
    Update* ptr_undo_update = nullptr;
    transaction::Undo* undo = data_item->m_version.get_undo();
    assert(context.m_version != numeric_limits<uint64_t>::max() && "No version set");
    context.validate_version(); // is the pointer `undo' valid?

    bool fetch_from_storage = (undo == nullptr || context.m_transaction->can_read_optimistic(undo, (void**) &ptr_undo_update, context));

    Update result;
    if(fetch_from_storage){ // fetch from the store
        result = data_item->m_update;
    } else { // fetch from the undo log
        result = *ptr_undo_update; // copy the update
    }

    context.validate_version(); // check we are not returning rubbish to the caller
    return result;
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

string Update::to_string() const {
    stringstream ss;
    if(is_empty()){
        ss << "Null ";
    } else if(is_insert()){
        ss << "Insert ";
    } else {
        ss << "Remove ";
    }
    if(is_vertex()){
        ss << "vertex " << source();
    } else {
        ss << "edge " << source() << " -> " << destination() << " (weight: " << weight() << ")";
    }

    return ss.str();
}

void Update::dump() const {
    cout << to_string() << endl;
}

ostream& operator<<(ostream& out, const Update& update) {
    out << update.to_string();
    return out;
}


} // namespace
