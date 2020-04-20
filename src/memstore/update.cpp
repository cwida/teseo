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

#include <teseo/memstore/data_item.hpp>
#include "teseo/memstore/update.hpp"


#include "teseo/memstore/context.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"

using namespace std;

namespace teseo::memstore {

Update Update::read_delta(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* ptr){
    return read_delta_impl(context, vertex, edge, ptr, ptr->get_undo());
}

Update Update::read_delta_optimistic(Context& context, uint64_t version, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* ptr){
    const transaction::Undo* undo = ptr->get_undo();

    // is the pointer we just read is still valid
    context.m_segment->m_latch.validate_version(version); // throws Abort{}

    // if so, then proceed to the implementation
    return read_delta_impl(context, vertex, edge, ptr, undo);
}

Update Update::read_delta_impl(Context& context, const memstore::Vertex* vertex, const memstore::Edge* edge, const Version* version, const transaction::Undo* undo){
    Update* ptr_undo_update = nullptr;
    Update result;

    bool response = context.m_transaction->can_read(undo, (void**) &ptr_undo_update);

    if(response == true){ // fetch from the storage
        result.m_update_type = version->is_insert() ? Update::Insert : Update::Remove;
        if(edge == nullptr){ // this is a vertex;
            result.m_entry_type = Update::Vertex;
            result.m_key = Key (vertex->m_vertex_id );
            result.m_weight = 0;
        } else { // this is an edge
            result.m_entry_type = Update::Edge;
            result.m_key = Key ( vertex->m_vertex_id, edge->m_destination );
            result.m_weight = edge->m_weight;
        }

    } else { // fetch from the undo log
        assert(ptr_undo_update != nullptr && "A living version of this record must exist");
        result = *ptr_undo_update; // copy the update
        // the key pair src -> dst of the undo record must be equal to the one retrieved from the undo record
        assert(result.source() == vertex->m_vertex_id && "Source mismatch");
        assert((edge == nullptr || (edge->m_destination == result.destination())) && "Destination mismatch");
    }

    return result;
}

Update Update::read_delta(Context& context, const memstore::DataItem* data_item){
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

} // namespace
