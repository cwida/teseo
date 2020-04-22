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

#include "teseo/memstore/remove_vertex.hpp"

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/transaction/transaction_impl.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

RemoveVertex::RemoveVertex(Context& context, uint64_t vertex_id, vector<uint64_t>* out_outgoing_edges) :
        m_context(context), m_vertex_id(vertex_id), m_outgoing_edges(out_outgoing_edges), m_owns_outgoing_edges(false), m_key(vertex_id) {
    assert(vertex_id > 0 && "Vertices start from 1");
    assert(context.m_tree != nullptr && "Memstore not set");
    assert(context.m_transaction != nullptr && "Transaction not set");

    if(out_outgoing_edges == nullptr && m_context.m_tree->is_undirected()){
        m_outgoing_edges = new vector<uint64_t>();
        m_owns_outgoing_edges = true;
    }

    m_scratchpad = new uint64_t[ SparseFile::max_num_qwords() ];
}

RemoveVertex::~RemoveVertex(){
    if(m_owns_outgoing_edges){
        delete m_outgoing_edges; m_outgoing_edges = nullptr;
        m_owns_outgoing_edges = false;
    }

    delete m_scratchpad; m_scratchpad = nullptr;
}


/*****************************************************************************
 *                                                                           *
 *   Operator                                                                *
 *                                                                           *
 *****************************************************************************/

uint64_t RemoveVertex::operator()(){
    COUT_DEBUG("vertex id: " << m_vertex_id);
    Memstore* memstore = m_context.m_tree;

    m_key = Key{m_vertex_id};
    if(m_outgoing_edges != nullptr) { m_outgoing_edges->clear(); }
    m_unlock_required = false;

    try {
        step1_lock_and_remove();
    } catch (...){
        if(m_unlock_required){ step2_unlock(); }
        context().m_transaction->do_rollback(m_num_items_removed);
        throw;
    }

    // Did we remove the vertex?
    if(m_num_items_removed == 0){ // vertex not found
        assert(m_unlock_required == false && "If the vertex does not exist, it cannot be locked");
        throw Error { Key { m_vertex_id }, Error::VertexDoesNotExist };
    }

    if(m_unlock_required){ step2_unlock(); }

    uint64_t num_edges_removed = m_num_items_removed -1;  // the vertex + outgoing edges

    if(num_edges_removed > 0 && memstore->is_undirected()){ // remove incoming edges
        assert(m_outgoing_edges != nullptr && "It should have recorded all edges removed");
        assert(m_outgoing_edges->size() == num_edges_removed); // as above

        try {
            for(uint64_t i = 0; i < num_edges_removed; i++){
                memstore->remove_edge(m_context.m_transaction, m_outgoing_edges->at(i), m_vertex_id);
                m_num_items_removed++;
            }
        } catch(...){
            m_context.m_transaction->do_rollback(m_num_items_removed);
            throw;
        }
    }

    return num_edges_removed;
}

void RemoveVertex::step1_lock_and_remove(){
    do {
        context::ScopedEpoch epoch;

        try {
            context().writer_enter(m_key);
            context().m_segment->remove_vertex(*this);
            context().writer_exit();
        } catch( Abort ) {
            assert(context().m_segment == nullptr && "Segment still locked");
        } catch( ... ) {
            if(context().m_segment != nullptr){ context().writer_exit(); }  // release the lock on the segment
            throw;
        }

    } while( ! done() );
}

void RemoveVertex::step2_unlock(){
    if(m_key.source() != m_vertex_id){
        m_key.set(m_vertex_id, numeric_limits<uint64_t>::max());
    }

    do {
        context::ScopedEpoch epoch;

        try {
            context().writer_enter(m_key);
            context().m_segment->unlock_vertex(*this);
            context().writer_exit();
        } catch (Abort) {
            // try again
        }
    } while(!done());
}

/*****************************************************************************
 *                                                                           *
 *   Keep track of the removed edges                                         *
 *                                                                           *
 *****************************************************************************/

void RemoveVertex::record_removed_edge(Edge* edge){
    m_num_items_removed++;
    if(m_outgoing_edges != nullptr){
        m_outgoing_edges->push_back(edge->m_destination);
    }
}

void RemoveVertex::record_removed_edge(DataItem* item){
    assert(item != nullptr);
    assert(!item->m_update.is_empty() && "The item is empty");
    assert(item->m_update.is_edge() && "The item is not an edge");

    m_num_items_removed++;
    if(m_outgoing_edges != nullptr){
        m_outgoing_edges->push_back(item->m_update.destination());
    }
}


} // namespace


