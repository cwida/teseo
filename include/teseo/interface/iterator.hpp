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

#pragma once

#include "teseo.hpp"

#include "teseo/aux/auxiliary_snapshot.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/scan.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/interface.hpp"

//#define DEBUG
//#include "teseo/util/debug.hpp"

namespace teseo::interface {

/**
 * Wrapper for an Iterator scan
 */
template<bool logical, typename Callback>
class ScanEdges {
    ScanEdges(const ScanEdges&) = delete;
    ScanEdges& operator=(const ScanEdges&) = delete;

    bool m_vertex_found; // whether we have already traversed the source vertex
    const uint64_t m_vertex_id; // the vertex we are visiting
    transaction::TransactionImpl* m_transaction; // the user transaction
    const Callback& m_callback; // the user callback, the function ultimately invoked for each visited edge

private:
    // Execute the iterator
    void do_scan(memstore::Memstore* sa);

public:
    // Initialise the instance & start the iterator
    ScanEdges(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const Callback& callback);

    // Trampoline to the user callback
    bool operator()(uint64_t source, uint64_t destination, double weight);
};


template<bool logical, typename Callback>
ScanEdges<logical, Callback>::ScanEdges(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const Callback& callback) :
    m_vertex_found(false), m_vertex_id(vertex_id), m_transaction(txn), m_callback(callback) {
    do_scan(sa);
}

template<bool logical, typename Callback>
void ScanEdges<logical, Callback>::do_scan(memstore::Memstore* sa){
    //COUT_DEBUG("this: " << this);

    try {
        if(m_transaction->is_read_only()){
            sa->scan(m_transaction, m_vertex_id, /* edge destination */ 0, *this);
        } else {
            sa->scan_nolock(m_transaction, m_vertex_id, /* edge destination */ 0, *this);
        }

        if(!m_vertex_found){
            throw memstore::Error { memstore::Key { m_vertex_id }, memstore::Error::Type::VertexDoesNotExist };
        }
    } catch( const memstore::Error& error ){
        util::handle_error(error);
    }
};

template<bool logical, typename Callback>
bool ScanEdges<logical, Callback>::operator()(uint64_t source, uint64_t destination, double weight){
    //COUT_DEBUG("this: " << this << ", source: " << source << ", destination: " << destination << ", weight: " << weight);
    if(logical){
        source = m_transaction->aux_snapshot()->logical_id(source);
        destination = m_transaction->aux_snapshot()->logical_id(destination);
    }

    if(source != m_vertex_id){
        return false;
    } else if(destination == 0){
        m_vertex_found = true;
        return true;
    } else {
        uint64_t external_destination_id = destination -1; // I2E, internally vertices are shifted by +1
        return m_callback(external_destination_id, weight);
    }
};

} // namespace

namespace teseo  {

// trampoline to the implementation
template<typename Callback>
void Iterator::edges(uint64_t external_vertex_id, bool logical, Callback&& callback) const {
    if(is_closed()) throw LogicalError("LogicalError", "The iterator is closed", __FILE__, __LINE__, __FUNCTION__);
    m_num_alive ++; // to avoid an iterator being closed while in use

    transaction::TransactionImpl* txn = reinterpret_cast<transaction::TransactionImpl*>(m_pImpl);
    if(logical && !txn->is_read_only()){ throw LogicalError("LogicalError", "Logical vertices not supported for read-write transactions yet", __FILE__, __LINE__, __FUNCTION__); }

    memstore::Memstore* sa = context::global_context()->memstore();
    uint64_t internal_vertex_id = 0;
    if(logical){
        int64_t rank = external_vertex_id;
        if(rank >= txn->graph_properties().m_vertex_count) throw LogicalError("LogicalError", "Invalid logical vertex", __FILE__, __LINE__, __FUNCTION__);
        internal_vertex_id = txn->aux_snapshot()->vertex_id(rank);
    } else {
        internal_vertex_id = external_vertex_id +1; // E2I, the vertex ID 0 is reserved, translate all vertex IDs to +1
    }

    try {
        if(logical){
            interface::ScanEdges<true, Callback> scan(txn, sa, internal_vertex_id, callback);
        } else {
            interface::ScanEdges<false, Callback> scan(txn, sa, internal_vertex_id, callback);
        }

    } catch (...){
        m_num_alive--;
        throw;
    }

    m_num_alive --;
}

} // namespace

