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

#include "teseo/aux/static_view.hpp"
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
template<bool logical, typename View, typename Callback>
class ScanEdges {
    ScanEdges(const ScanEdges&) = delete;
    ScanEdges& operator=(const ScanEdges&) = delete;

    bool m_vertex_found; // whether we have already traversed the source vertex
    const uint64_t m_vertex_id; // the vertex we are visiting
    transaction::TransactionImpl* m_transaction; // the user transaction
    View const * const m_view; // materialised view to translate the vertex IDs into logical IDs
    const Callback& m_callback; // the user callback, the function ultimately invoked for each visited edge

private:
    // Execute the iterator
    void do_scan(memstore::Memstore* sa);

public:
    // Initialise the instance & start the iterator
    ScanEdges(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const View* view, const Callback& callback);

    // Trampoline to the user callback
    bool operator()(uint64_t source, uint64_t destination, double weight);
};


template<bool logical, typename View, typename Callback>
ScanEdges<logical, View, Callback>::ScanEdges(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const View* view, const Callback& callback) :
    m_vertex_found(false), m_vertex_id(vertex_id), m_transaction(txn), m_view(view), m_callback(callback) {
    do_scan(sa);
}

template<bool logical, typename View, typename Callback>
void ScanEdges<logical, View, Callback>::do_scan(memstore::Memstore* sa){

    try {
        if(m_transaction->is_read_only()){
            if(m_view != nullptr){
                sa->scan_direct(m_transaction, m_vertex_id, /* edge destination */ 0, m_view, m_view->logical_id(m_vertex_id), *this);
            } else {
                sa->scan(m_transaction, m_vertex_id, /* edge destination */ 0, *this);
            }
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

template<bool logical, typename View, typename Callback>
bool ScanEdges<logical, View, Callback>::operator()(uint64_t source, uint64_t destination, double weight){
    //COUT_DEBUG("this: " << this << ", source: " << source << ", destination: " << destination << ", weight: " << weight);

    if(source != m_vertex_id){
        return false;
    } else if(destination == 0){
        m_vertex_found = true;
        return true;
    } else {
        if(!logical){
            uint64_t external_destination_id = destination -1; // I2E, internally vertices are shifted by +1
            return m_callback(external_destination_id, weight);
        } else {
            uint64_t rank = m_view->logical_id(destination);
            assert(rank != aux::NOT_FOUND && "The destination should always exist");
            return m_callback(rank, weight);
        }

    }
};

template<bool logical, typename Callback>
void scan_impl2(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const aux::View* view, Callback&& callback){
    if(txn->is_read_only()){
        interface::ScanEdges<logical, aux::StaticView, Callback> scan(txn, sa, vertex_id, static_cast<const aux::StaticView*>(view), callback);
    } else {
        interface::ScanEdges<logical, aux::View, Callback> scan(txn, sa, vertex_id, static_cast<const aux::View*>(view), callback);
    }
}

} // namespace

namespace teseo  {

// trampoline to the implementation
template<typename Callback>
void Iterator::edges(uint64_t external_vertex_id, bool logical, Callback&& callback) const {


    if(is_closed()) throw LogicalError("LogicalError", "The iterator is closed", __FILE__, __LINE__, __FUNCTION__);
    m_num_alive ++; // to avoid an iterator being closed while in use

    try {

        transaction::TransactionImpl* txn = reinterpret_cast<transaction::TransactionImpl*>(m_pImpl);
        if(logical && !txn->is_read_only()){ throw LogicalError("LogicalError", "Logical vertices not supported for read-write transactions yet", __FILE__, __LINE__, __FUNCTION__); }

        const aux::View* view = nullptr;
        if(txn->has_aux_view() || logical){
            view = txn->aux_view(/* numa aware ? */ true);
        }

        memstore::Memstore* sa = context::global_context()->memstore();
        uint64_t internal_vertex_id = 0;
        if(logical){
            int64_t rank = external_vertex_id;
            internal_vertex_id = view->vertex_id(rank);
            if(internal_vertex_id == aux::NOT_FOUND) throw LogicalError("LogicalError", "Invalid logical vertex", __FILE__, __LINE__, __FUNCTION__);
        } else {
            internal_vertex_id = external_vertex_id +1; // E2I, the vertex ID 0 is reserved, translate all vertex IDs to +1
        }


        if(logical){
            interface::scan_impl2<true>(txn, sa, internal_vertex_id, view, callback);
        } else {
            interface::scan_impl2<false>(txn, sa, internal_vertex_id, view, callback);
        }

    } catch (...){
        m_num_alive--;
        throw;
    }

    m_num_alive --;
}

} // namespace

