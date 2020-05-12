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

#include "teseo/context/global_context.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/scan.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/interface.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

namespace teseo::interface {

// Issue the driver to restart the iterator
struct Restart {};

template<bool is_read_only, typename Callback>
class Scan {
    Scan(const Scan&) = delete;
    Scan& operator=(const Scan&) = delete;

    const transaction::TransactionImpl* m_transaction; // pointer to the related transaction object
    bool m_vertex_found = false; // whether we have traversed the source vertex
    // For Read-Write (RW) transactions, there is a chance that a writes occur while we are using the iterator.
    // The idea is to detect the write by checking the version associated to the transaction's latch, and restart it
    // in case of mismatch. The fields source/destination keep track of the point where to begin the new scan.
    // Read-Only (RO) transactions, source is always the vertex to visit, while the fields destination and version
    // are ignored.
    const uint64_t m_source; // the source to visit, this is the given Vertex ID
    uint64_t m_destination; // the next destination to visit
    uint64_t m_version; // for R.W. transaction, the time when the Transaction's optimistic latch was checked
    const Callback& m_callback; // the user callback, the function ultimately invoked for each visited edge

private:
    // Execute the iterator
    void do_scan(memstore::Memstore* sa);

public:
    // Initialise the object and start the scan
    Scan(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const Callback& callback);

    // Trampoline to the user callback
    bool operator()(uint64_t source, uint64_t destination, double weight);
};


template<bool is_read_only, typename Callback>
Scan<is_read_only, Callback>::Scan(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const Callback& callback) :
    m_transaction(txn), m_source(vertex_id), m_destination(0), m_version(0), m_callback(callback) {
    do_scan(sa);
}

template<bool is_read_only, typename Callback>
void Scan<is_read_only, Callback>::do_scan(memstore::Memstore* sa){
    COUT_DEBUG("this: " << this);

    try {
        if(is_read_only){
            sa->scan(m_transaction, m_source, m_destination, *this);
        } else {
            bool done = false;
            do {
                m_version = m_transaction->latch().read_version();
                try {
                    sa->scan_nolock(m_transaction, m_source, m_destination, *this);
                    done = true;
                } catch(Restart){ }
            } while(!done);
        }

        if(!m_vertex_found){
            throw memstore::Error { memstore::Key { m_source }, memstore::Error::Type::VertexDoesNotExist };
        }
    } catch( const memstore::Error& error ){
        util::handle_error(error);
    }
};

template<bool is_read_only, typename Callback>
bool Scan<is_read_only, Callback>::operator()(uint64_t source, uint64_t destination, double weight){
    COUT_DEBUG("this: " << this << ", source: " << source << ", destination: " << destination << ", weight: " << weight);

    // detect if a write occurred while the iterator was executing
    if(!is_read_only && m_transaction->latch().read_version() != m_version){
        throw Restart{};
    }

    if(source != m_source){ // we're done
        return false;
    } else if(destination == 0){
        m_vertex_found = true;
        return true;
    } else {
        if(!is_read_only) { m_destination = destination +1; } // save the point to restart the iterator in case of a Restart{} exception
        uint64_t external_destination_id = destination -1; // I2E, internally vertices are shifted by +1
        return m_callback(external_destination_id, weight);
    }
};

} // namespace

namespace teseo  {

// trampoline to the implementation
template<typename Callback>
void Iterator::scan_out(uint64_t external_vertex_id, Callback&& callback) const{
    transaction::TransactionImpl* txn = reinterpret_cast<transaction::TransactionImpl*>(m_pImpl);
    memstore::Memstore* sa = context::global_context()->memstore();
    uint64_t internal_vertex_id = external_vertex_id +1; // E2I, the vertex ID 0 is reserved, translate all vertex IDs to +1
    if( txn->is_read_only() ){
        interface::Scan</* read only ? */ true> scan(txn, sa, internal_vertex_id, callback);
    } else {
        interface::Scan</* read only ? */ false> scan(txn, sa, internal_vertex_id, callback);
    }
}

} // namespace

