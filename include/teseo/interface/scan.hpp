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

template<typename Callback>
class Scan {
    bool m_vertex_found = false;
    const uint64_t m_vertex_id;
    const Callback& m_callback;

private:

    // Execute the iterator
    void do_scan(transaction::TransactionImpl* txn, memstore::Memstore* sa);

public:
    Scan(transaction::TransactionImpl* txn, memstore::Memstore* sa, uint64_t vertex_id, const Callback& callback) : m_vertex_id(vertex_id), m_callback(callback){
        do_scan(txn, sa);
    }

    Scan(const Scan& scan) : m_vertex_id(scan.m_vertex_id), m_callback(scan.m_callback) {
        COUT_DEBUG("copy ctor: " << &scan << " -> " << this);
    }

    Scan& operator=(const Scan& scan)  {
        m_vertex_id = scan.m_vertex_id;
        m_callback = scan.m_callback;
        COUT_DEBUG("copy assignment: " << &scan << " -> " << this);
        return *this;
    }

    Scan(Scan&& scan) : m_vertex_id(scan.m_vertex_id), m_callback(scan.m_callback) {
        COUT_DEBUG("move ctor: " << &scan << " -> " << this);
    }

    Scan& operator=(Scan&& scan)  {
        m_vertex_id = scan.m_vertex_id;
        m_callback = scan.m_callback;
        COUT_DEBUG("move assignment: " << &scan << " -> " << this);
        return *this;
    }

    // Trampoline to the user callback
    bool operator()(uint64_t source, uint64_t destination, double weight);
};



template<typename Callback>
void Scan<Callback>::do_scan(transaction::TransactionImpl* txn, memstore::Memstore* sa){
    COUT_DEBUG("this: " << this);

    try {
        if(txn->is_read_only()){
            sa->scan(txn, m_vertex_id, *this);
        } else {
            sa->scan_nolock(txn, m_vertex_id, *this);
        }

        if(!m_vertex_found){
            throw memstore::Error { memstore::Key { m_vertex_id }, memstore::Error::Type::VertexDoesNotExist };
        }
    } catch( const memstore::Error& error ){
        util::handle_error(error);
    }
};

template<typename Callback>
bool Scan<Callback>::operator()(uint64_t source, uint64_t destination, double weight){
    COUT_DEBUG("this: " << this << ", source: " << source << ", destination: " << destination << ", weight: " << weight);

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
void Transaction::scan_out(uint64_t external_vertex_id, Callback&& callback) const{
    transaction::TransactionImpl* txn = reinterpret_cast<transaction::TransactionImpl*>(m_pImpl);
    memstore::Memstore* sa = context::global_context()->memstore();
    uint64_t internal_vertex_id = external_vertex_id +1; // E2I, the vertex ID 0 is reserved, translate all vertex IDs to +1
    interface::Scan scan(txn, sa, internal_vertex_id, callback);
}

} // namespace

