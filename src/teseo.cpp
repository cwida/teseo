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

#include "teseo.hpp"

#include <cassert>
#include <cinttypes>
#include <mutex>
#include <string>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/memstore.hpp"

#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/error.hpp"

using namespace std;

// perform the proper cast to the global context
#define GCTXT reinterpret_cast<context::GlobalContext*>(m_pImpl)

// perform the proprt cast to a transaction impl
#define TXN reinterpret_cast<transaction::TransactionImpl*>(m_pImpl)

// The vertex ID 0 is reserved to avoid the confusion of the key <42, 0> in the Index, both referring
// to the vertex 42 and the edge 42 -> 0
static uint64_t E2I(uint64_t e){ return e +1; };

namespace teseo {

/*****************************************************************************
 *                                                                           *
 *  Global context                                                           *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_CLASS
#define COUT_DEBUG_CLASS "Teseo"

bool g_teseo_test = false;

Teseo::Teseo() : m_pImpl(new context::GlobalContext()) {

}

Teseo::~Teseo(){
    delete GCTXT; m_pImpl = nullptr;
}

void Teseo::register_thread(){
    GCTXT->register_thread();
}

void Teseo::unregister_thread(){
    GCTXT->unregister_thread();
}

Transaction Teseo::start_transaction(bool read_only){
    profiler::ScopedTimer profiler { profiler::TESEO_START_TRANSACTION };

    transaction::TransactionImpl* tx_impl = context::thread_context()->create_transaction(read_only);
    return Transaction(tx_impl);
}

void* Teseo::handle_impl(){
    return m_pImpl;
}

/*****************************************************************************
 *                                                                           *
 * Transaction                                                               *
 *                                                                           *
 *****************************************************************************/

#define CHECK_NOT_READ_ONLY if(TXN->is_read_only()){ RAISE_EXCEPTION(LogicalError, "Operation not allowed: the transaction is read only"); }
#define CHECK_NOT_TERMINATED if(TXN->is_terminated()) { RAISE_EXCEPTION(LogicalError, "Transaction already terminated"); }
static void handle_error(const memstore::Error& error);



Transaction::Transaction(void* tx_impl): m_pImpl(tx_impl){
    TXN->incr_user_count();
}

Transaction::Transaction(const Transaction& copy) : m_pImpl(copy.m_pImpl){
    if(m_pImpl != nullptr){
        TXN->incr_user_count();
    }
}

Transaction& Transaction::operator=(const Transaction& copy){
    if(this != &copy){
        assert(m_pImpl != nullptr && "How was this txn wrapper initialised in the first place?");
        if(m_pImpl != nullptr){ TXN->decr_user_count(); }
        TXN->decr_user_count();
        m_pImpl = copy.m_pImpl;
        if(m_pImpl != nullptr){ TXN->incr_user_count(); }
    }
    return *this;
}

Transaction::Transaction(Transaction&& move) : m_pImpl(move.m_pImpl) {
    move.m_pImpl = nullptr;
}

Transaction& Transaction::operator=(Transaction&& move){
    if(this != &move){
        //assert(m_pImpl != nullptr && "How was this txn wrapper initialised in the first place?");
        if(m_pImpl != nullptr){ TXN->decr_user_count(); }
        m_pImpl = move.m_pImpl;
        move.m_pImpl = nullptr;
    }

    return *this;
}

Transaction::~Transaction() {
    if(m_pImpl != nullptr){ // <= it may have been unset by a move ctor/assignment
        TXN->decr_user_count();
        m_pImpl = nullptr; // use the ref count mechanism to release the impl memory
    }
}

uint64_t Transaction::num_edges() const {
    profiler::ScopedTimer profiler { profiler::TESEO_NUM_EDGES };

    do {
        context::ScopedEpoch epoch;

        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            uint64_t result = TXN->graph_properties().m_edge_count;
            TXN->latch().validate_version(version);

            return result;
        } catch( Abort ) { /* retry */ }
    } while(true);
}

uint64_t Transaction::num_vertices() const {
    profiler::ScopedTimer profiler { profiler::TESEO_NUM_VERTICES };

    do {
        context::ScopedEpoch epoch;

        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            uint64_t result = TXN->graph_properties().m_vertex_count;
            TXN->latch().validate_version(version);

            return result;
        } catch( Abort ) { /* retry */ }
    } while(true);
}


void Transaction::insert_vertex(uint64_t vertex){
    profiler::ScopedTimer profiler { profiler::TESEO_INSERT_VERTEX };

    CHECK_NOT_READ_ONLY

    lock_guard<util::OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    memstore::Memstore* sa = context::global_context()->memstore();

    try {
        sa->insert_vertex(TXN, E2I(vertex)); // 0 -> 1, 1 -> 2, so on...
    } catch(const memstore::Error& e){
        handle_error(e);
    }

    TXN->local_graph_changes().m_vertex_count++;
}

bool Transaction::has_vertex(uint64_t vertex) const {
    profiler::ScopedTimer profiler { profiler::TESEO_HAS_VERTEX };

    memstore::Memstore* sa = context::global_context()->memstore();

    do {
        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            bool result = sa->has_vertex(TXN, E2I(vertex));
            TXN->latch().validate_version(version);

            return result;
        } catch( Abort ) { /* retry */ }
    } while(true);
}


uint64_t Transaction::remove_vertex(uint64_t vertex){
    profiler::ScopedTimer profiler { profiler::TESEO_REMOVE_VERTEX };

    CHECK_NOT_READ_ONLY

    lock_guard<util::OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    memstore::Memstore* sa = context::global_context()->memstore();
    uint64_t num_removed_edges = 0;
    try {
        num_removed_edges = sa->remove_vertex(TXN, E2I(vertex));
    } catch(const memstore::Error& error){
        handle_error(error);
    }

    TXN->local_graph_changes().m_vertex_count --;
    TXN->local_graph_changes().m_edge_count -= num_removed_edges;

    return num_removed_edges;
}

void Transaction::insert_edge(uint64_t source, uint64_t destination, double weight){
    profiler::ScopedTimer profiler { profiler::TESEO_INSERT_EDGE };

    CHECK_NOT_READ_ONLY

    lock_guard<util::OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    memstore::Memstore* sa = context::global_context()->memstore();
    try {
        sa->insert_edge(TXN, E2I(source), E2I(destination), weight);
    } catch(const memstore::Error& error){
        handle_error(error);
    }

    TXN->local_graph_changes().m_edge_count++;
}

bool Transaction::has_edge(uint64_t source, uint64_t destination) const {
    profiler::ScopedTimer profiler { profiler::TESEO_HAS_EDGE };

    memstore::Memstore* sa = context::global_context()->memstore();

    do {
        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            bool result = sa->has_edge(TXN, E2I(source), E2I(destination));
            TXN->latch().validate_version(version);

            return result;
        } catch( Abort ) {
            /* nop, retry... */
        } catch( const memstore::Error& error ) {
            handle_error(error);
        }
    } while(true);
}

double Transaction::get_weight(uint64_t source, uint64_t destination) const {
    profiler::ScopedTimer profiler { profiler::TESEO_GET_WEIGHT };

    memstore::Memstore* sa = context::global_context()->memstore();

    do {
        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            double result = sa->get_weight(TXN, E2I(source), E2I(destination));
            TXN->latch().validate_version(version);

            return result;
        } catch( Abort ) {
            /* nop, retry... */
        } catch( const memstore::Error& error ) {
            handle_error(error);
        }
    } while(true);
}

void Transaction::remove_edge(uint64_t source, uint64_t destination){
    profiler::ScopedTimer profiler { profiler::TESEO_REMOVE_EDGE };

    CHECK_NOT_READ_ONLY

    lock_guard<util::OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    memstore::Memstore* sa = context::global_context()->memstore();
    try {
        sa->remove_edge(TXN, E2I(source), E2I(destination));
    } catch(const memstore::Error& error){
        handle_error(error);
    }

    TXN->local_graph_changes().m_edge_count--;
}

bool Transaction::is_read_only() const {
    return TXN->is_read_only();
}

void Transaction::commit(){
    TXN->commit();
}

void Transaction::rollback() {
    TXN->rollback();
}

void* Transaction::handle_impl() {
    return m_pImpl;
}

static void handle_error(const memstore::Error& error){
    using namespace memstore;

    const uint64_t source = error.m_key.source() -1; // external vertex 0 -> internal vertex 1
    const uint64_t destination = error.m_key.destination() -1; // external vertex 0 -> internal vertex 1

    switch(error.m_type){
    case Error::VertexLocked:
        RAISE(TransactionConflict, "Conflict detected, the vertex " << source << " is currently locked by another transaction. "
                "Restart this transaction to alter this object");
    case Error::VertexAlreadyExists:
        RAISE(LogicalError, "The vertex" << source << " already exists");
    case Error::VertexDoesNotExist:
        RAISE(LogicalError, "The vertex " << source << " does not exist");
    case Error::VertexPhantomWrite:
        RAISE(TransactionConflict, "Conflict detected, phantom write detected for the vertex " << source);
    case Error::EdgeLocked:
        RAISE(TransactionConflict, "Conflict detected, the edge " << source << " -> " << destination << " is currently locked "
                "by another transaction. Restart this transaction to alter this object");
    case Error::EdgeAlreadyExists:
        RAISE(LogicalError, "The edge " << source << " -> " << destination << " already exists");
    case Error::EdgeDoesNotExist:
        RAISE(LogicalError, "The edge " << source << " -> " << destination << " does not exist");
    default:
        RAISE(InternalError, "Error type not registered");
    }
}


}
