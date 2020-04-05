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
#include <mutex>

#include "context/global_context.hpp"
#include "context/scoped_epoch.hpp"
#include "context/thread_context.hpp"
#include "context/transaction_impl.hpp"
#include "memstore/sparse_array.hpp"
#include "latch.hpp"

using namespace std;
using namespace teseo::internal;
using namespace teseo::internal::context;
using namespace teseo::internal::memstore;

// perform the proper cast to the global context
#define GCTXT reinterpret_cast<teseo::internal::context::GlobalContext*>(m_pImpl)

// perform the proprt cast to a transaction impl
#define TXN reinterpret_cast<teseo::internal::context::TransactionImpl*>(m_pImpl)

namespace teseo {

/*****************************************************************************
 *                                                                           *
 *  Debug                                                                    *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_CLASS "?"
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[" << COUT_DEBUG_CLASS << "::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *  Global context                                                           *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_CLASS
#define COUT_DEBUG_CLASS "Teseo"

bool g_teseo_test = false;

Teseo::Teseo() : m_pImpl(new GlobalContext()) {

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
    TransactionImpl* tx_impl = new TransactionImpl(shptr_thread_context(), read_only);
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
#undef COUT_DEBUG_CLASS
#define COUT_DEBUG_CLASS "Transaction"

#define CHECK_NOT_READ_ONLY if(TXN->is_read_only()){ RAISE_EXCEPTION(LogicalError, "Operation not allowed: the transaction is read only"); }
#define CHECK_NOT_TERMINATED if(TXN->is_terminated()) { RAISE_EXCEPTION(LogicalError, "Transaction already terminated"); }

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
        assert(m_pImpl != nullptr && "How was this txn wrapper initialised in the first place?");
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
    do {
        ScopedEpoch epoch;

        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            uint64_t result = TXN->graph_properties().m_edge_count;
            TXN->latch().validate_version(version);

            return result;
        } catch( teseo::internal::Abort ) { /* retry */ }
    } while(true);
}

uint64_t Transaction::num_vertices() const {
    do {
        ScopedEpoch epoch;

        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            uint64_t result = TXN->graph_properties().m_vertex_count;
            TXN->latch().validate_version(version);

            return result;
        } catch( teseo::internal::Abort ) { /* retry */ }
    } while(true);
}


void Transaction::insert_vertex(uint64_t vertex){
    CHECK_NOT_READ_ONLY

    lock_guard<OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    SparseArray* sa = global_context()->storage();
    sa->insert_vertex(TXN, vertex);

    TXN->local_graph_changes().m_vertex_count++;
}

bool Transaction::has_vertex(uint64_t vertex) const{
    SparseArray* sa = global_context()->storage();

    do {
        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            bool result = sa->has_vertex(TXN, vertex);
            TXN->latch().validate_version(version);

            return result;
        } catch( teseo::internal::Abort ) { /* retry */ }
    } while(true);
}

void Transaction::insert_edge(uint64_t source, uint64_t destination, double weight){
    CHECK_NOT_READ_ONLY

    lock_guard<OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    SparseArray* sa = global_context()->storage();
    sa->insert_edge(TXN, source, destination, weight);

    TXN->local_graph_changes().m_edge_count++;
}

bool Transaction::has_edge(uint64_t source, uint64_t destination) const {
    SparseArray* sa = global_context()->storage();

    do {
        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            bool result = sa->has_edge(TXN, source, destination);
            TXN->latch().validate_version(version);

            return result;
        } catch( teseo::internal::Abort ) { /* retry */ }
    } while(true);
}

double Transaction::get_weight(uint64_t source, uint64_t destination) const {
    SparseArray* sa = global_context()->storage();

    do {
        try {
            uint64_t version = TXN->latch().read_version();
            CHECK_NOT_TERMINATED
            double result = sa->get_weight(TXN, source, destination);
            TXN->latch().validate_version(version);

            return result;
        } catch( teseo::internal::Abort ) { /* retry */ }
    } while(true);
}

void Transaction::remove_edge(uint64_t source, uint64_t destination){
    CHECK_NOT_READ_ONLY

    lock_guard<OptimisticLatch<0>> lock(TXN->latch());
    CHECK_NOT_TERMINATED

    SparseArray* sa = global_context()->storage();
    sa->remove_edge(TXN, source, destination);

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


}
