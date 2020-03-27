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

using namespace std;
using namespace teseo::internal::context;

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

Transaction Teseo::start_transaction(){
    TransactionImpl* tx_impl = new TransactionImpl(shptr_thread_context(), GCTXT->next_transaction_id());
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
            uint64_t result = TXN->graph_properties().m_vertex_count;
            TXN->latch().validate_version(version);

            return result;
        } catch( teseo::internal::Abort ) { /* retry */ }
    } while(true);
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
