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

#include "undo.hpp"

#include <iostream>
#include <mutex>

#include "memstore/sparse_array.hpp"
#include "transaction.hpp"

using namespace std;

namespace teseo::internal::context {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[Undo::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Init                                                                    *
 *                                                                           *
 *****************************************************************************/

Undo::Undo(Transaction* tx, void* data_structure, Undo* next, UndoType type, uint32_t length) : m_transaction(tx), m_data_structure(data_structure), m_next(next), m_type(type), m_flags(0), m_length_payload(length) {
    set_flag(UndoFlag::UNDO_FIRST);

    if(next != nullptr){ // make a backward pointer
        Transaction* tx_previous = next->transaction();
        assert(tx != tx_previous && "The next undo record must belong to a different transaction, it's to simplify the lock mngmnt");
        assert(tx_previous->is_terminated() && "Overwriting an item currently locked");

        tx_previous->m_latch.lock_write();
        next->set_flag(UndoFlag::UNDO_FIRST, false);
        next->m_previous = this;
        tx_previous->m_latch.unlock_write();
    }
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

Transaction* Undo::transaction() { return m_transaction; }
const Transaction* Undo::transaction() const { return m_transaction; }

uint64_t Undo::transaction_id() const {
    return m_transaction->ts_write();
}

void Undo::set_flag(UndoFlag flag, bool value) {
    if(value){
        m_flags |= flag;
    } else {
        m_flags &= ~flag;
    }
}

bool Undo::has_flag(UndoFlag flag) const {
    return m_flags & flag;
}

uint64_t Undo::length() const {
    return sizeof(Undo) + m_length_payload;
}

void* Undo::payload() const {
    return reinterpret_cast<void*>(this + sizeof(Undo));
}

/*****************************************************************************
 *                                                                           *
 *   Processing                                                              *
 *                                                                           *
 *****************************************************************************/

void Undo::mark_chain_obsolete(){
    Undo* undo = this;
    do {
        auto tx = undo->transaction();
        tx->m_latch.lock_write();
        Undo* next = undo->m_next;
        if(!undo->has_flag(UNDO_PROCESSED)){
            undo->set_flag(UNDO_PROCESSED, true);
            tx->tick_undo(); // ref counter on the number of undo to still process
        }
        undo->m_previous = nullptr;
        undo->m_next = nullptr;
        tx->m_latch.unlock_write();

        undo = next;
    } while (undo != nullptr);
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void Undo::dump() const {
    cout << "UNDO ";
    switch(m_type){
    case UndoType::SparseArrayUpdate:
        cout << "SparseArray";
        break;
    default:
        cout << "Unknown (" << reinterpret_cast<int>(m_type) << ")";
    }
    if(has_flag(UndoFlag::UNDO_FIRST)){
        cout << ", data structure: " << m_data_structure;
    } else {
        cout << ", previous: " << m_previous;
    }
    cout << ", next: " << m_next;

    if(has_flag(UndoFlag::UNDO_PROCESSED)){
        cout << ", PROCESSED";
    }

    if(m_type == UndoType::SparseArrayUpdate){
        void* data_structure = nullptr; const Undo* u = this;
        while(data_structure == nullptr && u->m_previous != nullptr){
            if(u->has_flag(UndoFlag::UNDO_FIRST)){
                data_structure = u->m_data_structure;
            } else {
                u = u->m_previous;
            }
        }

        cout << ", payload: {";
        reinterpret_cast<teseo::internal::memstore::SparseArray*>(data_structure)->dump_undo(payload());
        cout << "}";
    }
}

void Undo::dump_chain(int prefix_blank_spaces) const {
    int index = 1;
    const Undo* u = this;
    do {
        for(int i = 0; i < prefix_blank_spaces; i++) cout << " ";
        cout << index << ". ";
        u->dump();

        u = u->m_next;
        index++;
    } while (u != nullptr);
}



} // namespace
