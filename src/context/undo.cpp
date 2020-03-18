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

        tx_previous->m_undo_latch.lock();
        next->set_flag(UndoFlag::UNDO_FIRST, false);
        next->m_previous = this;
        tx_previous->m_undo_latch.unlock();
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
    return reinterpret_cast<void*>(const_cast<Undo*>(this) + sizeof(Undo));
}

Undo* Undo::next() const {
    return m_next;
}

/*****************************************************************************
 *                                                                           *
 *   Processing                                                              *
 *                                                                           *
 *****************************************************************************/
void Undo::mark_chain_obsolete(Undo* head){
    if(head == nullptr) return; // nop

    Undo* current = head;
    Transaction* tx_current = current->transaction();
    tx_current->m_undo_latch.lock();

    do {
        current->do_ignore();

        Undo* next = current->m_next;
        Transaction* tx_next { nullptr };
        if(next != nullptr){ // lock coupling
            tx_next = next->transaction();
            tx_next->m_undo_latch.lock();
        }

        tx_current->m_undo_latch.unlock();

        current = next;
        tx_current = tx_next;
    } while (current != nullptr);
}

void Undo::mark_first(void* data_structure){
    lock_guard<OptimisticLatch<0>> xlock(transaction()->m_undo_latch);
    set_flag(UNDO_FIRST);
    m_data_structure = data_structure;
}

void Undo::ignore(){
    lock_guard<OptimisticLatch<0>> xlock(transaction()->m_undo_latch);
    do_ignore();
}

void Undo::do_ignore(){
    assert(!has_flag(UNDO_REVERTED)); // already reverted / ignored

    m_previous = m_next = nullptr;
    set_flag(UNDO_FIRST, false);
    set_flag(UNDO_REVERTED, true);
    transaction()->tick_undo();
}

void Undo::rollback(){
    assert(transaction()->m_undo_latch.is_locked() && "The undo latch should be locked");

    if(has_flag(UNDO_REVERTED)) return; // already processed
    assert(has_flag(UNDO_FIRST) && "Otherwise the transaction associated to this undo entry should have already been terminated");

    switch(m_type){
    case UndoType::SparseArrayUpdate: {
        auto sparse_array = reinterpret_cast<teseo::internal::memstore::SparseArray*>(m_data_structure);
        sparse_array->process_undo(payload(), m_next);
    } break;
    default:
        assert(0 && "Case not treated");
    }

    m_previous = m_next = nullptr;
    set_flag(UNDO_FIRST, false);
    set_flag(UNDO_REVERTED, true);
    transaction()->tick_undo();
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
        cout << "Unknown (" << static_cast<int>(m_type) << ")";
    }
    if(has_flag(UndoFlag::UNDO_FIRST)){
        cout << ", data structure: " << m_data_structure;
    } else {
        cout << ", previous: " << m_previous;
    }
    cout << ", next: " << m_next;

    if(has_flag(UndoFlag::UNDO_REVERTED)){
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
