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
#include "thread_context.hpp"
#include "transaction_impl.hpp"

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

Undo::Undo(TransactionImpl* tx, TransactionRollbackImpl* data_structure, Undo* next, uint32_t length) : m_transaction(tx), m_data_structure(data_structure), m_next(next), m_length_payload(length) {

}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

TransactionImpl* Undo::transaction() { return m_transaction; }
const TransactionImpl* Undo::transaction() const { return m_transaction; }

uint64_t Undo::transaction_id() const {
    return m_transaction->ts_write();
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

void Undo::rollback() {
    m_data_structure->do_rollback(payload(), next());
    transaction()->decr_system_count();
}

std::pair<Undo*, uint64_t> Undo::prune(Undo* head, const TransactionSequence* sequence){
    assert(sequence != nullptr && "The sequence cannot be a nullptr");
    assert(thread_context()->epoch() && "Because this method involves a managed object of the GC (the argument `sequence'), it needs to invoked inside an epoch");

    // Is this chain empty?
    using result_t = std::pair<Undo*, uint64_t>;
    if(head == nullptr) return result_t(nullptr, 0);

    TransactionSequenceIterator A(sequence);
    assert(!A.done() && "At least the sequence should contain the transaction that firstly created the active tx list");
    while(!A.done() && A.key() >= head->transaction_id()) A.next();

    if(A.done()){
        clear(head);
        return result_t(nullptr, 0);
    }

    Undo* parent = head;
    uint64_t length = 0; // the length of the chain
    do {
        Undo* child = parent->next();
        length++;

        while( child != nullptr && (A.done() || (A.key() < child->transaction_id())) ){
            assert(child->transaction()->is_terminated() && "Only the head of the chain can belong to a transaction still running");

            // Okay, we don't need a latch on the assumption that the method #prune is invoked
            // by a transaction holding an xlock to the segment in sparse array where head is present
            parent->m_next = child->m_next;

            child->transaction()->decr_system_count();

            child = parent->m_next;
        }

        parent = child;

        if(parent != nullptr){
            while(!A.done() && A.key() >= parent->transaction_id()) A.next();
        }
    } while(parent != nullptr);

    return result_t(head, length);
}

std::pair<Undo*, uint64_t> Undo::prune(Undo* head, uint64_t high_water_mark) {
    // Is this chain empty?
    using result_t = std::pair<Undo*, uint64_t>;
    if(head == nullptr || high_water_mark >= head->transaction_id()){
        clear(head);
        return result_t(nullptr, 0);
    } else {
        uint64_t length = 1;
        Undo* parent = head;
        Undo* child = parent->next();

        while(child != nullptr){
            if(child->transaction_id() >= high_water_mark){
                parent = child;
                child = parent->next();
                length++;
            } else { // we're done!
                parent->m_next = nullptr;
                clear(child);
                child = nullptr;
            }
        }

        return result_t(head, length);
    }
}


void Undo::clear(Undo* head){
    while(head != nullptr){
        Undo* child = head->next();
        head->transaction()->decr_system_count();
        head = child;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

string Undo::to_string() const {
    stringstream ss;
    ss << "UNDO (" << (void*) this << "), "
            "transaction r=" << transaction()->ts_read() << " w= " << transaction()->ts_write() << ", "
            "data structure: " << m_data_structure << ", payload length: " << length() << ", next: " << next() << ", ";
    return ss.str();
}

void Undo::dump() const {
    cout << to_string() << "\n";
}

void Undo::dump_chain(Undo* u, int prefix_blank_spaces){
    int index = 1;
    while(u != nullptr){
        for(int i = 0; i < prefix_blank_spaces; i++) cout << " ";
        cout << index << ". " << u->to_string() << "\n";

        // next iteration
        u = u->next();
        index++;
    }
}

} // namespace
