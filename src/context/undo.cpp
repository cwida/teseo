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
#include <sstream>
#include <string>

#include "memstore/sparse_array.hpp"
#include "profiler/scoped_timer.hpp"
#include "util/miscellaneous.hpp"
#include "global_context.hpp"
#include "thread_context.hpp"
#include "transaction_impl.hpp"

using namespace std;
using namespace teseo::internal::util;

namespace teseo::internal::context {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
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
    return reinterpret_cast<void*>(const_cast<Undo*>(this) + 1);
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
    profiler::ScopedTimer profiler { profiler::UNDO_PRUNE_AT };
    assert(sequence != nullptr && "The sequence cannot be a nullptr");
    assert(thread_context()->epoch() && "Because this method involves a managed object of the GC (the argument `sequence'), it needs to invoked inside an epoch");

    // Is this chain empty?
    using result_t = std::pair<Undo*, uint64_t>;
    if(head == nullptr) return result_t(nullptr, 0);

    TransactionSequenceForwardIterator A(sequence);
    assert(!A.done() && "At least it should contain one transaction ID in the sequence");

    //COUT_DEBUG("head: " << head << ", sequence: " << *sequence << ", payload: " << *((teseo::internal::memstore::SparseArray::Update*) head->payload()));

    // Step 1: we want to skip all undo records that either:
    // a) belong to a pending transaction. This transaction did not commit yet and may roll back its changes.
    // b) belong to an undo newer than the first tx in `sequence'. The transaction list could be outdated and
    //    we cannot be sure whether undo records newer than first(sequence) are in use by newer transactions.
    Undo* parent = nullptr;
    Undo* child = head;
    uint64_t length = 0; // the length of the chain
    while(child != nullptr && child->transaction_id() >= A.key()) {
        // NB: there is no need to explicitly check on !child->transaction()->is_terminated(). If the undo
        // belong to a non terminated transaction, its transaction_id() is #ts_write, a value higher than
        // any startTime or commitTime.

        parent = child;
        child = child->m_next;
        length++;
    }
    // have we exhausted the chain?
    if(child == nullptr) return result_t(head, length);

    // Step 2: the list of undos [head, ..., parent] must survive because of step 1. Before processing `child',
    // we wonder whether there are any tx older than child
    while(!A.done() && A.key() >= child->transaction_id()) A.next();
    if(A.done()){ // if all tx are newer than child => the surviving undos are [head, ..., parent]
        clear(child);
        if(parent != nullptr){
            parent->m_next = nullptr;
            return result_t(head, length);
        } else {
            assert(head == child);
            return result_t(nullptr, 0);
        }
    }

    // Step 3: we want to remove unused undo records for the transactions intermediate in A. The idea is to keep
    // three pointers around:
    // a) parent is the last undo record we know must be mantained
    // b) current is the candidate undo that contains the information visible by A.key()
    // c) child is current's child and is the undo that contains the information to know if current is visible by A.key
    Undo* current = parent;
    do {
        assert(!A.done() && "Otherwise it shouldn't have entered this loop");
        while( child != nullptr && child->transaction_id() > A.key()  ){
            assert(child->transaction()->is_terminated() && "This case should have already been account by Step 1 in the code above");
            if(current != parent){ current->transaction()->decr_system_count(); }

            // Okay, we don't need a latch on the assumption that the method #prune is invoked
            // by a transaction holding an xlock to the segment in the sparse array where the head is present
            current = child;
            child = child->m_next;
        }

        // insert `current' in the chain of surviving undo records: [head, ...., parent, current]
        if(parent != current){
            if(parent == nullptr){ // corner case: replace the head
                head = current;
            } else { // standard case: remove the undo records in between
                parent->m_next = current;
            }

            parent = current;
            length++;
        }

        // again, did we exhaust the chain of undo records?
        if(child == nullptr) return result_t(head, length); // we're done

        // move the iterator forward
        do { A.next(); } while( !A.done() && A.key() >= child->transaction_id() );

        // next iteration
        current = child;
        child = child->m_next;
    } while(!A.done());

    // Step 4: as we depleted the list of active transactions, everything that comes after [head, ..., parent] is
    // unnecessary and can be safely removed:
    clear(parent->m_next);
    parent->m_next = nullptr;
    return result_t(head, length);
}

std::pair<Undo*, uint64_t> Undo::prune(Undo* head, uint64_t high_water_mark) {
    profiler::ScopedTimer profiler { profiler::UNDO_PRUNE_HWM };

    // Is this chain empty?
    using result_t = std::pair<Undo*, uint64_t>;
    if(head == nullptr) return result_t(nullptr, 0);

    // Note: the condition used to be: (!current->transaction()->is_terminated() || current->transaction_id() >= high_water_mark))
    // I don't the above is still necessary, because if the TS is not terminated, then high_water_mark will be less or equal than TS anyway

    // skip the undo records for the transactions still in flight
    Undo* parent = nullptr;
    Undo* child = head;
    uint64_t length = 0;
    while(child != nullptr && child->transaction_id() >= high_water_mark){
        parent = child;
        child = child->m_next;
        length++;
    }

    clear(child);
    if(parent != nullptr){
        parent->m_next = nullptr;
        return result_t(head, length);
    } else {
        assert(head == child);
        return result_t(nullptr, 0);
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
            "data structure: " << m_data_structure << ", payload length: " << length() << ", " <<
            "payload: " << m_data_structure->str_undo_payload(payload()) << ", next: " << next();
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
