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

#include "transaction.hpp"
#include "utility.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>

using namespace std;

namespace teseo::internal::context {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[Transaction::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
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
Transaction::Transaction(uint64_t transaction_id) : m_transaction_id(transaction_id), m_num_undo_todo(0), m_state(State::PENDING), m_undo_last(nullptr){
    m_undo_last = &m_undo_buffer;
}

Transaction::~Transaction(){

    // release all undo buffers acquired
    UndoBuffer* buffer = m_undo_last;
    while(buffer != &m_undo_buffer){
        UndoBuffer* next = buffer->m_next;
        delete buffer;
        buffer = next;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

uint64_t Transaction::ts_read() const {
    return m_transaction_id;
}

uint64_t Transaction::ts_write() const {
    switch(m_state){
    case State::PENDING:
        return m_transaction_id + (numeric_limits<uint64_t>::max()>>1);
    default:
        return m_transaction_id;
    }
}

bool Transaction::is_terminated() const {
    return m_state != State::PENDING;
}

/*****************************************************************************
 *                                                                           *
 *   Undo                                                                    *
 *                                                                           *
 *****************************************************************************/

Undo* Transaction::add_undo(void* data_structure, Undo* next, UndoType type, uint32_t payload_length, void* payload) {
    uint64_t total_length = sizeof(Undo) + payload_length;
    assert(total_length <= UndoBuffer::BUFFER_SZ && "This entry won't fit any undo buffer");

    UndoBuffer* undo_buffer = m_undo_last;
    if(undo_buffer->m_space_left < total_length){
        undo_buffer = new UndoBuffer();
        undo_buffer->m_next = m_undo_last;
        m_undo_last = undo_buffer;
    }

    void* ptr = undo_buffer->m_buffer + undo_buffer->m_space_left - total_length;
    undo_buffer->m_space_left -= total_length;

    // Init the undo record
    Undo* undo = new (ptr) Undo(this, data_structure, next, type, payload_length);
    memcpy(ptr + sizeof(Undo), payload, payload_length);

    m_num_undo_todo++;

    return undo;
}

void Transaction::tick_undo(){
    assert(m_num_undo_todo > 0 && "Underflow");
    m_num_undo_todo--;
}

} // namespace


