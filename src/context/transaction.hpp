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

#include <cinttypes>

#include "latch.hpp"
#include "undo.hpp"

namespace teseo::internal::context {

class Transaction {
    friend class Undo;

    struct UndoBuffer {
        constexpr static uint64_t BUFFER_SZ = 264192; // 256kb
        char m_buffer[BUFFER_SZ];
        uint64_t m_space_left = BUFFER_SZ;
        UndoBuffer* m_next {nullptr}; // pointer to the next undo log in the chain
    };

    enum class State {
        PENDING,
        COMMITTED,
        ABORTED
    };

    Latch m_latch;
    uint64_t m_transaction_id; // either the startTime or commitTime of the transaction, depending on m_state
    int m_num_undo_todo; // number of undos to still process
    State m_state;
    UndoBuffer* m_undo_last; // pointer to the last undo log in the chain
    UndoBuffer m_undo_buffer; // first undo log in the chain


public:
    Transaction(uint64_t transaction_id);

    // Destructor
    ~Transaction();

    // Get the startTime or commitTime of the transaction
    uint64_t ts_read() const;

    // Get the transactionID or commitTime of the transaction
    uint64_t ts_write() const;

    // Check whether the current transaction terminated
    bool is_terminated() const;

    // Add an undo record
    Undo* add_undo(void* data_structure, Undo* next, UndoType type, uint32_t payload_length, void* payload);

    // Tick the number of undos processed/obsolete
    void tick_undo();
};


}
