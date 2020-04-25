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

#include "teseo/transaction/memory_pool.hpp"

#include "teseo/context/static_configuration.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo_buffer.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

#include <cassert>
#include <cstdlib>
#include <mutex>
#include <stdexcept>

using namespace std;

namespace teseo::transaction {

MemoryPool* MemoryPool::create(){
    uint64_t space_total = sizeof(MemoryPool) + sizeof(uint16_t) * capacity() + entry_size() * capacity();
    void* ptr = malloc(space_total);
    if(ptr == nullptr) throw bad_alloc{};
    return new (ptr) MemoryPool();
}

void MemoryPool::destroy(MemoryPool* mempool){
    if(mempool != nullptr){
        mempool->~MemoryPool();
        free(mempool);
    }
}

MemoryPool::MemoryPool() : m_next(capacity()){
    uint16_t* __restrict free_slots = array_free_slots();
    for(uint16_t i = 0, sz = capacity(); i < sz; i++){ free_slots[i] = sz - i -1; }
}

MemoryPool::~MemoryPool(){
    assert(is_empty() && "Some transactions are still in use");
}

uint16_t* MemoryPool::array_free_slots(){
    return reinterpret_cast<uint16_t*>(this + 1);
}

uint8_t* MemoryPool::buffer(){
    return reinterpret_cast<uint8_t*>(array_free_slots() + capacity());
}

bool MemoryPool::is_full() const{
    return m_next == 0;
}

bool MemoryPool::is_empty() const{
    return m_next == capacity();
}

double MemoryPool::fill_factor() const {
    return 1.0 - static_cast<double>(m_next) / capacity();
}

uint64_t MemoryPool::capacity() {
    return context::StaticConfiguration::transaction_memory_pool_size;
}

uint64_t MemoryPool::entry_size(){
    return sizeof(MemoryPool*) + sizeof(TransactionImpl) + UndoBuffer::undobuffer_sz(context::StaticConfiguration::transaction_undo_embedded_size);
}

TransactionImpl* MemoryPool::create_transaction(std::shared_ptr<context::ThreadContext> thread_context, bool read_only){
    uint64_t slotno = 0;

    {
        scoped_lock<util::SpinLock> lock(m_latch);
        if(is_full()) return nullptr;
        slotno = array_free_slots()[--m_next];
    }

    uint64_t* slot = reinterpret_cast<uint64_t*>(buffer() + slotno * entry_size());

    // 1) Save the memory pool instance address, so we can eventually identify the memory pool to deallocated the transaction
    reinterpret_cast<MemoryPool**>(slot)[0] = this;

    // 2) Init the undo buffer
    TransactionImpl* transaction = reinterpret_cast<TransactionImpl*>(slot + 1);
    UndoBuffer* undo_buffer = new (reinterpret_cast<void*>(transaction +1)) UndoBuffer( context::StaticConfiguration::transaction_undo_embedded_size );

    // 3) Init the transaction object
    transaction = new (transaction) TransactionImpl(undo_buffer, thread_context, read_only);

    COUT_DEBUG("memory pool: " << this << ", slot: " << slot << " (" << slotno << "), transaction: " << transaction);

    return transaction;
}

void MemoryPool::destroy_transaction(TransactionImpl* transaction){
    if(transaction == nullptr) return;
    uint64_t* slot = reinterpret_cast<uint64_t*>(transaction) -1;
    MemoryPool* instance = reinterpret_cast<MemoryPool**>(slot)[0];
    instance->do_destroy_transaction(slot);
}

void MemoryPool::do_destroy_transaction(uint64_t* slot_address){
    assert(reinterpret_cast<MemoryPool**>(slot_address)[0] == this);

    TransactionImpl* transaction = reinterpret_cast<TransactionImpl*>(slot_address + 1);
    COUT_DEBUG("memory pool: " << this << ", transaction: " << transaction);
    transaction->~TransactionImpl();
    UndoBuffer* undo_buffer = reinterpret_cast<UndoBuffer*>(transaction +1);
    undo_buffer->~UndoBuffer();

    assert((reinterpret_cast<uint8_t*>(slot_address) - buffer()) % entry_size() == 0 && "Is this a slot inside this memory pool?");
    uint64_t slot_id = (reinterpret_cast<uint8_t*>(slot_address) - buffer()) / entry_size();
    assert(slot_id < capacity());


    scoped_lock<util::SpinLock> lock(m_latch);
    assert(!is_empty() && "If it's empty, no allocations have been made from this pool");
    array_free_slots()[m_next++] = (uint16_t) slot_id;
}



} // namespace





