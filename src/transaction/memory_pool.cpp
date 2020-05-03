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

#include <cassert>
#include <cstdlib>
#include <mutex>
#include <stdexcept>

#include "teseo/context/static_configuration.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo_buffer.hpp"
#include "teseo/util/compiler.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::transaction {

MemoryPool* MemoryPool::create(){
    uint64_t space_total = sizeof(MemoryPool) + sizeof(uint32_t) * capacity() + entry_size() * capacity();
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
    // create the free list
    uint32_t* __restrict free_slots = array_free_slots();
    for(uint32_t i = 0, sz = capacity(); i < sz; i++){ free_slots[i] = sz - i -1; }

    // mark the locations pointer in the free list as null
    for(uint64_t i = 0; i < capacity(); i++){
        uint64_t* slot = reinterpret_cast<uint64_t*>(buffer() + i * entry_size());
        *slot = 0;
    }
}

MemoryPool::~MemoryPool(){

}

uint32_t* MemoryPool::array_free_slots(){
    return reinterpret_cast<uint32_t*>(this + 1);
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

TransactionImpl* MemoryPool::create_transaction(context::GlobalContext* global_context, bool read_only){
    if(is_full()) return nullptr;

    uint64_t slotno = array_free_slots()[--m_next];
    uint64_t* slot = reinterpret_cast<uint64_t*>(buffer() + slotno * entry_size());

    // 1) Save the memory pool instance address, so we can eventually identify the memory pool to deallocated the transaction
    reinterpret_cast<MemoryPool**>(slot)[0] = this;

    // 2) Init the undo buffer
    TransactionImpl* transaction = reinterpret_cast<TransactionImpl*>(slot + 1);
    UndoBuffer* undo_buffer = new (reinterpret_cast<void*>(transaction +1)) UndoBuffer( context::StaticConfiguration::transaction_undo_embedded_size );

    // 3) Init the transaction object
    transaction = new (transaction) TransactionImpl(undo_buffer, global_context, read_only);

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
    assert((reinterpret_cast<uint8_t*>(slot_address) - buffer()) % entry_size() == 0 && "Is this a slot inside this memory pool?");

    TransactionImpl* transaction = reinterpret_cast<TransactionImpl*>(slot_address + 1);
    COUT_DEBUG("memory pool: " << this << ", transaction: " << transaction);
    transaction->~TransactionImpl();
    UndoBuffer* undo_buffer = reinterpret_cast<UndoBuffer*>(transaction +1);
    undo_buffer->~UndoBuffer();

    util::compiler_barrier();
    *slot_address = 0;
}

void MemoryPool::rebuild_free_list(){
    m_next = 0;

    // mark the locations pointer in the free list as null
    for(uint64_t i = 0; i < capacity(); i++){
        uint64_t* slot = reinterpret_cast<uint64_t*>(buffer() + i * entry_size());
        if(*slot == 0){ // this slot is free
            array_free_slots()[m_next] = i;
            m_next++;
        }
    }
}

} // namespace





