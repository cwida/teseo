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

#include "teseo/memstore/payload_file.hpp"

#include <cassert>
#include <cstdlib> // malloc, free
#include <cstring> // memmove, memcpy
#include <iostream>
#include <limits>
#include <stdexcept>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/payload_iterator.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

PayloadFile::PayloadFile(uint64_t capacity) : m_capacity(capacity), m_cardinality(0), m_empty_lhs(0), m_empty_rhs(m_capacity), m_next(nullptr){
    assert(capacity <= static_cast<uint64_t>(numeric_limits<typeof(m_capacity)>::max()) && "Overflow");
}

PayloadFile::~PayloadFile(){

}

PayloadFile* create_payload_block(){
    return create_payload_block(context::StaticConfiguration::memstore_payload_file_next_block_size);
}

PayloadFile* create_payload_block(uint64_t capacity){
    void* raw = malloc(capacity * sizeof(double) + sizeof(PayloadFile));
    if(raw == nullptr) throw std::bad_alloc{};
    PayloadFile* block = new (raw) PayloadFile(capacity);
    return block;
}

void destroy_payload_block(PayloadFile* block){
    ::free(block);
}

bool PayloadFile::is_full() const {
    return cardinality() >= capacity();
}

void PayloadFile::insert(uint64_t position, double element){
    PayloadFile* block = this;

    // shall we insert the element in this block?
    while(position > block->cardinality()){
        assert(block->m_next != nullptr && "Requested to insert a new element in the next block, which has not been allocated yet");
        position -= block->cardinality();
        block = block->m_next;
    }

    // If this block is full, create a new block and distribute half of the elements in it
    if(block->is_full()){
        block->split();

        // Again, check if we need to insert the element in the next block
        if(position > block->cardinality()){
            position -= block->cardinality();
            block = block->m_next;
        }
    }

    // Insert the element, either in the LHS or in the RHS of the block
    uint64_t relative_position = position;
    if(relative_position <= block->m_empty_lhs){ // insert in the LHS of the block
        block->do_insert_lhs(relative_position, element);
    } else {
        relative_position -= block->m_empty_lhs;
        block->do_insert_rhs(relative_position, element);
    }
}

void PayloadFile::do_insert_lhs(uint64_t relative_position, double element){
    assert(relative_position < static_cast<uint64_t>(numeric_limits<typeof(m_capacity)>::max()) && "Overflow");
    assert(relative_position <= m_empty_lhs && "It should have been inserted in the RHS");
    assert(!is_full() && "The block is full");

    double* file = data();
    uint64_t boundary = m_empty_lhs;

    memmove(/* to */ file + relative_position +1, /* from */ file + relative_position, sizeof(file[0]) * (boundary - relative_position));
    file[relative_position] = element;

    m_empty_lhs++;
    m_cardinality++;
}

void PayloadFile::do_insert_rhs(uint64_t relative_position, double element){
    assert(relative_position < static_cast<uint64_t>(numeric_limits<typeof(m_capacity)>::max()) && "Overflow");
    assert(relative_position > 0 && "If the position is 0, it should have been appended in the LHS");
    assert(!is_full() && "The block is full");

    double* file = data() + m_empty_rhs;

    memmove(/* to */ file -1, /* from */ file, sizeof(file[0]) * relative_position);
    file[relative_position -1] = element; /* -1 because file still refers to the old start of the file */

    m_empty_rhs--;
    m_cardinality++;
}

void PayloadFile::split(){
    // Create and a link a new block
    PayloadFile* next = create_payload_block();
    next->m_next = m_next;
    m_next = next;

    // First, decide how many elements from the LHS and the RHS to move to the new block
    uint64_t num_elements_to_move = cardinality() /2;
    uint64_t block1_num_elts_lhs = 0; // we're going to move `num_elts_lhs' from the lhs
    uint64_t block1_num_elts_rhs = m_capacity - m_empty_rhs; // we're going to move `num_elts_rhs' from the rhs
    if(block1_num_elts_rhs <= num_elements_to_move){
        block1_num_elts_lhs = num_elements_to_move - block1_num_elts_rhs;
    } else {
        block1_num_elts_rhs = num_elements_to_move;
    }
    double* block1_start_lhs = data() + static_cast<uint64_t>(m_empty_lhs) - block1_num_elts_lhs;
    double* block1_start_rhs = data() + static_cast<uint64_t>(m_capacity) - block1_num_elts_rhs;
    double* block1_rem_start_lhs = data();
    uint64_t block1_rem_num_elts_lhs = static_cast<uint64_t>(m_empty_lhs) - block1_num_elts_lhs;
    double* block1_rem_start_rhs = data() + static_cast<uint64_t>(m_empty_rhs);
    uint64_t block1_rem_num_elts_rhs = block1_start_rhs - block1_rem_start_rhs;
    m_empty_lhs -= block1_num_elts_lhs;
    m_empty_rhs += block1_num_elts_rhs;
    m_cardinality -= num_elements_to_move;

    // Second, decide how many elements to move to the LHS and RHS of the new block
    uint64_t block2_num_elts_lhs = num_elements_to_move / 2;
    uint64_t block2_num_elts_rhs = num_elements_to_move - block2_num_elts_lhs;
    next->m_cardinality = num_elements_to_move;
    next->m_empty_lhs = block2_num_elts_lhs;
    next->m_empty_rhs = next->capacity() - block2_num_elts_rhs;
    double* block2_start_lhs = next->data();
    double* block2_start_rhs = next->data() + next->m_empty_rhs;

    { // Third, copy the elements from this block to the new block
        // to block2, lhs
        uint64_t num_elts = min(block1_num_elts_lhs, block2_num_elts_lhs);
        if(num_elts > 0){ // from block1, lhs
            memcpy(/* to */ block2_start_lhs, /* from */ block1_start_lhs, num_elts * sizeof(data()[0]));
            block1_start_lhs += num_elts;
            block2_start_lhs += num_elts;
            block1_num_elts_lhs -= num_elts;
            block2_num_elts_lhs -= num_elts;
        }
        num_elts = min(block1_num_elts_rhs, block2_num_elts_lhs);
        if(num_elts > 0){ // from block1, rhs
            memcpy(/* to */ block2_start_lhs, /* from */ block1_start_rhs, num_elts * sizeof(data()[0]));
            block1_start_rhs += num_elts;
            block2_start_lhs += num_elts;
            block1_num_elts_rhs -= num_elts;
            block2_num_elts_lhs -= num_elts;
        }
        assert(block2_num_elts_lhs == 0 && "Not all elements were copied");

        // to block2, rhs
        num_elts = min(block1_num_elts_lhs, block2_num_elts_rhs);
        if(num_elts > 0){ // from block1, lhs
            memcpy(/* to */ block2_start_rhs, /* from */ block1_start_lhs, num_elts * sizeof(data()[0]));
            block1_start_lhs += num_elts;
            block2_start_rhs += num_elts;
            block1_num_elts_lhs -= num_elts;
            block2_num_elts_rhs -= num_elts;
        }
        num_elts = min(block1_num_elts_rhs, block2_num_elts_rhs);
        if(num_elts > 0){ // from block1, rhs
            memcpy(/* to */ block2_start_rhs, /* from */ block1_start_rhs, num_elts * sizeof(data()[0]));
            block1_start_rhs += num_elts;
            block2_start_rhs += num_elts;
            block1_num_elts_rhs -= num_elts;
            block2_num_elts_rhs -= num_elts;
        }
        assert(block2_num_elts_rhs == 0 && "Not all elements were copied");
    }

   { // Fourth, redistribute the elements in this block
       uint64_t target_lhs = cardinality() / 2;
       uint64_t target_rhs = cardinality() - target_lhs;
       COUT_DEBUG("target_lhs:" << target_lhs << ", target_rhs: " << target_rhs << ", block1 rem lhs: " << block1_rem_num_elts_lhs << ", block1 rem rhs: " << block1_rem_num_elts_rhs);
       m_empty_lhs = target_lhs;
       m_empty_rhs = capacity() - target_rhs;
       assert(cardinality() == static_cast<uint64_t>(m_empty_lhs) + (capacity() - static_cast<uint64_t>(m_empty_rhs)));

       // Move `target_rhs' to the right hand side of this block
       uint64_t num_elts = min(block1_rem_num_elts_rhs, target_rhs);
       if(num_elts > 0){
           memmove(data() + capacity() - num_elts, block1_rem_start_rhs + block1_rem_num_elts_rhs - num_elts, num_elts * sizeof(data()[0]));
           block1_rem_num_elts_rhs -= num_elts;
           target_rhs -= num_elts;
       }
       num_elts = min(block1_rem_num_elts_lhs, target_rhs);
       if(num_elts > 0){
           memcpy(data() + static_cast<uint64_t>(m_empty_rhs), block1_rem_start_lhs + block1_rem_num_elts_lhs - num_elts, num_elts * sizeof(data()[0]));
           block1_rem_num_elts_lhs -= num_elts;
           target_rhs -= num_elts;
       }
       assert(target_rhs == 0 && "Not all elements were copied");

       // Move `target_lhs' to the left hand side of this block
       num_elts = min(block1_rem_num_elts_rhs, target_lhs);
       if(num_elts > 0){
           memcpy(data() + block1_rem_num_elts_lhs, block1_rem_start_rhs, num_elts * sizeof(data()[0]));
           block1_rem_num_elts_rhs -= num_elts;
           target_lhs -= num_elts;
       }
       // The elements in the LHS are already in the correct position
       target_lhs -= block1_rem_num_elts_lhs;
       assert(target_lhs == 0 && "Not all elements were copied");
   }
}

void PayloadFile::remove(uint64_t position){
    COUT_DEBUG("position: " << position);

    PayloadFile* parent = nullptr;
    PayloadFile* block = this;

    // Find the block where the element needs to be removed
    while(position >= block->cardinality()){
        assert(block->m_next != nullptr && "The element does not exist");
        parent = block;
        position -= block->cardinality();
        block = block->m_next;
    }

    // Remove the element, either from the LHS or the RHS
    if(position < block->m_empty_lhs){
        block->do_remove_lhs(position);
    } else {
        block->do_remove_rhs(position - static_cast<uint64_t>(block->m_empty_lhs));
    }

    // If possible, merge this block with its neighbours
    if(block->m_next != nullptr && (block->cardinality() + block->m_next->cardinality() < 0.8 * block->capacity())){
        block->merge();
    } else if(parent != nullptr && (parent->cardinality() + block->cardinality() < 0.8 * parent->capacity())){
        parent->merge();
    }
}

void PayloadFile::do_remove_lhs(uint64_t relative_position){
    assert(relative_position < static_cast<uint64_t>(m_empty_lhs) && "Invalid position");
    memmove(data() + relative_position, data() + relative_position + 1, sizeof(data()[0]) * (static_cast<uint64_t>(m_empty_lhs) - (relative_position +1)));
    m_empty_lhs--;
    m_cardinality--;
}

void PayloadFile::do_remove_rhs(uint64_t relative_position){
    assert((relative_position < capacity() - static_cast<uint64_t>(m_empty_rhs)) && "Invalid position");
    double* start = data() + static_cast<uint64_t>(m_empty_rhs);
    memmove(start + 1, start, sizeof(data()[0]) * relative_position);
    m_empty_rhs++;
    m_cardinality--;
}

void PayloadFile::merge(){
    assert(m_next != nullptr && "This block does not have a next neighbour");
    assert((capacity() >= cardinality() + m_next->cardinality()) && "Overflow");

    // Move all elts from the rhs to the lhs in the current block
    uint64_t num_elts = capacity() - m_empty_rhs;
    memmove(data() + m_empty_lhs, data() + m_empty_rhs, num_elts * sizeof(data()[0]));
    m_empty_lhs += num_elts;

    // Move all elts from the next block to the RHS of the current block
    double* start = data() + capacity() - m_next->cardinality();
    uint64_t block2_num_elts_lhs = m_next->m_empty_lhs;
    uint64_t block2_num_elts_rhs = m_next->capacity() - m_next->m_empty_rhs;
    memcpy(start, m_next->data(), block2_num_elts_lhs * sizeof(data()[0]));
    memcpy(start + block2_num_elts_lhs, m_next->data() + m_next->m_empty_rhs, block2_num_elts_rhs * sizeof(data()[0]));
    m_empty_rhs = capacity() - m_next->cardinality();

    // Update the number of elements in the block
    m_cardinality += m_next->cardinality();

    // Update the links between blocks
    auto zombie = m_next;
    zombie->m_empty_lhs = 0;
    zombie->m_empty_rhs = zombie->m_capacity;
    zombie->m_cardinality = 0;

    m_next = zombie->m_next;

    context::thread_context()->gc_mark(zombie, (void (*)(void*)) destroy_payload_block);
}

void PayloadFile::clear(){
    PayloadFile* current = this;

    do {
        current->m_cardinality = 0;
        current->m_empty_lhs = 0;
        current->m_empty_rhs = current->m_capacity;
        PayloadFile* next = current->m_next;

        if(current != this){ // Do not deallocate the first block
            context::thread_context()->gc_mark(current, (void (*)(void*)) destroy_payload_block);
        }

        current = next;
    } while(current != nullptr);
}


double PayloadFile::get(uint64_t position) const {
    const PayloadFile* block = this;

    // Find the block relative to the given position
    while(block->cardinality() <= position){
        assert(block->m_next != nullptr && "Invalid position");
        position -= block->cardinality();
        block = block->m_next;
    }

    // Left hand side ?
    if(position < block->m_empty_lhs){
        return (block->data())[position];
    } else { // right hand side
        position -= block->m_empty_lhs;
        const double* start = block->data() + block->m_empty_rhs;
        return start[position];
    }
}

PayloadIterator PayloadFile::iterator() const {
    return PayloadIterator(this);
}

void PayloadFile::dump() const {
    const PayloadFile* block = this;
    uint64_t block_num = 0;
    uint64_t position = 0;

    do {
        cout << "[Block #" << block_num << ", address: " << block << ", capacity: " << block->capacity() << ", cardinality: " << block->cardinality() << ", lhs: " << block->m_empty_lhs << ", rhs: " << block->m_empty_rhs << "]\n";
        cout << "Left hand side:\n";
        for(uint64_t i = 0, sz = block->m_empty_lhs; i < sz; i++){
            cout << "[" << position << "] [" << i << "] " << block->data()[i] << "\n";
            position++;
        }
        cout << "Right hand side:\n";
        for(uint64_t i = 0, sz = block->capacity() - block->m_empty_rhs; i < sz; i++){
            cout << "[" << position << "] [" << i << "] " << (block->data() + block->m_empty_rhs)[i] << "\n";
            position++;
        }

        // next block
        block = block->m_next;
        if(block != nullptr){ cout << endl; }
    } while(block != nullptr);
}

} // namespace
