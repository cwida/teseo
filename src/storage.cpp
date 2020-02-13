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

#include "storage.hpp"

#include <cassert>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>

#include "utility.hpp"

using namespace std;

namespace teseo::internal {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex g_debugging_mutex [[maybe_unused]]; // context.cpp
#define DEBUG
#define COUT_CLASS_NAME "Unknown"
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[" << COUT_CLASS_NAME << "::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Key                                                                     *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "Storage::Key"

// Key, convenience class
Storage::Key::Key() : Key(numeric_limits<uint64_t>::max(), numeric_limits<uint64_t>::max()) { }
Storage::Key::Key(uint64_t vertex_id) : Key(vertex_id, 0) { }
Storage::Key::Key(uint64_t src, uint64_t dst) : m_source(src), m_destination(dst) { }
uint64_t Storage::Key::get_source() const { return m_source; }
uint64_t Storage::Key::get_destination() const { return m_destination; }
void Storage::Key::set(uint64_t vertex_id) { m_source = vertex_id; m_destination = 0; }
void Storage::Key::set(uint64_t source, uint64_t destination) { m_source = source; m_destination = destination; }
bool Storage::Key::operator==(const Key& other) const { return get_source() == other.get_source() && get_destination() == other.get_destination(); }
bool Storage::Key::operator!=(const Key& other) const { return !(*this == other); }
bool Storage::Key::operator<(const Key& other) const { return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() < other.get_destination()); }
bool Storage::Key::operator<=(const Key& other) const { return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() <= other.get_destination()); }
bool Storage::Key::operator>(const Key& other) const { return !(*this <= other); }
bool Storage::Key::operator>=(const Key& other) const { return !(*this < other); }
Storage::Key Storage::Key::min(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::min(), numeric_limits< decltype(Key{}.get_destination()) >::min()); }
Storage::Key Storage::Key::max(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::max(), numeric_limits< decltype(Key{}.get_destination()) >::max()); }
std::ostream& operator<<(std::ostream& out, const Storage::Key& key){
    out << key.get_source() << " -> " << key.get_destination();
    return out;
}

/*****************************************************************************
 *                                                                           *
 *   Gate                                                                    *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "Storage::Gate"

Storage::Gate::Gate(uint64_t gate_id, uint64_t num_segments) : m_gate_id(gate_id), m_num_segments(num_segments) {
    m_num_active_threads = 0;
    m_space_left = 0;
    m_fence_low_key = m_fence_high_key = Key::max();

    // Init the separator keys
    for(int64_t i = 0; i < window_length(); i++){
        set_separator_key(i, Key::max());
    }
}

Storage::Gate::~Gate() {
    // nop
}

int64_t Storage::Gate::gate_id() const noexcept {
    return m_gate_id;
}

int64_t Storage::Gate::window_start() const noexcept {
    return gate_id() * window_length();
}

int64_t Storage::Gate::window_length() const noexcept {
    return m_num_segments;
}

auto Storage::Gate::separator_keys() -> Key* {
    return reinterpret_cast<Key*>( reinterpret_cast<uint8_t*>(this) + sizeof(Gate) );
}

auto Storage::Gate::separator_keys() const -> const Key* {
    return const_cast<Storage::Gate*>(this)->separator_keys();
}

void Storage::Gate::lock(){
    m_spin_lock.lock();
#if !defined(NDEBUG)
    barrier();
    assert(m_locked == false && "Spin lock already acquired");
    m_locked = true;
    m_owned_by = get_thread_id();
    barrier();
#endif
}

void Storage::Gate::unlock(){
#if !defined(NDEBUG)
    barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    barrier();
#endif
    m_spin_lock.unlock();
}

uint64_t Storage::Gate::find(Key key) const {
    assert(m_fence_low_key <= key && key <= m_fence_high_key && "Fence keys check: the key does not belong to this gate");
    int64_t i = 0, sz = window_length() -1;
    const Key* __restrict keys = separator_keys();
    while(i < sz && keys[i] <= key) i++;
    return window_start() + i;
}

void Storage::Gate::set_separator_key(size_t segment_id, Key key){
    assert(segment_id >= window_start() && segment_id < window_start() + window_length());
    if(segment_id > window_start()){
        separator_keys()[segment_id - window_start() -1] = key;
    }

#if !defined(NDEBUG)
    if(segment_id > window_start()) { assert(get_separator_key(segment_id) == key); } // otherwise it's given by the fence key
#endif
}

auto Storage::Gate::get_separator_key(size_t segment_id) const -> Key {
    assert(segment_id >= window_start() && segment_id < window_start() + window_length());
    if(segment_id == window_start())
        return m_fence_low_key;
    else
        return separator_keys()[segment_id - window_start() -1];
}

Storage::Gate::Direction Storage::Gate::check_fence_keys(Key key) const {
    assert(m_locked && m_owned_by == get_thread_id() && "To perform this check the lock must have been acquired by the same thread currently operating");
    if(m_fence_low_key == Key::max())  // this array is not valid anymore, restart the operation
        return Direction::INVALID;
    else if(key < m_fence_low_key)
        return Direction::LEFT;
    else if(key > m_fence_high_key)
        return Direction::RIGHT;
    else
        return Direction::GO_AHEAD;
}

void Storage::Gate::set_fence_keys(Key min, Key max){
    m_fence_low_key = min;
    m_fence_high_key = max;
}

uint64_t Storage::Gate::memory_footprint(uint64_t num_segments){
    if(num_segments > 0) num_segments--; // because the first separator key is implicitly stored as fence key
    uint64_t min_space = sizeof(Gate) + num_segments * sizeof(Key);
    assert(min_space % 8 == 0 && "Expected at least to be aligned to the word");
    return min_space;
}


/*****************************************************************************
 *                                                                           *
 *   Leaf                                                                    *
 *                                                                           *
 *****************************************************************************/

#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "Storage::Leaf"

Storage::Leaf::Leaf(uint16_t num_gates, uint16_t num_segments_per_gate, uint32_t space_per_segment) : m_num_gates(num_gates), m_num_segments_per_gate(num_segments_per_gate), m_space_per_segment(num_segments_per_gate){
    m_previous = m_next =  nullptr;

    // init the gates
    for(int i = 0; i < num_gates(); i++){
        new (get_gate(i)) Gate(i, num_segments_per_gate);
    }

    // init the segments
    for(int i = 0; i < num_segments(); i++){
        new (get_segment(i)) Segment(m_space_per_segment - sizeof(Segment));
    }
}

int64_t Storage::Leaf::num_gates() const {
    return m_num_gates;
}

int64_t Storage::Leaf::num_segments() const {
    return num_gates() * m_num_segments_per_gate;
}

uint64_t Storage::Leaf::get_total_gate_size() const {
    return Gate::memory_footprint(m_num_segments_per_gate) + ((uint64_t) m_num_segments_per_gate) * m_space_per_segment;
}

Storage::Gate* Storage::Leaf::get_gate(uint64_t gate_id){
    assert((gate_id >= 0 && gate_id < num_gates()) && "Invalid gate_id");
    return reinterpret_cast<Gate*>( reinterpret_cast<uint8_t*>(this) + sizeof(Leaf) + get_total_gate_size() * gate_id );
}

Storage::Gate* Storage::Leaf::get_gate_by_segment_id(uint64_t segment_id) {
    return get_gate(segment_id / m_num_segments_per_gate);
}

Storage::Segment* Storage::Leaf::get_segment(uint64_t segment_id){
    assert((segment_id >= 0 && segment_id < num_segments()) && "Invalid segment_id");

    Gate* gate = get_gate_by_segment_id(segment_id);
    uint64_t relative_segment_id = segment_id % m_num_segments_per_gate;
    uint8_t* segment_start_offset = reinterpret_cast<uint8_t*>(gate) + Gate::memory_footprint(m_num_segments_per_gate) + relative_segment_id * m_space_per_segment;
    return reinterpret_cast<Segment*>(segment_start_offset);
}

Storage::Leaf* Storage::Leaf::allocate(uint64_t memory_budget, uint64_t num_segments_per_gate, uint64_t space_per_segment){
    if(memory_budget % 8 != 0) RAISE_EXCEPTION(InternalError, "The memory budget is not a multiple of 8 ")
    if(memory_budget < (space_per_segment * 4)) RAISE_EXCEPTION(InternalError, "The memory budget must be at least 4 times the space per segment");
    if(num_segments_per_gate == 0) RAISE_EXCEPTION(InternalError, "Great, 0 segments per gates");
    if(space_per_segment % 8 != 0) RAISE_EXCEPTION(InternalError, "The space per segment should also be a multiple of 8");
    if(space_per_segment == 0) RAISE_EXCEPTION(InternalError, "The space per segment is 0");

    // 1. Decide the memory layout of the leaf
    // 1a) compute the amount of space required by a single gate and all of its associated segments
    double gate_total_sz = Gate::memory_footprint(num_segments_per_gate) + num_segments_per_gate * (sizeof(Segment) + space_per_segment);
    // 1b) solve the inequality LeafSize + x * gate_total_sz >= memory_budget, where x will be our final number of gates
    double num_gates = ceil( (static_cast<double>(memory_budget) - sizeof(Leaf) ) / gate_total_sz);
    // 1c) how many bytes we need to remove to each segment from 'space_per_segment', so that our memory budget is satisfied
    double surplus_total = gate_total_sz * num_gates - memory_budget;
    double surplus_per_segment = ceil(surplus_total / num_gates * num_segments_per_gate);
    // 1d) compute the new amount of space that can be given to each segment
    uint64_t new_space_per_segment = space_per_segment - static_cast<uint64_t>(surplus_per_segment);
    // 1e) round up to the previous multiple of 8
    new_space_per_segment -= (new_space_per_segment % 8);

    // 2. Allocate the leaf
    void* heap { nullptr };
    int rc = posix_memalign(&heap, /* alignment = */ memory_budget,  /* size = */ memory_budget);
    if(rc != 0) throw std::runtime_error("Storage::Leaf::allocate, cannot obtain a chunk of aligned memory");
    Leaf* leaf = new (heap) Leaf((uint16_t) num_gates, (uint16_t) num_segments_per_gate, (uint32_t) new_space_per_segment + /* header */ sizeof(Segment));

    return leaf;
}

} // namespace


