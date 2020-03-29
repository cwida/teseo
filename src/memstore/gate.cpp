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

#include "gate.hpp"

#include <mutex>
#include "context/global_context.hpp"
#include "util/miscellaneous.hpp"

using namespace teseo::internal::context;
using namespace teseo::internal::util;
using namespace std;

namespace teseo::internal::memstore {


/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::lock_guard<std::mutex> lock(g_debugging_mutex); std::cout << "[Gate::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


Gate::Gate(uint64_t gate_id, uint64_t num_separator_keys) : m_gate_id(gate_id), m_num_separator_keys(num_separator_keys) {
    m_num_active_threads = 0;
    m_used_space = 0;
    m_fence_low_key = m_fence_high_key = Key::max();

    // Init the separator keys
    for(int64_t i = 0; i < window_length(); i++){
        set_separator_key(i, Key::max());
    }
}

Gate::~Gate() {
    // nop
}

int64_t Gate::id() const noexcept {
    return m_gate_id;
}

int64_t Gate::window_start() const noexcept {
    return id() * window_length();
}

int64_t Gate::window_length() const noexcept {
    return m_num_separator_keys;
}

auto Gate::separator_keys() -> Key* {
    return reinterpret_cast<Key*>( reinterpret_cast<uint8_t*>(this) + sizeof(Gate) );
}

auto Gate::separator_keys() const -> const Key* {
    return const_cast<Gate*>(this)->separator_keys();
}

void Gate::lock(){
    m_latch.lock();
#if !defined(NDEBUG)
    barrier();
    assert(m_locked == false && "Spin lock already acquired");
    m_locked = true;
    m_owned_by = get_thread_id();
    barrier();
#endif
}

void Gate::unlock(){
#if !defined(NDEBUG)
    barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    barrier();
#endif
    m_latch.unlock();
}

uint64_t Gate::find(Key key) const {
    assert(m_fence_low_key <= key && key <= m_fence_high_key && "Fence keys check: the key does not belong to this gate");
    int64_t i = 0, sz = window_length() -1;
    const Key* __restrict keys = separator_keys();
    while(i < sz && keys[i] <= key) i++;
    return i;
}

void Gate::set_separator_key(size_t position, Key key){
//    assert(segment_id >= window_start() && segment_id < window_start() + window_length());
    assert(position >= 0 && (int64_t) position < window_length());

    if(position > 0){
        separator_keys()[position -1] = key;
    }

#if !defined(NDEBUG)
    if(position > 0) { assert(get_separator_key(position) == key); } // otherwise it's given by the fence key
#endif
}

auto Gate::get_separator_key(uint64_t position) const -> Key {
    assert(position >= 0 && (int64_t) position < window_length());

    if(position == 0)
        return m_fence_low_key;
    else
        return separator_keys()[position -1];
}

Gate::Direction Gate::check_fence_keys(Key key) const {
    // not true anymore: there may be an optimistic reader
    //assert(m_locked && m_owned_by == get_thread_id() && "To perform this check the lock must have been acquired by the same thread currently operating");

    if(m_fence_low_key == Key::max())  // this array is not valid anymore, restart the operation
        return Direction::INVALID;
    else if(key < m_fence_low_key)
        return Direction::LEFT;
    else if(key > m_fence_high_key)
        return Direction::RIGHT;
    else
        return Direction::GO_AHEAD;
}

void Gate::set_fence_keys(Key min, Key max){
    m_fence_low_key = min;
    m_fence_high_key = max;
}

uint64_t Gate::memory_footprint(uint64_t num_separator_keys){
    if(num_separator_keys > 0) num_separator_keys--; // because the first separator key is implicitly stored as fence key
    uint64_t min_space = sizeof(Gate) + num_separator_keys * sizeof(Key);
    assert(min_space % 8 == 0 && "Expected at least to be aligned to the word");
    return min_space;
}

void Gate::wake_next(){
    COUT_DEBUG("gate id: " << id());
    assert(m_locked && "To invoke this method the internal lock must be acquired first");
    if(m_queue.empty()) return; // nop

    switch(m_queue[0].m_purpose){
    case State::READ:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::READ);
        break;
    case State::WRITE:
    case State::REBAL:
        m_queue[0].m_promise->set_value();
        m_queue.pop();
        break;
    default:
        assert(0 && "Invalid state");
    }
}

void Gate::wake_all(){
    COUT_DEBUG("gate id: " << id());
    assert((m_locked || m_state == State::REBAL) && "To invoke this method the internal lock must be acquired first");

    while(!m_queue.empty()){
        m_queue[0].m_promise->set_value(); // notify
        m_queue.pop();
    }
}

} // namespace
