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

#include "teseo/memstore/segment.hpp"

#include <cassert>
#include <chrono>

#include "teseo/memstore/key.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/thread.hpp"

using namespace std;


namespace teseo::memstore {

Segment::Segment() : m_fence_key( KEY_MAX ) {
    m_num_active_threads = 0;
    m_time_last_rebal = chrono::steady_clock::now();
    m_rebal_context = nullptr;
}

Segment::~Segment() {

}

void Segment::lock(){
    m_latch.lock();
#if !defined(NDEBUG)
    util::barrier();
    assert(m_locked == false && "Spin lock already acquired");
    m_locked = true;
    m_owned_by = util::Thread::get_thread_id();
    util::barrier();
#endif
}

void Segment::unlock(){
#if !defined(NDEBUG)
    util::barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    util::barrier();
#endif
    m_latch.unlock();
}

void Segment::wake_next(){
    assert(m_locked && "To invoke this method the internal lock must be acquired first");
    if(m_queue.empty()) return;

    // FREE are "optimistic readers". These are readers that don't modify the version of the latch. There
    // is no point to wake them immediately if there are other readers or writers in the queue, because
    // they will abort immediately once another reader/writer accesses the latch. The idea is either to skip
    // them or wake all of them if there are no other entities in the queue
    if(m_queue[0].m_purpose == State::FREE){
        uint64_t sz = m_queue.size();
        uint64_t i = 0;
        do {
            m_queue.append(m_queue[0]);
            m_queue.pop();
            i++;
        } while(i < sz && m_queue[0].m_purpose == State::FREE);

        if(i == sz){ // are all the items optimistic readers?
            wake_all();
            return; // done
        }
    }

    switch(m_queue[0].m_purpose){
    case State::READ:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::READ);
        break;
    case State::WRITE:
        m_queue[0].m_promise->set_value();
        m_queue.pop();
        break;
    case State::REBAL:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::REBAL);
        break;
    default:
        assert(0 && "Invalid state");
    }
}

void Segment::wake_all(){
    assert((m_locked || m_state == State::REBAL) && "To invoke this method the internal lock must be acquired first");

    while(!m_queue.empty()){
        m_queue[0].m_promise->set_value(); // notify
        m_queue.pop();
    }
}

} // namespace
