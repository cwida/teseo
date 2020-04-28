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
#include "teseo/profiler/event_thread.hpp"

#include <chrono>

#include "teseo/util/chrono.hpp"
#include "teseo/util/thread.hpp"

using namespace std;
using namespace std::chrono;

namespace teseo::profiler {

EventThread::EventThread() : m_event_list(){
    m_time_ctor = system_clock::now();
    m_thread_id = util::Thread::get_thread_id();
    m_thread_name = util::Thread::get_name();
}

void EventThread::close() {
    m_time_dtor = system_clock::now();
}

bool EventThread::has_events() const{
    for(uint64_t i = 0; i < m_event_list.size(); i++){
        if(m_event_list[i].m_num_invocations > 0){
            return true;
        }
    }

    return false;
}

void EventThread::to_json(std::ostream& out) const {
    out << "{";
    out << "\"thread_id\": " << m_thread_id << ", ";
    out << "\"thread_name\": \"" << m_thread_name << "\", ";
    out << "\"start_time\": \"" << util::to_string(m_time_ctor) << "\", ";
    out << "\"end_time\": \"" << util::to_string(m_time_dtor) << "\", ";
    out << "\"events\": [";
    bool first = true;

    for(uint64_t i = 0; i < m_event_list.size(); i++){
        const EventData& event = m_event_list[i];

        if(event.m_num_invocations > 0){
            if(!first) out << ", ";

            out << "{";
            out << "\"event\": \"" << magic_enum::enum_name((EventName) i) << "\", ";
            out << "\"microseconds\": " << duration_cast<chrono::microseconds>(event.m_total_time).count() << ", ";
            out << "\"num_created\": " << event.m_num_scoped_timers << ", ";
            out << "\"num_invoked\": " << event.m_num_invocations;
            out << "}";

            first = false;
        }
    }

    out << "]";

    out << "}";
}

}
