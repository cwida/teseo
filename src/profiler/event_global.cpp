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

#include "teseo/profiler/event_global.hpp"

#include <fstream>
#include <iostream>

#include "teseo/profiler/event_thread.hpp"
#include "teseo/util/chrono.hpp"
#include "teseo/util/thread.hpp"

using namespace std;
using namespace std::chrono;

namespace teseo::profiler {

EventGlobal::EventGlobal() : m_time_ctor(system_clock::now()){

}

EventGlobal::~EventGlobal(){
    for(uint64_t i = 0; i < m_event_threads.size(); i++){
        delete m_event_threads[i];  m_event_threads[i] = nullptr;
    }
}

void EventGlobal::acquire(EventThread* ev_thread){
    ev_thread->close();
    m_event_threads.push_back(ev_thread);
}


void EventGlobal::to_json(std::ostream& out) const {
    out << "{";
    out << "\"start_time\": \"" << util::to_string(m_time_ctor) << "\", ";
    out << "\"end_time\": \"" << util::to_string(system_clock::now()) << "\", ";
    out << "\"thread_id\": " << util::Thread::get_thread_id() << ", ";
    out << "\"thread_name\": \"" << util::Thread::get_name() << "\", ";
    out << "\"thread_events\": [ ";
    bool first = true;
    for(uint64_t i = 0; i < m_event_threads.size(); i++){
        if(m_event_threads[i]->has_events()){
            if(!first) out << ", ";
            m_event_threads[i]->to_json(out);
            first = false;
        }
    }
    out << "]";
    out << "}";
}

} // namespace


