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

#include "scoped_timer.hpp"

#include "context/thread_context.hpp"
#include "event_global.hpp"
#include "event_thread.hpp"

using namespace std;
using namespace teseo::internal::context;
using namespace teseo::internal::util;

namespace teseo::internal::profiler {

#if defined(HAVE_PROFILER)

ScopedTimer::ScopedTimer(EventName event, bool start_immediately) : m_event(nullptr){
    m_event = thread_context()->profiler()->get_event(event);
    m_event->m_num_scoped_timers++;
    if(start_immediately) start();
}

ScopedTimer::~ScopedTimer(){
    stop();
    m_event->m_total_time += m_timer.duration<chrono::microseconds>();
}

void ScopedTimer::start(){
    m_event->m_num_invocations++;
    m_timer.resume();
}

void ScopedTimer::stop(){
    m_timer.stop();
}

#endif

} // namespace


