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

#include "util/timer.hpp"
#include "event_name.hpp"


namespace teseo::internal::profiler {

#if defined(HAVE_PROFILER)

struct EventData; // forward decl.

class ScopedTimer {
    EventData* m_event; // pointer to the event data
    util::Timer m_timer; // internal timer

public:
    // Create a timer for the given event
    ScopedTimer(EventName event, bool start_immediately = true);

    // Destructor
    ~ScopedTimer();

    // Restart the timer
    void start();

    // Stop the timer
    void stop();
};


#else
// Dummy class
class ScopedTimer {
public:
    ScopedTimer(EventName event, bool start_immediately = true){ };
    void start(){ }
    void stop(){ }
};
#endif

}

