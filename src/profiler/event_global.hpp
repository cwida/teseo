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

#include <chrono>
#include <ostream>
#include <string>
#include <vector>

namespace teseo::internal::profiler {

class EventThread; // forward decl.

/**
 * List of all events recorded in the terminated ThreadContexts
 */
class EventGlobal {
    std::vector<EventThread*> m_event_threads; // list of terminated event threads
    const std::chrono::time_point<std::chrono::system_clock> m_time_ctor; // when the instance was created

public:
    /**
     * Create a new instance
     */
    EventGlobal();

    /**
     * Destructor
     */
    ~EventGlobal();

    /**
     * Load the given event thread into the global list
     */
    void acquire(EventThread* ev_thread);

    /**
     * Dump the recorded events in json format to the given output stream
     */
    void to_json(std::ostream& out) const;

};

}
