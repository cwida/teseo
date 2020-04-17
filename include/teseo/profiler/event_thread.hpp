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

#include "teseo/profiler/event_list.hpp"

namespace teseo::profiler {

class EventThread {
    EventList m_event_list;
    uint64_t m_thread_id;
    std::string m_thread_name;
    std::chrono::time_point<std::chrono::system_clock> m_time_ctor; // when the instance was created
    std::chrono::time_point<std::chrono::system_clock> m_time_dtor; // when this instance was removed from the thread context

public:
    /**
     * Init the instance
     */
    EventThread();

    /**
     * Mark as completed the usage of this instance
     */
    void close();

    /**
     * Check whether there have been any event recording
     */
    bool has_events() const;

    /**
     * Retrieve the data associated to the given event
     */
    EventData* get_event(EventName event){
        return &(m_event_list[(int) event]);
    }

    /**
     * Dump the recorded events in json format to the given output stream
     */
    void to_json(std::ostream& out) const;
};

} // namespace
