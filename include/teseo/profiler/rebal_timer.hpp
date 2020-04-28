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

#include "teseo/profiler/rebal_time_unit.hpp"
#include "teseo/util/timer.hpp"

namespace teseo::profiler {

#if defined(HAVE_PROFILER)
/**
 * Simple timer to account the duration of a task inside a rebalance
 */
class RebalanceTimer {
    RebalanceTimer(const RebalanceTimer&) = delete;
    RebalanceTimer& operator=(const RebalanceTimer&) = delete;


    RebalanceTimeUnit* m_counter; // pointer to the event data
    util::Timer m_timer; // internal timer

public:
    // Create a timer for the given counter
    RebalanceTimer(RebalanceTimeUnit* counter, bool start_immediately = true) : m_counter(counter) {
        if(start_immediately) start();
    }

    // Destructor
    ~RebalanceTimer(){
        stop();
        *m_counter += m_timer.duration<RebalanceTimeUnit>();
    }

    // Restart the timer
    void start(){ m_timer.resume(); }

    // Stop the timer
    void stop() { m_timer.stop(); }
};

#else
// Dummy class
class RebalanceTimer {
public:
    RebalanceTimer(){ };
    void start(){ }
    void stop(){ }
};
#endif

}
