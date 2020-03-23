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


// extracted from libcommon
#pragma once

#include <chrono>
#include <string>

#include "miscellaneous.hpp"

namespace teseo::internal::util {

/**
 * A simple timer to keep track of the world clock time.
 *
 * Usage:
 *
 * Timer timer;
 * timer.start();
 * ... computation ...
 * timer.stop();
 * cout << "Elapsed time: " << timer << "\n";
 *
 */
class Timer {
//  Timer(const Timer&) = delete;
//  Timer& operator=(const Timer& timer) = delete;
    friend Timer operator+(Timer t1, Timer t2);

    using clock = std::chrono::steady_clock;

    clock::time_point m_t0; // start time
    clock::time_point m_t1; // end time

public:
    /**
     * Create a new timer, but doesn't start it
     */
    Timer(){ }

    /**
     * Start tracking the time. If the timer was already previously executed, reset the accounted time to zero.
     */
    void start(){
        m_t1 = clock::time_point{};
        barrier();
        m_t0 = clock::now();
        barrier();
    }

    /**
     * Restart the tracking of time, without resetting the time previously accounted before;
     */
    void resume(){
        if(m_t0 != clock::time_point{}) return; // already running;
        if(m_t1 == clock::time_point{}){ // this timer has never been executed
            start();
        } else {
            barrier();
            m_t0 = clock::now() - (m_t1 - m_t0);
            barrier();
        }
    }

    /**
     * Stop tracking the time
     */
    void stop() {
        barrier();
        m_t1 = clock::now();
        barrier();
    }

    /**
     * Retrieve the amount of time elapsed as a std::chrono duration
     */
    template<typename D>
    D duration() const {
        return std::chrono::duration_cast<D>(m_t1 - m_t0);
    }

    /**
     * Retrieve the amount of time elapsed as an integer representation of a std::chrono duration
     */
    template<typename D>
    uint64_t convert() const {
        return static_cast<uint64_t>( duration<D>().count() );
    }

    /**
     * Retrieve the total amount of nanoseconds accounted by timer
     */
    uint64_t nanoseconds() const{ return convert<std::chrono::nanoseconds>(); }

    /**
     * Retrieve the total amount of microseconds accounted by timer
     */
    uint64_t microseconds() const{ return convert<std::chrono::microseconds>(); }

    /**
     * Retrieve the total amount of milliseconds accounted by timer
     */
    uint64_t milliseconds() const{ return convert<std::chrono::milliseconds>(); }

    /**
     * Retrieve the total amount of seconds accounted by timer
     */
    uint64_t seconds() const{ return convert<std::chrono::seconds>(); }

    /**
     * Get a string representation of the time accounted by this timer
     */
    std::string to_string() const;
};

// Print to the output stream the time accounted by the given timer
std::ostream& operator<<(std::ostream& out, const Timer& timer);

// Add up the time accounted by the two given timers
Timer operator+(Timer t1, Timer t2);

} // namesapce

