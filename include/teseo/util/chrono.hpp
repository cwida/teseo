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
#include <string>
#include <sys/time.h>

namespace teseo::util {

/**
 * Cast a chrono::duration to a timeval
 */
template<typename Duration>
struct timeval duration2timeval(Duration duration){
    uint64_t microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    struct timeval result;
    result.tv_sec = microseconds / (/* milli */ 1000 * /* micro */ 1000);
    result.tv_usec = microseconds % (/* milli */ 1000 * /* micro */ 1000);
    return result;
}

/**
 * Convert the given time point to a human readable representation
 */
std::string to_string(const std::chrono::time_point<std::chrono::system_clock>& tp);


} // namespace
