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

#include <cinttypes>
#include <ostream>
#include <string>

namespace teseo::memstore {

/**
 * This class provides information about the current state of a segment's latch. It is only used
 * for debugging and testing purposing.
 */
class LatchState {
public:
    const bool m_invalid = false; // true if the segment is part of a leaf that has been deleted, due to a resize or a merge.
    const bool m_xlock = false; // true if an xlock is currently acquired in the segment. That is some thread is changing the segment meta-information, e.g. managing the waiting queue.
    const bool m_writer = false; // true if a writer is currently operating in the segment.
    const bool m_rebalancer = false; // true if a rebalancer is currently operating in the segment.
    const bool m_wait = false; // true if there is at least one thread waiting in the queue
    const uint64_t m_readers = 0; // the current number of readers operating in the segment
    const uint64_t m_version = 0; // the current version of the latch, incremented after each write in the segment

    /**
     * Init the class with the content of the latch
     */
    LatchState(uint64_t latch);

    /**
     * Retrieve a string representation of this instance
     */
    std::string to_string() const;
};

// Dump the content of the instance to the given output stream
std::ostream& operator<<(std::ostream& out, const LatchState& ls);

}
