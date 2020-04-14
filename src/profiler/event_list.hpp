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

#include <array>
#include <chrono>
#include "third-party/magic_enum.hpp"
#include "event_name.hpp"

namespace teseo::internal::profiler {

struct EventData {
    EventData(const EventData&) = delete;
    EventData& operator=(const EventData& ) = delete;
    EventData() { }

    std::chrono::microseconds m_total_time;
    uint64_t m_num_scoped_timers = 0;
    uint64_t m_num_invocations = 0;
};

using EventList = std::array<EventData, magic_enum::enum_count<EventName>()>;

} // namaspace
