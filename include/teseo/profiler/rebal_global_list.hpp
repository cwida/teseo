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
#include <cinttypes>

#include "teseo/profiler/rebal_list.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::profiler {

/**
 * The sequence of recordings, but maintain a sequence for each thread type
 */
class GlobalRebalanceList {
    std::array<RebalanceList, 3> m_lists; // recordings for each thread type
    std::array<uint64_t, 3> m_num_threads; // number of registered threads in each list
    util::Latch m_latch; // make the method #insert thread safe


public:
    /**
     * Constructor
     */
    GlobalRebalanceList();

    /**
     * Save the given recordings
     */
    void insert(const RebalanceList* list);

    /**
     * Dump the recording into a json
     */
    void to_json(std::ostream& out);
};

} // namespace
