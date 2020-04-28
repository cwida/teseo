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

#include "teseo/profiler/rebal_profiler.hpp"

#include <cassert>
#include <chrono>

#include "teseo/context/thread_context.hpp"
#include "teseo/profiler/rebal_list.hpp"
#include "teseo/rebalance/plan.hpp"

using namespace std;
using namespace std::chrono;

namespace teseo::profiler {

#if defined(HAVE_PROFILER)

RebalanceProfiler::RebalanceProfiler(const rebalance::Plan& plan) : m_time_created(steady_clock::now()) {
    m_fields.m_window_length = plan.num_output_segments();
    if(plan.is_split()){
        m_fields.m_type = RebalanceType::SPLIT;
    } else if(plan.is_spread()){
        m_fields.m_type = RebalanceType::REBALANCE;
    } else {
        assert(plan.is_merge());
        m_fields.m_type = RebalanceType::MERGE;
    }

    assert(duration_cast<microseconds>(m_fields.m_load_time).count() < 10000);
}

RebalanceProfiler::~RebalanceProfiler(){
    m_fields.m_total_time = steady_clock::now() - m_time_created;
    context::thread_context()->profiler_rebalances()->insert(m_fields);
}

#endif

} // namespace
