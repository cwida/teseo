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

#include "teseo/rebalance/rebalance.hpp"

#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/rebalance/merge_operator.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

namespace teseo::rebalance {

void handle_rebalance(memstore::Memstore* memstore, memstore::Key& key) {
    profiler::ScopedTimer profiler { profiler::ARS_HANDLE_REQUEST };
    COUT_DEBUG("Key: " << key);
    context::ScopedEpoch epoch; // protect from the GC

    try {
        memstore::Context context { memstore };
        Crawler crawler { context, key };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality_ub() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    } catch (Abort) {
        /* nop */
    } catch (RebalanceNotNecessary){
        /* nop */
    }
}

} // namespace
