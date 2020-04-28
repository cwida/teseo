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
#include "catch.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;

TEST_CASE( "segment_set_state", "[segment]" ) {
    Teseo teseo;
    ScopedEpoch epoch;
    Segment* segment = global_context()->memstore()->index()->find(0, 0).leaf()->get_segment(0);

    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());

    segment->set_state(Segment::State::READ);
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(!segment->has_requested_rebalance());

    segment->set_state(Segment::State::WRITE);
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(!segment->has_requested_rebalance());

    segment->set_state(Segment::State::REBAL);
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::REBAL);
    REQUIRE(!segment->has_requested_rebalance());

    segment->set_state(Segment::State::FREE);
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());
}


