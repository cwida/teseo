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
//#include "teseo/context/scoped_epoch.hpp"
//#include "teseo/memstore/context.hpp"
//#include "teseo/memstore/dense_file.hpp"
//#include "teseo/memstore/index.hpp"
//#include "teseo/memstore/leaf.hpp"
//#include "teseo/memstore/memstore.hpp"
//#include "teseo/memstore/segment.hpp"
//#include "teseo/memstore/sparse_file.hpp"
//#include "teseo/rebalance/crawler.hpp"
//#include "teseo/rebalance/plan.hpp"
//#include "teseo/rebalance/scratchpad.hpp"
//#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/runtime/runtime.hpp"
//#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;


/*****************************************************************************
 *                                                                           *
 *   Sparse segment                                                          *
 *                                                                           *
 *****************************************************************************/
/**
 * Validate a scan over an empty segment
 */
TEST_CASE("scan_empty", "[scan_sparse_file][scan]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS( tx_ro.scan_out(10, [](uint64_t destination, double weight){  return true; }), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS( tx_rw.scan_out(10, [](uint64_t destination, double weight){  return true; }), LogicalError );
}

/**
 * A scan on a segment with two edges
 */
TEST_CASE("scan_two_edges"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    uint64_t num_hits = 0;
    tx_ro.scan_out(10, [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 1){
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
        }
        num_hits++;
        return true;
    });
    REQUIRE(num_hits == 2);
}
