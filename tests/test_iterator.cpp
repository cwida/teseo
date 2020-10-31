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
#include "teseo/memstore/segment.hpp"
#include "teseo/runtime/runtime.hpp"
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
TEST_CASE("iter_empty", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto iter = tx_ro.iterator();
    REQUIRE_THROWS_AS( iter.edges(10, false, [](uint64_t destination, double weight){  return true; }), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    iter = tx_rw.iterator();
    REQUIRE_THROWS_AS( iter.edges(10, false, [](uint64_t destination, double weight){  return true; }), LogicalError );
}

/**
 * Scan a node with no edges attached
 */
TEST_CASE("iter_zero_edges", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.commit();

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    uint64_t num_hits = 0;
    it_ro.edges(10, false, [&num_hits](uint64_t destination, double weight){ num_hits++; return true; });
    REQUIRE(num_hits == 0);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, [&num_hits](uint64_t destination, double weight){ num_hits++; return true; });
    REQUIRE(num_hits == 0);
}

/**
 * Scan a node with only one edge attached
 */
TEST_CASE("iter_one_edge", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        }
        num_hits++;
        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == 1);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == 1);
}

/**
 * A scan on a segment with two edges
 */
TEST_CASE("iter_two_edges", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 1){
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
        }
        num_hits++;
        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == 2);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == 2);
}

/**
 * Check a scan can skip over removed edges
 */
TEST_CASE("iter_removed_edges", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_edge(10, 20);
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_edge(10, 40); // non committed

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 30); // 10 -> 30 should be visible
            REQUIRE(weight == 1030);
        } else if(num_hits == 1){
            REQUIRE(destination == 40); // 10 -> 40 has not been removed yet, as the transaction did not commit
            REQUIRE(weight == 1040);
        }
        num_hits++;
        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == 2);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == 2);
}

/**
 * Check that a scan can be interrupted earlier
 */
TEST_CASE("iter_terminate1", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        num_hits++;
        if(num_hits == 1){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 2){
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
            return false;
        };

        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == 2);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == 2);
}

/**
 * Scan both the LHS & RHS of the segment #0
 */
TEST_CASE("iter_lhs_and_rhs", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 60;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        num_hits++;
        if(num_hits == 1){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 2){
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
        } else if(num_hits == 3){
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
        } else if(num_hits == 4){
            REQUIRE(destination == 50);
            REQUIRE(weight == 1050);
        }

        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == 4);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == 4);
}

/**
 * Scan over multiple segments (3), but still inside the same leaf
 */
TEST_CASE("iter_multiple_segments", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 200;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    const uint64_t expected_num_edges = max_vertex_id / 10 -1;

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        num_hits++;
        uint64_t expected_vertex_id = 10 + 10 * num_hits;
        double expected_weight = 1000 + expected_vertex_id;

        REQUIRE(destination == expected_vertex_id);
        REQUIRE(weight == expected_weight);

        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);
}

/**
 * Scan over multiple leaves (2)
 */
TEST_CASE("iter_multiple_leaves", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 400;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    const uint64_t expected_num_edges = max_vertex_id / 10 -1;

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        num_hits++;
        uint64_t expected_vertex_id = 10 + 10 * num_hits;
        double expected_weight = 1000 + expected_vertex_id;

        REQUIRE(destination == expected_vertex_id);
        REQUIRE(weight == expected_weight);

        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);
}

/**
 * Scan over multiple leaves, but terminate the range scan earlier, once we reached vertex 400
 */
TEST_CASE("iter_terminate2", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 600;
    uint64_t max_vertex_visited = 400;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    const uint64_t expected_num_edges = max_vertex_visited / 10 -1;

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    uint64_t num_hits = 0;
    auto check = [max_vertex_visited, &num_hits](uint64_t destination, double weight){
        num_hits++;
        uint64_t expected_vertex_id = 10 + 10 * num_hits;
        double expected_weight = 1000 + expected_vertex_id;

        REQUIRE(destination == expected_vertex_id);
        REQUIRE(weight == expected_weight);

        return destination < max_vertex_visited;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);
}

/**
 * Sparse file, one vertex, one edge in different stages: committed/uncommitted/removed
 */
TEST_CASE("iter_sparse1", "[iterator_sparse_file] [iterator]"){
    using namespace Catch::Matchers;

    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        }
        num_hits++;
        return true;
    };

    auto iter1_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE(num_hits == 0);
    auto iter1_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE(num_hits == 0);

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);

    // as tx started later than iter1, any change made should not be visible to tx1
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");

    // as tx is uncommitted, its changes should not be visible to iter2
    auto iter2_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    auto iter2_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");

    tx.commit();

    // the changes of tx should still not be visible to iter1 and iter2
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");

    // as iter3 started after the commit of tx, its changes should be now be visible
    auto iter3_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);

    auto iter3_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    tx = teseo.start_transaction();
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);

    // older iterators should report the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    // iter4 should see the same results of iter3 as tx did not commit yet
    auto iter4_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    auto iter4_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    tx.commit();

    // older iterators should report the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    // iter 5 should see the new edge
    auto iter5_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    auto iter5_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);


    tx = teseo.start_transaction();
    tx.remove_vertex(10);

    // older iterators should still see the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);

    // as tx did not commit yet, iter6 should see the same results as iter5
    auto iter6_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    auto iter6_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);

    tx.commit(); // vertex 10 removed

    // older iterators should still see the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);

    // finally iter7 should not see anymore vertex 10, as tx committed
    auto iter7_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    REQUIRE_THROWS_WITH(iter7_ro.edges(10, false, check), "The vertex 10 does not exist");
    auto iter7_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    REQUIRE_THROWS_WITH(iter7_rw.edges(10, false, check), "The vertex 10 does not exist");
}

/**
 * Sparse file, scan over different multiple edges in different states: inserted/uncommited/removed
 */
TEST_CASE("iter_sparse2", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t max_vertex_id = 60;

    auto tx1 = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx1.insert_vertex(vertex_id);
    }
    tx1.insert_edge(10, 20, 1020);
    tx1.insert_edge(10, 40, 1040);
    tx1.insert_edge(10, 50, 1050);
    tx1.insert_edge(10, 60, 1060);

    // fire a rebalance
    global_context()->runtime()->rebalance_first_leaf();

    // commit after the rebalance to avoid the version pruning
    tx1.commit();

    auto tx2 = teseo.start_transaction();
    tx2.remove_edge(10, 40);
    tx2.commit();

    auto tx3 = teseo.start_transaction();
    tx3.remove_edge(10, 50);
    // don't commit

    auto iter_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    auto iter_rw = teseo.start_transaction(/* read only ? */ false).iterator();

    auto tx4 = teseo.start_transaction();
    tx4.remove_edge(10, 60);
    tx4.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 20 is a plain old committed edge, it should be visible no problem
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 1){ // 10 -> 50 should still be visible, as tx3 did not commit
            REQUIRE(destination == 50);
            REQUIRE(weight == 1050);
        } else if(num_hits == 2){ // 10 -> 60 should still be visible, as tx4 started after iter
            REQUIRE(destination == 60);
            REQUIRE(weight == 1060);
        }

        num_hits++;
        return true;
    };

    num_hits = 0;
    REQUIRE_NOTHROW( iter_ro.edges(10, false, check) );
    REQUIRE(num_hits == 3);
    num_hits = 0;
    REQUIRE_NOTHROW( iter_rw.edges(10, false, check) );
    REQUIRE(num_hits == 3);
}

/**
 * Read-write transactions can alter their snapshot, but the opened iterators should
 * still remain consistent.
 * Test on a single segment, with one edge.
 */
TEST_CASE("iter_remove1", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 20
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);

            REQUIRE_NOTHROW( tx_rw.remove_edge(10, 30) ); // remove 10 -> 30
        } else if(num_hits == 1){ // 10 -> 40
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
        }

        num_hits++;
        return true;
    };
    iter.edges(10, false, check);
    REQUIRE(num_hits == 2);
}

/**
 * Alter the content of the snapshot, this time the edge removed is the same edge read by the iterator.
 */
TEST_CASE("iter_remove2", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 20
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);

            REQUIRE_NOTHROW( tx_rw.remove_edge(10, 20) ); // remove 10 -> 20, the edge just read
        } else if(num_hits == 1){ // 10 -> 30
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
        } else if(num_hits == 2){ // 10 -> 40
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
        }

        num_hits++;
        return true;
    };
    iter.edges(10, false, check);
    REQUIRE(num_hits == 3);
}

/**
 * Alter the content of the snapshot, remove the very first edge of the sequence
 */
TEST_CASE("iter_remove3", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 20
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 1){ // 10 -> 30
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
        } else if(num_hits == 2){ // 10 -> 40
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
            REQUIRE_NOTHROW( tx_rw.remove_edge(10, 20) ); // remove 10 -> 20, the very first edge of the sequence
        }

        num_hits++;
        return true;
    };
    iter.edges(10, false, check);
    REQUIRE(num_hits == 3);
}

/**
 * Alter the content of the snapshot, remove the last edge of the sequence
 */
TEST_CASE("iter_remove4", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 20
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 1){ // 10 -> 30
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
        } else if(num_hits == 2){ // 10 -> 40
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
            REQUIRE_NOTHROW( tx_rw.remove_edge(10, 40) ); // remove 10 -> 40, the edge just read
        }

        num_hits++;
        return true;
    };
    iter.edges(10, false, check);
    REQUIRE(num_hits == 3);
}

/**
 * Alter the content of the snapshot, remove the edges in the next segment, not yet visited
 */
TEST_CASE("iter_remove5", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_vertex(50);
    tx.insert_vertex(60);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);
    tx.insert_edge(10, 60, 1060);

    global_context()->runtime()->rebalance_first_leaf();
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        num_hits++;
        if(num_hits <= 3){
            uint64_t expected_vertex_id = 10 + num_hits * 10;
            uint64_t expected_weight = 1000 + expected_vertex_id;
            REQUIRE(destination == expected_vertex_id);
            REQUIRE(weight == expected_weight);
        }
        if(num_hits == 3){
            tx_rw.remove_edge(10, 50);
            tx_rw.remove_edge(10, 60);
        }

        return true;
    };
    iter.edges(10, false, check);
    REQUIRE(num_hits == 3);
}


/**
 * Insert a new edge in the snapshot. The new edge should be visible in the iterator.
 */
TEST_CASE("iter_insert1", "[iterator_sparse_file][iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 30, 10 -> 20 does not exist yet
            REQUIRE(destination == 30);
            REQUIRE(weight == 1030);
            tx_rw.insert_edge(10, 20, 1020);
            tx_rw.insert_edge(10, 40, 1040);
        } else if(num_hits == 1){ // 10 -> 40
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
        }

        num_hits++;
        return true;
    };
    iter.edges(10, false, check);
    REQUIRE(num_hits == 2);
}

/**
 * Insert multiple new edges in the snapshot
 */
TEST_CASE("iter_insert2", "[iterator_sparse_file][iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t max_vertex_id = 100;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        if(vertex_id % 20 == 0){
            tx.insert_edge(10, vertex_id, 1000 + vertex_id);
        }
    }
    tx.commit();

    global_context()->runtime()->rebalance_first_leaf();

    auto tx_rw = teseo.start_transaction(/* read-only ? */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&tx_rw, &num_hits](uint64_t destination, double weight){
        num_hits++;

        uint64_t expected_vertex_id = 10 + num_hits * 10;
        uint64_t expected_weight = 1000 + expected_vertex_id;
        REQUIRE( destination == expected_vertex_id );
        REQUIRE( weight == expected_weight );

        if(destination % 20 == 0 && (destination + 10) <= max_vertex_id){
            tx_rw.insert_edge(10, destination + 10, 1000 + destination + 10);
        }

        return true;
    };
    iter.edges(10, false, check);

    uint64_t expected_num_edges = max_vertex_id / 10 -1;
    REQUIRE(num_hits == expected_num_edges);
}

/*****************************************************************************
 *                                                                           *
 *   Iterator state                                                          *
 *                                                                           *
 *****************************************************************************/

/**
 * Do not allow to use an iterator after it has been explicitly closed
 */
TEST_CASE("iter_close1", "[iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto iter_ro = tx_ro.iterator();
    REQUIRE_NOTHROW(iter_ro.edges(10, false, [](uint64_t destination, double weight){ return true; }));
    iter_ro.close();
    REQUIRE_THROWS_WITH(iter_ro.edges(10, false, [](uint64_t destination, double weight){ return true; }), "The iterator is closed");

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    auto iter_rw = tx_rw.iterator();
    REQUIRE_NOTHROW(iter_rw.edges(10, false, [](uint64_t destination, double weight){ return true; }));
    iter_rw.close();
    REQUIRE_THROWS_WITH(iter_rw.edges(10, false, [](uint64_t destination, double weight){ return true; }), "The iterator is closed");
}

/**
 * Do not allow to close the iterator itself from the callback
 */
TEST_CASE("iter_close2", "[iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto iter_ro = tx_ro.iterator();
    REQUIRE_THROWS_WITH(iter_ro.edges(10, false, [&iter_ro](uint64_t destination, double weight){
        iter_ro.close();
        return true;
    }), "Cannot close the iterator while in use");

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    auto iter_rw = tx_rw.iterator();
    REQUIRE_THROWS_WITH(iter_rw.edges(10, false, [&iter_rw](uint64_t destination, double weight){
        iter_rw.close();
        return true;
    }), "Cannot close the iterator while in use");
}

/**
 * Do not allow to terminate the transaction while the iterator is being in use
 */
TEST_CASE("iter_terminate_transaction", "[iterator]"){
    using namespace Catch::Matchers;
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto iter_ro = tx_ro.iterator();
    // try with #commit
    REQUIRE_THROWS_WITH(
            iter_ro.edges(10, false, [&tx_ro](uint64_t destination, double weight){
        tx_ro.commit();
        return true;
    }), Contains("The transaction cannot be terminated") ) ;
    // try with #rollback
    REQUIRE_THROWS_WITH(iter_ro.edges(10, false, [&tx_ro](uint64_t destination, double weight){
        tx_ro.rollback();
        return true;
    }), Contains("The transaction cannot be terminated") );

    // as iter_ro is still in scope
    REQUIRE_THROWS_WITH( tx_ro.commit(), Contains("The transaction cannot be terminated"));
    REQUIRE_THROWS_WITH( tx_ro.rollback(), Contains("The transaction cannot be terminated"));

    // finally, close the iterator
    iter_ro.close();
    REQUIRE_NOTHROW( tx_ro.commit() );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    auto iter_rw = tx_rw.iterator();
    // try with #commit
    REQUIRE_THROWS_WITH(
            iter_rw.edges(10, false, [&tx_rw](uint64_t destination, double weight){
        tx_rw.commit();
        return true;
    }), Contains("The transaction cannot be terminated") ) ;
    // try with #rollback
    REQUIRE_THROWS_WITH(iter_rw.edges(10, false, [&tx_rw](uint64_t destination, double weight){
        tx_rw.rollback();
        return true;
    }), Contains("The transaction cannot be terminated") );

    // as iter_rw is still in scope
    REQUIRE_THROWS_WITH( tx_rw.commit(), Contains("The transaction cannot be terminated"));
    REQUIRE_THROWS_WITH( tx_rw.rollback(), Contains("The transaction cannot be terminated"));

    // finally, close the iterator
    iter_rw.close();
    REQUIRE_NOTHROW( tx_rw.commit() );
}

/**
 * Read only transaction insert or remove edges in their snapshot and should not be able to alter
 * an iterator.
 */

TEST_CASE("iter_read_only", "[iterator]"){
    using namespace Catch::Matchers;
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto iter_ro = tx_ro.iterator();

    REQUIRE_THROWS_AS(tx_ro.remove_edge(10, 30), LogicalError);
    REQUIRE_THROWS_WITH(tx_ro.remove_edge(10, 30), Contains("the transaction is read only"));

    uint64_t num_hits = 0;
    iter_ro.edges(10, false, [&tx_ro, &num_hits](uint64_t destination, double weight){
        REQUIRE_THROWS_AS(tx_ro.remove_edge(10, 30), LogicalError);
        REQUIRE_THROWS_WITH(tx_ro.remove_edge(10, 30), Contains("the transaction is read only"));

        num_hits ++;
        uint64_t expected_vertex_id = 10 + num_hits * 10;
        uint64_t expected_weight = expected_vertex_id + 1000;
        REQUIRE(destination == expected_vertex_id);
        REQUIRE(weight == expected_weight);

        return true;
    });

    REQUIRE(num_hits == 3);
    REQUIRE_THROWS_AS(tx_ro.remove_edge(10, 30), LogicalError); // still ...
    REQUIRE_THROWS_WITH(tx_ro.remove_edge(10, 30), Contains("the transaction is read only"));
}

/*****************************************************************************
 *                                                                           *
 *   Dense file                                                              *
 *                                                                           *
 *****************************************************************************/

/**
 * Dense file, scan over one vertex and one edge
 */
TEST_CASE("iter_dense1", "[iterator_dense_file] [iterator]"){
    using namespace Catch::Matchers;

    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    {   // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        }
        num_hits++;
        return true;
    };

    auto iter1_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE(num_hits == 0);
    auto iter1_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE(num_hits == 0);

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);

    // as tx started later than iter1, any change made should not be visible to tx1
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");

    // as tx is uncommitted, its changes should not be visible to iter2
    auto iter2_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    auto iter2_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");

    tx.commit();

    // the changes of tx should still not be visible to iter1 and iter2
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");

    // as iter3 started after the commit of tx, its changes should be now be visible
    auto iter3_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);

    auto iter3_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    tx = teseo.start_transaction();
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);

    // older iterators should report the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    // iter4 should see the same results of iter3 as tx did not commit yet
    auto iter4_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    auto iter4_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    tx.commit();

    // older iterators should report the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);

    // iter 5 should see the new edge
    auto iter5_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    auto iter5_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);


    tx = teseo.start_transaction();
    tx.remove_vertex(10);

    // older iterators should still see the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);

    // as tx did not commit yet, iter6 should see the same results as iter5
    auto iter6_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    auto iter6_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);

    tx.commit(); // vertex 10 removed

    // older iterators should still see the same results as before
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter1_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_ro.edges(10, false, check), "The vertex 10 does not exist");
    REQUIRE_THROWS_WITH(iter2_rw.edges(10, false, check), "The vertex 10 does not exist");
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter3_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_ro.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter4_rw.edges(10, false, check));
    REQUIRE(num_hits == 0);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter5_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_ro.edges(10, false, check));
    REQUIRE(num_hits == 1);
    num_hits = 0;
    REQUIRE_NOTHROW(iter6_rw.edges(10, false, check));
    REQUIRE(num_hits == 1);

    // finally iter7 should not see anymore vertex 10, as tx committed
    auto iter7_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    REQUIRE_THROWS_WITH(iter7_ro.edges(10, false, check), "The vertex 10 does not exist");
    auto iter7_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    REQUIRE_THROWS_WITH(iter7_rw.edges(10, false, check), "The vertex 10 does not exist");
}


/**
 * Dense file, scan over different multiple edges in different states
 */
TEST_CASE("iter_dense2", "[iterator_dense_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t max_vertex_id = 60;

    {   // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx1 = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx1.insert_vertex(vertex_id);
    }
    tx1.insert_edge(10, 20, 1020);
    tx1.insert_edge(10, 40, 1040);
    tx1.insert_edge(10, 50, 1050);
    tx1.insert_edge(10, 60, 1060);
    tx1.commit();

    auto tx2 = teseo.start_transaction();
    tx2.remove_edge(10, 40);
    tx2.commit();

    auto tx3 = teseo.start_transaction();
    tx3.remove_edge(10, 50);
    // don't commit

    auto iter_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    auto iter_rw = teseo.start_transaction(/* read only ? */ false).iterator();

    auto tx4 = teseo.start_transaction();
    tx4.remove_edge(10, 60);
    tx4.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        if(num_hits == 0){ // 10 -> 20 is a plain old committed edge, it should be visible no problem
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
        } else if(num_hits == 1){ // 10 -> 50 should still be visible, as tx3 did not commit
            REQUIRE(destination == 50);
            REQUIRE(weight == 1050);
        } else if(num_hits == 2){ // 10 -> 60 should still be visible, as tx4 started after iter
            REQUIRE(destination == 60);
            REQUIRE(weight == 1060);
        }

        num_hits++;
        return true;
    };

    num_hits = 0;
    REQUIRE_NOTHROW( iter_ro.edges(10, false, check) );
    REQUIRE(num_hits == 3);
    num_hits = 0;
    REQUIRE_NOTHROW( iter_rw.edges(10, false, check) );
    REQUIRE(num_hits == 3);
}

/**
 * Alter the snapshot while the iterator is running
 */
TEST_CASE("iter_dense3", "[iterator_dense_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    {   // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_vertex(50);
    tx.insert_vertex(60);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 50, 1050);
    tx.insert_edge(10, 60, 1060);
    tx.commit();


    tx = teseo.start_transaction(); // noise
    auto tx_rw = teseo.start_transaction(/* read only */ false);
    auto iter = tx_rw.iterator();

    uint64_t num_hits = 0;
    auto check = [&num_hits, &tx_rw, &tx](uint64_t destination, double weight){
        if(num_hits == 0){
            REQUIRE(destination == 20);
            REQUIRE(weight == 1020);
            tx.insert_edge(10, 30, 1030);
            tx.remove_edge(10, 50);
            tx_rw.insert_edge(10, 40, 1040);
            tx_rw.remove_edge(10, 60);
        } else if(num_hits == 1){
            REQUIRE(destination == 40);
            REQUIRE(weight == 1040);
        } else if(num_hits == 2){
            REQUIRE(destination == 50);
            REQUIRE(weight == 1050);
        }

        num_hits++;
        return true;
    };

    iter.edges(10, false, check);
    REQUIRE(num_hits == 3);
}

/**
 * Scan over multiple leaves (2). The first and the third segment are dense files
 */
TEST_CASE("iter_mixed", "[iterator_dense_file] [iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 400;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    const uint64_t expected_num_edges = max_vertex_id / 10 -1;

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    { // make the first and fourth segment a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(3);
        Segment::to_dense_file(context);
    }

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        num_hits++;
        uint64_t expected_vertex_id = 10 + 10 * num_hits;
        double expected_weight = 1000 + expected_vertex_id;

        REQUIRE(destination == expected_vertex_id);
        REQUIRE(weight == expected_weight);

        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);
}


/**
 * Iterator interface, check that we can specify as callback simply void fn(uint64_t) to access
 * the destination vertices without the vertices
 */
TEST_CASE("iter_api_no_weights", "[iterator]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 400;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    const uint64_t expected_num_edges = max_vertex_id / 10 -1;

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination){
        num_hits++;
        uint64_t expected_vertex_id = 10 + 10 * num_hits;
        REQUIRE(destination == expected_vertex_id);

        // no return
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, false, check);
    REQUIRE(num_hits == expected_num_edges);
}

