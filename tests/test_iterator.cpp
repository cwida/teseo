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
TEST_CASE("iter_empty", "[iterator_sparse_file] [iterator]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto iter = tx_ro.iterator();
    REQUIRE_THROWS_AS( iter.edges(10, [](uint64_t destination, double weight){  return true; }), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    iter = tx_rw.iterator();
    REQUIRE_THROWS_AS( iter.edges(10, [](uint64_t destination, double weight){  return true; }), LogicalError );
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
    it_ro.edges(10, [&num_hits](uint64_t destination, double weight){ num_hits++; return true; });
    REQUIRE(num_hits == 0);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, [&num_hits](uint64_t destination, double weight){ num_hits++; return true; });
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == 1);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == 2);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == 2);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == 2);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == 4);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
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
    it_ro.edges(10, check);
    REQUIRE(num_hits == expected_num_edges);

    auto it_rw = teseo.start_transaction(/* read only ? */ false).iterator();
    num_hits = 0;
    it_rw.edges(10, check);
    REQUIRE(num_hits == expected_num_edges);
}

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
    REQUIRE_NOTHROW(iter_ro.edges(10, [](uint64_t destination, double weight){ return true; }));
    iter_ro.close();
    REQUIRE_THROWS_WITH(iter_ro.edges(10, [](uint64_t destination, double weight){ return true; }), "The iterator is closed");

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    auto iter_rw = tx_rw.iterator();
    REQUIRE_NOTHROW(iter_rw.edges(10, [](uint64_t destination, double weight){ return true; }));
    iter_rw.close();
    REQUIRE_THROWS_WITH(iter_rw.edges(10, [](uint64_t destination, double weight){ return true; }), "The iterator is closed");
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
    REQUIRE_THROWS_WITH(iter_ro.edges(10, [&iter_ro](uint64_t destination, double weight){
        iter_ro.close();
        return true;
    }), "Cannot close the iterator while in use");

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    auto iter_rw = tx_rw.iterator();
    REQUIRE_THROWS_WITH(iter_rw.edges(10, [&iter_rw](uint64_t destination, double weight){
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
            iter_ro.edges(10, [&tx_ro](uint64_t destination, double weight){
        tx_ro.commit();
        return true;
    }), Contains("The transaction cannot be terminated") ) ;
    // try with #rollback
    REQUIRE_THROWS_WITH(iter_ro.edges(10, [&tx_ro](uint64_t destination, double weight){
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
            iter_rw.edges(10, [&tx_rw](uint64_t destination, double weight){
        tx_rw.commit();
        return true;
    }), Contains("The transaction cannot be terminated") ) ;
    // try with #rollback
    REQUIRE_THROWS_WITH(iter_rw.edges(10, [&tx_rw](uint64_t destination, double weight){
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

