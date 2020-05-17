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
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/util/thread.hpp"
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
 * Validate the degree on an empty segment
 */
TEST_CASE("degree_empty", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually


    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS( tx_ro.degree(10), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS( tx_rw.degree(10), LogicalError );
}

/**
 * Validate the degree on a segment with a single vertex, with no edges attached
 */
TEST_CASE("degree_single1", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10)  == 0 );
    REQUIRE_THROWS_AS( tx_ro.degree(5), LogicalError );
    REQUIRE_THROWS_AS( tx_ro.degree(15), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10)  == 0 );
    REQUIRE_THROWS_AS( tx_rw.degree(5), LogicalError );
    REQUIRE_THROWS_AS( tx_rw.degree(15), LogicalError );
}

/**
 * Validate the degree on a segment with a single vertex, removed but uncommitted
 */
TEST_CASE("degree_single2", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(10);

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10)  == 0 );
    REQUIRE_THROWS_AS( tx_ro.degree(5), LogicalError );
    REQUIRE_THROWS_AS( tx_ro.degree(15), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10)  == 0 );
    REQUIRE_THROWS_AS( tx_rw.degree(5), LogicalError );
    REQUIRE_THROWS_AS( tx_rw.degree(15), LogicalError );
}

/**
 * Validate the degree on a segment with one non committed vertex
 */
TEST_CASE("degree_single3", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS( tx_ro.degree(10), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS( tx_rw.degree(10), LogicalError);

}

/**
 * Validate the degree on a segment with two vertices and one edge attached
 */
TEST_CASE("degree_single4", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 1 );
    REQUIRE( tx_ro.degree(20) == 1 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 1 );
    REQUIRE( tx_ro.degree(20) == 1 );
}

/**
 * Validate the degree on the LHS of a segment
 */
TEST_CASE("degree_lhs1", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 2 );
    REQUIRE( tx_ro.degree(20) == 1 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 2 );
    REQUIRE( tx_ro.degree(20) == 1 );
}

/**
 * Validate the degree on the LHS of a non dirty segment
 */
TEST_CASE("degree_lhs2", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    {
        context::ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        Context::sparse_file(leaf, 0)->prune();
    }

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 3 );
    REQUIRE( tx_ro.degree(20) == 1 );
    REQUIRE( tx_ro.degree(30) == 1 );
    REQUIRE( tx_ro.degree(40) == 1 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 3 );
    REQUIRE( tx_ro.degree(20) == 1 );
    REQUIRE( tx_ro.degree(30) == 1 );
    REQUIRE( tx_ro.degree(40) == 1 );
}

/**
 * Validate the degree on the LHS of a segment, multiple edges in different states (committed/uncommitted/removed)
 */
TEST_CASE("degree_lhs3", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(30);
    tx.insert_edge(10, 30, 1020);
    tx.commit();

    {
        context::ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        Context::sparse_file(leaf, 0)->prune();
    }

    tx = teseo.start_transaction();
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.insert_vertex(40);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(20); // commit
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(30); // non committed

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 2 ); // 10 -> 30 and 10 -> 40
    REQUIRE_THROWS_AS( tx_ro.degree(20), LogicalError);
    REQUIRE( tx_ro.degree(30) == 1 ); // 30 -> 10

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 2 ); // 10 -> 30 and 10 -> 40
    REQUIRE_THROWS_AS( tx_rw.degree(20), LogicalError);
    REQUIRE( tx_rw.degree(30) == 1 ); // 30 -> 10
}

/**
 * Validate the degree on the RHS of a segment, simple case with committed transactions
 */
TEST_CASE("degree_rhs1", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(20) == 1 ); // 20 -> 10

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(20) == 1 ); // 20 -> 10
}
/**
 * Validate the degree on the RHS of a segment, with a removed edge
 */
TEST_CASE("degree_rhs2", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();
    tx = teseo.start_transaction();
    tx.remove_edge(10, 20);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(20) == 0 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(20) == 0 );
}

/**
 * Validate the degree on the RHS of a segment, with a whole vertex removed
 */
TEST_CASE("degree_rhs3", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();
    tx = teseo.start_transaction();
    tx.remove_vertex(20);
    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS( tx_ro.degree(20), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS( tx_rw.degree(20), LogicalError );
}

/**
 * Validate the degree on the RHS of a segment, with a whole vertex removed but the transaction is not committed
 */
TEST_CASE("degree_rhs4", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();
    tx = teseo.start_transaction();
    tx.remove_vertex(20);

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(20) == 1 ); // 20 -> 10

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(20) == 1 ); // 20 -> 10
}

/**
 * Validate the degree of a node spanning both the LHS and RHS
 */
TEST_CASE("degree_segment1", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 60;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 4 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 4 );
}

/**
 * Validate the degree of a node spanning both the LHS and RHS
 *
 * The last edge of the vertex is also the last edge of the first segment.
 */
TEST_CASE("degree_segment2", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 100;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 4 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 4 );
}

/**
 * Validate the degree with a vertex spanning two segments
 */
TEST_CASE("degree_multiple_segments1", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 100;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);
    tx.insert_edge(10, 60, 1060);

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 5 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 5 );
}

/**
 * Validate the degree with a vertex spanning four segments
 */
TEST_CASE("degree_multiple_segments2", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 300;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 29 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 29 );
}

/**
 * Validate the degree with a vertex spanning four segments, the last edge at the border of the first leaf
 */
TEST_CASE("degree_multiple_segments3", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 320;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 31 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 31 );
}

/**
 * Validate the degree with a vertex spanning multiple leaves
 */
TEST_CASE("degree_multiple_leaves", "[degree_sparse_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    uint64_t max_verted_id = 1000;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 99 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 99 );
}


/*****************************************************************************
 *                                                                           *
 *   Dense segment                                                           *
 *                                                                           *
 *****************************************************************************/

/**
 * Dense file, check the degree with an empty or a single vertex
 */
TEST_CASE("degree_dense1", "[degree_dense_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    {   // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx1_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS(tx1_ro.degree(10), LogicalError);
    auto tx1_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS(tx1_rw.degree(10), LogicalError);

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);

    // as tx started later than tx1, any change made should not be visible to tx1
    REQUIRE_THROWS_AS(tx1_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx1_rw.degree(10), LogicalError);

    // as tx is uncommitted, its changes should not be visible to tx2
    auto tx2_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);
    auto tx2_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);


    tx.commit();

    // as tx started later than tx1, any change made should not be visible to tx1
    REQUIRE_THROWS_AS(tx1_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx1_rw.degree(10), LogicalError);

    // as before, tx did not commit yet when tx2 started
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);

    // as tx3 started after the commit of tx, its changes should be now be visible
    auto tx3_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE(tx3_ro.degree(10) == 0);
    auto tx3_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE(tx3_rw.degree(10) == 0);

    tx = teseo.start_transaction();
    tx.remove_vertex(10);

    // the new tx cannot change the snapshot or the previous results obtained with tx1, tx2 and tx3
    REQUIRE_THROWS_AS(tx1_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx1_rw.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);
    REQUIRE(tx3_ro.degree(10) == 0);
    REQUIRE(tx3_rw.degree(10) == 0);

    // because tx did not commit yet, its changes should not be visible yet to new transactions
    auto tx4_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE(tx4_ro.degree(10) == 0);
    auto tx4_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE(tx4_rw.degree(10) == 0);

    tx.commit();

    // as before, tx[1-4] should see the same results as before
    REQUIRE_THROWS_AS(tx1_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx1_rw.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);
    REQUIRE_THROWS_AS(tx2_ro.degree(10), LogicalError);
    REQUIRE(tx3_ro.degree(10) == 0);
    REQUIRE(tx3_rw.degree(10) == 0);
    REQUIRE(tx4_ro.degree(10) == 0);
    REQUIRE(tx4_rw.degree(10) == 0);

    // newer transactions should not be able to see the vertex 10
    auto tx5_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS(tx5_ro.degree(10), LogicalError);
    auto tx5_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS(tx5_rw.degree(10), LogicalError);
}

/**
 * Dense file, check the degree with multiple vertices
 */
TEST_CASE("degree_dense2", "[degree_dense_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    const uint64_t max_vertex_id = 100;

    {   // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();

    auto tx1_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE(tx1_ro.degree(10) == 9);
    REQUIRE(tx1_ro.degree(20) == 1); // 10 -> 20
    REQUIRE(tx1_ro.degree(100) == 1); // 10 -> 100
    auto tx1_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE(tx1_rw.degree(10) == 9);
    REQUIRE(tx1_rw.degree(20) == 1); // 10 -> 20
    REQUIRE(tx1_rw.degree(100) == 1); // 10 -> 100

    tx = teseo.start_transaction();
    tx.remove_vertex(20);
    tx.commit();

    // tx1 started before than tx, its changes should not be visible
    REQUIRE(tx1_ro.degree(10) == 9);
    REQUIRE(tx1_ro.degree(20) == 1); // 10 -> 20
    REQUIRE(tx1_ro.degree(100) == 1); // 10 -> 100
    REQUIRE(tx1_rw.degree(10) == 9);
    REQUIRE(tx1_rw.degree(20) == 1); // 10 -> 20
    REQUIRE(tx1_rw.degree(100) == 1); // 10 -> 100

    auto tx2_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE(tx2_ro.degree(10) == 8);
    REQUIRE_THROWS_AS(tx2_ro.degree(20), LogicalError);
    REQUIRE(tx2_ro.degree(100) == 1); // 10 -> 100
    auto tx2_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE(tx2_rw.degree(10) == 8);
    REQUIRE_THROWS_AS(tx2_rw.degree(20), LogicalError);
    REQUIRE(tx2_rw.degree(100) == 1); // 10 -> 100
}

/**
 * Mixed, check the degree of a vertex whose edges span multiple dense & sparse files
 */
TEST_CASE("degree_mixed", "[degree_dense_file][degree]"){
    Teseo teseo;
    global_context()->disable_aux_degree(); // don't rely on the aux snapshot
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    const uint64_t max_vertex_id = 400;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    // manually rebalance
    global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    { // make the second and fourth segment a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(3);
        Segment::to_dense_file(context);
    }

    uint64_t expected_degree = max_vertex_id / 10 - 1;
    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE(tx_ro.degree(10) == expected_degree);
    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE(tx_rw.degree(10) == expected_degree);
}
