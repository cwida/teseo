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
//#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
//#include "teseo/memstore/error.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/rebalance/crawler.hpp"
//#include "teseo/rebalance/merger_service.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/runtime/runtime.hpp"
//#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;


/*****************************************************************************
 *                                                                           *
 *   Sparse segment (ssf)                                                    *
 *                                                                           *
 *****************************************************************************/

/**
 * Validate the degree on an empty segment
 */
TEST_CASE("ssf_empty", "[ssf][degree]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE_THROWS_AS( tx_ro.degree(10), LogicalError );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE_THROWS_AS( tx_rw.degree(10), LogicalError );
}

/**
 * Validate the degree on a segment with a single vertex, with no edges attached
 */
TEST_CASE("ssf_single1", "[ssf][degree]"){
    Teseo teseo;
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
TEST_CASE("ssf_single2", "[ssf][degree]"){
    Teseo teseo;
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
TEST_CASE("ssf_single3", "[ssf][degree]"){
    Teseo teseo;
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
TEST_CASE("ssf_single4", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

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
TEST_CASE("ssf_lhs1", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

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
TEST_CASE("ssf_lhs2", "[ssf][degree]"){
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
TEST_CASE("ssf_lhs3", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

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
TEST_CASE("ssf_rhs1", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(20) == 1 ); // 20 -> 10

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(20) == 1 ); // 20 -> 10
}
/**
 * Validate the degree on the RHS of a segment, with a removed edge
 */
TEST_CASE("ssf_rhs2", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

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
TEST_CASE("ssf_rhs3", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

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
TEST_CASE("ssf_rhs4", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 50;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(30, 40, 3040);
    tx.insert_edge(30, 50, 3050);

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

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
TEST_CASE("ssf_segment1", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 60;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

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
TEST_CASE("ssf_segment2", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 100;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 4 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 4 );
}

/**
 * Validate the degree with a vertex spanning two segments
 */
TEST_CASE("ssf_multiple_segments1", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
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

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 5 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 5 );
}

/**
 * Validate the degree with a vertex spanning four segments
 */
TEST_CASE("ssf_multiple_segments2", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 300;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 29 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 29 );
}

/**
 * Validate the degree with a vertex spanning four segments, the last edge at the border of the first leaf
 */
TEST_CASE("ssf_multiple_segments3", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 320;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 31 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 31 );
}

/**
 * Validate the degree with a vertex spanning multiple leaves
 */
TEST_CASE("ssf_multiple_leaves", "[ssf][degree]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_verted_id = 1000;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    { // manually rebalance

        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf =  memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    REQUIRE( tx_ro.degree(10) == 99 );

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    REQUIRE( tx_rw.degree(10) == 99 );
}


/*****************************************************************************
 *                                                                           *
 *   Dense segment (dsf)                                                     *
 *                                                                           *
 *****************************************************************************/

