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
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/rebalance/merger_service.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;

static_assert(StaticConfiguration::test_mode, "Reconfigure in test mode (configure --enable-test), otherwise the sparse arrays are too large for these tests");

/**
 * Insert some vertices in the sparse array, but don't trigger a rebalance
 */
TEST_CASE("sf_vertex_insert", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(20);
    tx.commit();

    //global_context()->memstore()->dump();

    tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.commit();

    tx = teseo.start_transaction();
    tx.insert_vertex(30);
    tx.commit();

    tx = teseo.start_transaction();
    REQUIRE( tx.has_vertex(10) == true );
    REQUIRE( tx.has_vertex(20) == true );
    REQUIRE( tx.has_vertex(30) == true );
    REQUIRE( tx.has_vertex(5) == false );
    REQUIRE( tx.has_vertex(15) == false );
    REQUIRE( tx.has_vertex(25) == false );
    REQUIRE( tx.has_vertex(35) == false );

    //global_context()->memstore()->dump();
}


/**
 * Try to insert an edge in the sparse array
 */
TEST_CASE("sf_edge_insert_lhs", "[sf] [memstore]"){
    Teseo teseo;

    auto tx = teseo.start_transaction();
    REQUIRE_NOTHROW( tx.insert_vertex(20) );
    REQUIRE_NOTHROW( tx.insert_vertex(10) );
    REQUIRE_NOTHROW( tx.insert_edge(20, 10, 1020));
    REQUIRE_THROWS_WITH( tx.insert_edge(10, 20, 2010), Catch::Contains("The edge") && Catch::Contains("already exists") ); // already inserted
    REQUIRE_THROWS_WITH( tx.insert_edge(10, 30, 2010), Catch::Contains("vertex") && Catch::Contains("does not exist") ); // the vertex 30 does not exist
    REQUIRE_NOTHROW( tx.commit() );

    //global_context()->storage()->dump();
}

/**
 * Insert some edges in the right hand side of a segment.
 */
TEST_CASE("sf_edge_insert_rhs", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    for(uint64_t vertex_id = 10; vertex_id <= 100; vertex_id += 10){
        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );
        REQUIRE_NOTHROW( tx.commit() );
    }

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        context.m_leaf = leaf;
        Segment* segment = leaf->get_segment(0);
        context.m_segment = segment;
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.window_start() == 0);
        REQUIRE(plan.window_length() == 2);
        REQUIRE(plan.cardinality() == 10);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    //memstore->dump();

    for(uint64_t vertex_id = 50; vertex_id <= 100; vertex_id += 10){
        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_edge(40, vertex_id, 1000 + vertex_id ) );

        for(uint64_t candidate = 50; candidate <= 100; candidate += 10){
            bool expected_result = candidate <= vertex_id;
            REQUIRE( tx.has_edge(40, candidate) == expected_result );
            REQUIRE( tx.has_edge(candidate, 40) == expected_result ); // because the graph is undirected
        }

        REQUIRE_NOTHROW( tx.commit() );
    }

    //memstore->dump();
}

/**
 * Validate roll back on a small chain of updates
 */
TEST_CASE("sf_rollback1", "[sf] [memstore]"){
    Teseo teseo;

    { // insert a few vertices, but do not commit
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 0 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(10) );
        REQUIRE( tx.num_vertices() == 1 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(20) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 0 );
        tx.rollback();
    }

    { // insert a few vertices and one edge, but do not commit
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 0 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(10) );
        REQUIRE( tx.num_vertices() == 1 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(20) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_edge(20, 10, 2010) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 1 );
        tx.rollback();
    }

    { // insert a few vertices, then insert an edge, remove it and reinsert it again, and rollback
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 0 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(10) );
        REQUIRE( tx.num_vertices() == 1 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(20) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_edge(20, 10, 2010) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 1 );
        REQUIRE_NOTHROW( tx.remove_edge(10, 20) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_edge(20, 10, 20100) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 1 );
        tx.rollback();
    }

    { // validate the last rollback
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 0 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE( tx.has_vertex(10) == false );
        REQUIRE( tx.has_vertex(20) == false );
        REQUIRE( tx.has_edge(10, 20) == false );
        REQUIRE( tx.has_edge(20, 10) == false );
    }
}


/**
 * Validate roll back over multiple segments, vertices only
 */
TEST_CASE("sf_rollback2", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    Transaction tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= 100; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        context.m_leaf = leaf;
        Segment* segment = leaf->get_segment(0);
        context.m_segment = segment;
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.window_start() == 0);
        REQUIRE(plan.window_length() == 2);
        REQUIRE(plan.cardinality() == 10);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    REQUIRE( tx.num_vertices() == 10 );
    REQUIRE( tx.num_edges() == 0 );
    tx.rollback();

    //memstore->dump();

    tx = teseo.start_transaction();
    REQUIRE( tx.num_vertices() == 0 );
    REQUIRE( tx.num_edges() == 0 );
    for(uint64_t vertex_id = 10; vertex_id <= 100; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
    }
}


/**
 * Validate roll back over multiple segments, vertices and edges
 */
TEST_CASE("sf_rollback3", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();
    constexpr uint64_t MAX_VERTEX_ID = 100;

    Transaction tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );
    }
    for(uint64_t vertex_id = 20; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE_NOTHROW( tx.insert_edge(10, vertex_id, (double) 1000 + vertex_id) );
    }
    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        context.m_leaf = leaf;
        Segment* segment = leaf->get_segment(0);
        context.m_segment = segment;
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

    REQUIRE( tx.num_vertices() == 10 );
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == true );
    }
    for(uint64_t vertex_id = 20; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_edge(10, vertex_id) == true );
        REQUIRE( tx.has_edge(vertex_id, 10) == true );
    }

    tx.rollback();

    //memstore->dump();

    tx = teseo.start_transaction();
    REQUIRE( tx.num_vertices() == 0 );
    REQUIRE( tx.num_edges() == 0 );
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
    }
    for(uint64_t vertex_id = 20; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_edge(10, vertex_id) == false );
        REQUIRE( tx.has_edge(vertex_id, 10) == false );
    }
}

/**
 * Validate roll back over multiple leaves, vertices and edges
 */
TEST_CASE("sf_rollback4", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();
    constexpr uint64_t MAX_VERTEX_ID = 200;

    Transaction tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );
    }
    for(uint64_t vertex_id = 20; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE_NOTHROW( tx.insert_edge(10, vertex_id, (double) 1000 + vertex_id) );
    }

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        context.m_leaf = leaf;
        Segment* segment = leaf->get_segment(0);
        context.m_segment = segment;
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

    REQUIRE( tx.num_vertices() == 20 );
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == true );
    }
    for(uint64_t vertex_id = 20; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_edge(10, vertex_id) == true );
        REQUIRE( tx.has_edge(vertex_id, 10) == true );
    }

    tx.rollback();

    //memstore->dump();

    tx = teseo.start_transaction();
    REQUIRE( tx.num_vertices() == 0 );
    REQUIRE( tx.num_edges() == 0 );
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
    }
    for(uint64_t vertex_id = 20; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE( tx.has_edge(10, vertex_id) == false );
        REQUIRE( tx.has_edge(vertex_id, 10) == false );
    }
}

/**
 * Mix and match transactions, with multiple writers, inserting new vertices
 */
TEST_CASE("sf_transactions", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    // tx1: insert vertex 10
    Transaction tx1 = teseo.start_transaction();
    REQUIRE_NOTHROW( tx1.insert_vertex(10) );

    // tx2: insert vertex 20
    Transaction tx2 = teseo.start_transaction();
    REQUIRE(tx2.num_vertices() == 0);
    REQUIRE(tx2.has_vertex(10) == false);
    tx2.insert_vertex(20);
    //global_context()->storage()->dump();
    REQUIRE_THROWS_AS(tx2.insert_vertex(10), TransactionConflict);

    // tx3: try to insert vertices 10 and 20, fire a TransactionConflict
    Transaction tx3 = teseo.start_transaction();
    REQUIRE(tx3.num_vertices() == 0);
    REQUIRE_THROWS_AS(tx3.insert_vertex(10), TransactionConflict);
    REQUIRE_THROWS_AS(tx3.insert_vertex(20), TransactionConflict);

    // tx1: commit, tx2: rollback, tx3: commit
    tx2.rollback();
    REQUIRE( tx3.num_vertices() == 0);
    REQUIRE_NOTHROW( tx3.insert_vertex(20) );
    REQUIRE( tx3.num_vertices() == 1);
    REQUIRE_THROWS_AS(tx3.insert_vertex(10), TransactionConflict);
    REQUIRE_NOTHROW( tx1.commit() );
    REQUIRE_THROWS_AS( tx3.insert_vertex(10), TransactionConflict ); // well, actually it's actually being modified
    REQUIRE( tx3.num_vertices() == 1 );
    REQUIRE_NOTHROW( tx3.commit() );

    // tx4: validate, tx5: add a new vertex, but it shouldn't be visible to tx4
    Transaction tx4 = teseo.start_transaction();
    Transaction tx5 = teseo.start_transaction();
    tx5.insert_vertex(30);
    REQUIRE(tx4.num_vertices() == 2);
    REQUIRE(tx4.num_edges() == 0);
    REQUIRE(tx4.has_vertex(10) == true);
    REQUIRE(tx4.has_vertex(20) == true);
    REQUIRE(tx4.has_vertex(30) == false);
    tx5.commit();
    REQUIRE(tx4.num_vertices() == 2);
    REQUIRE(tx4.num_edges() == 0);
    REQUIRE(tx4.has_vertex(10) == true);
    REQUIRE(tx4.has_vertex(20) == true);
    REQUIRE(tx4.has_vertex(30) == false);
    tx4.commit();
}

/**
 * Check that the sparse file can insert immediately the first edge because the vertex exists, but not the second one
 */
TEST_CASE("sf_is_source_visible1", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();
    // insert the first vertex with the interface
    auto tx = teseo.start_transaction();
    tx.insert_vertex(9); /* +1, the interface automatically increment the vertex id by 1 to skip the vertex ID 0 */

    // create a context
    Context context { memstore };
    ScopedEpoch epoch; // necessary for index()->find();
    context.m_leaf = memstore->index()->find(0).leaf();
    context.m_segment = context.m_leaf->get_segment(0);
    auto tximpl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    context.m_transaction = tximpl;

    // insert the edge manually, 10 -> 20 should succeed because the vertex 10 exists
    context.writer_enter(Key{0});
    // first, insert the update in the undo, flagged as deletion
    Update update { /* vertex ? */ false, /* insert ? */ false, Key(10, 20), 1020 };
    tximpl->add_undo(memstore, update);
    update.flip(); // insert -> remove, remove -> insert
    REQUIRE_NOTHROW( Segment::update(context, update, false) );

    // insert the second edge, 20 -> 10 should fail because the vertex 20 does not exist
    update.swap(); // 20 -> 10
    REQUIRE(update.source() == 20);
    REQUIRE(update.destination() == 10);
    update.flip(); // insert -> remove
    REQUIRE(update.is_remove());
    tximpl->add_undo(memstore, update);
    update.flip();
    REQUIRE(update.is_insert());
    REQUIRE_THROWS_AS( Segment::update(context, update, /* source vertex exists ? */ false), memstore::Error );

    context.writer_exit(); // clean up
}

/**
 * Same check, but this time the vertex belongs to a different segment
 */
TEST_CASE("sf_is_source_visible2", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    // insert the first vertex with the interface
    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= 60; vertex_id += 10){
        tx.insert_vertex(vertex_id -1); /* +1, the interface automatically increment the vertex id by 1 to skip the vertex ID 0 */
    }
    tx.insert_edge(9, 19, 1020);
    tx.insert_edge(9, 29, 1030);
    tx.insert_edge(9, 39, 1050);
    tx.insert_edge(9, 49, 1050);
    tx.insert_edge(39, 49, 1050);

    // create a context
    Context context { memstore };
    ScopedEpoch epoch; // necessary for index()->find();
    context.m_leaf = memstore->index()->find(0).leaf();
    Segment* segment0 = context.m_segment = context.m_leaf->get_segment(0);
    auto tximpl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    context.m_transaction = tximpl;

    { // spread the vertices over two (or more) segments
        segment0->set_state( Segment::State::WRITE );
        segment0->incr_num_active_threads();
#if !defined(NDEBUG)
        segment0->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    {
        // insert the edge manually, 10 -> 45 should succeed because the edge 10 -> 40 is in the same segment
        context.writer_enter(Key{0});
        // first, insert the update in the undo, flagged as deletion
        Update update { /* vertex ? */ false, /* insert ? */ false, Key(10, 45), 1045 };
        tximpl->add_undo(memstore, update);
        update.flip(); // insert -> remove, remove -> insert
        REQUIRE_NOTHROW( Segment::update(context, update, false) );
        context.writer_exit(); // clean up
    }

    {
        // insert the edge manually, 10 -> 55 should succeed because the edge 10 -> 50 is in the same segment
        context.writer_enter(Key{10, 55});
        // first, insert the update in the undo, flagged as deletion
        Update update { /* vertex ? */ false, /* insert ? */ false, Key(10, 55), 1055 };
        tximpl->add_undo(memstore, update);
        update.flip(); // insert -> remove, remove -> insert
        REQUIRE_NOTHROW( Segment::update(context, update, false) );
        context.writer_exit(); // clean up
    }

   // remove the edge manually inserted
   tximpl->do_rollback(2); // these are the two edges manually inserted: 10 -> 55 and 10 -> 45
   REQUIRE( tx.has_edge(9, 44) == false );
   REQUIRE( tx.has_edge(9, 54) == false );
   // remove the edges with dummy vertices
   tx.remove_edge(9, 49);
   tx.remove_edge(9, 39);

    { // [lhs] now attempting to insert directly 10 -> 55 should fire an exception
        // insert the edge manually, 10 -> 55 should succeed because the edge 10 -> 50 is in the same segment
        context.writer_enter(Key{10, 55});
        // first, insert the update in the undo, flagged as deletion
        Update update { /* vertex ? */ false, /* insert ? */ false, Key(10, 55), 1055 };
        tximpl->add_undo(memstore, update);
        update.flip(); // insert -> remove, remove -> insert
        REQUIRE_THROWS_AS( Segment::update(context, update, false), NotSureIfItHasSourceVertex );
        context.writer_exit(); // clean up
    }


    { // [rhs], same for 10 -> 45, this time it's the rhs
        context.writer_enter(Key{0});
        // first, insert the update in the undo, flagged as deletion
        Update update { /* vertex ? */ false, /* insert ? */ false, Key(10, 45), 1045 };
        tximpl->add_undo(memstore, update);
        update.flip(); // insert -> remove, remove -> insert
        REQUIRE_THROWS_AS( Segment::update(context, update, false), NotSureIfItHasSourceVertex );
        context.writer_exit(); // clean up
    }
}


/**
 * Remove non accessible records from the sparse file
 */
TEST_CASE("sf_prune1", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    // insert the first vertex with the interface
    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= 60; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);
    tx.insert_edge(40, 60, 4060);

    { // spread the vertices over two (or more) segments
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.remove_edge(10, 20); // Remove one edge of the LHS of segment 0
    tx.remove_edge(10, 40); // Remove one edge of the RHS of segment 0
    tx.remove_edge(50, 10); // Remove the first edge in the LHS of segment 3
    tx.remove_edge(60, 40); // Remove the first edge in the RHS of segment 3

    tx.commit();

    // refresh the active list of transactions
    this_thread::sleep_for(2* context::StaticConfiguration::tctimer_txnlist_lifetime);

    { // prune segment 0
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        uint64_t used_space_before = segment->used_space();
        Segment::prune(context);
        uint64_t used_space_after = segment->used_space();
        REQUIRE(( used_space_before - used_space_after) >= (3 * OFFSET_ELEMENT)); // 1 dummy vertex & two edges, plus the versions
    }

    { // prune segment 3
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(3);
        uint64_t used_space_before = segment->used_space();
        Segment::prune(context);
        uint64_t used_space_after = segment->used_space();
        REQUIRE(( used_space_before - used_space_after) >= (2 * OFFSET_ELEMENT)); // at least two edges
    }

    memstore->dump();

    tx = teseo.start_transaction();
    REQUIRE(tx.has_vertex(50));
    REQUIRE(tx.has_vertex(60));
}

/**
 * Remove non accessible records from the sparse file
 */
TEST_CASE("sf_prune2", "[sf] [memstore]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    // insert the first vertex with the interface
    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= 60; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 60, 1060);
    tx.commit();


    { // spread the vertices over two (or more) segments
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    // insert in the RHS of segment 0
    tx = teseo.start_transaction();
    tx.insert_edge(20, 30, 2030);
    tx.insert_edge(20, 40, 2030);
    tx.commit();

    // remove an intermediate edge of both the LHS and the RHS of segment 0
    tx = teseo.start_transaction();
    tx.remove_edge(10, 40);
    tx.remove_edge(20, 30);
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_edge(10, 20); // keep it uncommitted
    tx.remove_edge(10, 60); // keep it uncommitted
    tx.remove_edge(20, 40); // keep it uncommitted

    // refresh the active list of transactions
    this_thread::sleep_for(2* context::StaticConfiguration::tctimer_txnlist_lifetime);

    { // prune segment 0
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        uint64_t used_space_before = segment->used_space();
        Segment::prune(context);
        uint64_t used_space_after = segment->used_space();
        REQUIRE(( used_space_before - used_space_after) >= (2 * OFFSET_ELEMENT)); // at least the edge 10 -> 40 and 20 -> 30
    }

    tx.rollback();

    tx = teseo.start_transaction();
    REQUIRE(tx.has_edge(10, 20));
    REQUIRE(tx.has_edge(10, 60));
    REQUIRE(tx.has_edge(20, 40));
}


/**
 * Attempt to remove a non existing vertex from an empty file
 */
TEST_CASE("sf_remove_vertex_1", "[sf] [memstore] [remove_vertex]" ){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    REQUIRE_THROWS_AS(tx.remove_vertex(20), LogicalError); // Vertex 20 does not exist
    REQUIRE(tx.num_vertices() == 0);
    tx.insert_vertex(10);
    REQUIRE(tx.num_vertices() == 1);
    REQUIRE_THROWS_AS(tx.remove_vertex(20), LogicalError); // Vertex 20 does not exist
    REQUIRE(tx.num_vertices() == 1);
}

/**
 * Attempt to remove the only vertex in the file
 */
TEST_CASE("sf_remove_vertex_2", "[sf] [memstore] [remove_vertex]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    SECTION("same_transaction"){
        auto tx = teseo.start_transaction();
        tx.insert_vertex(10);
        REQUIRE(tx.has_vertex(10) == true);
        REQUIRE(tx.num_vertices() == 1);
        tx.remove_vertex(10);
        REQUIRE(tx.has_vertex(10) == false);
        REQUIRE(tx.num_vertices() == 0);
    }

    SECTION("different_transactions"){
        auto tx1 = teseo.start_transaction();
        tx1.insert_vertex(10);
        tx1.commit();

        auto tx2 = teseo.start_transaction();
        REQUIRE(tx2.has_vertex(10) == true);
        REQUIRE(tx2.num_vertices() == 1);
        tx2.remove_vertex(10);
        REQUIRE(tx2.has_vertex(10) == false);
        REQUIRE(tx2.num_vertices() == 0);
        tx2.rollback();

        auto tx3 = teseo.start_transaction();
        REQUIRE(tx3.has_vertex(10) == true);
        REQUIRE(tx3.num_vertices() == 1);
        tx3.remove_vertex(10);
        REQUIRE(tx3.has_vertex(10) == false);
        REQUIRE(tx3.num_vertices() == 0);
        tx3.commit();

        auto tx4 = teseo.start_transaction();
        REQUIRE(tx4.has_vertex(10) == false);
        REQUIRE(tx4.num_vertices() == 0);
    }
}

/**
 * Remove 10 vertices, with no edges attached from left to right.
 * Use only the LHS of the first segment.
 */
TEST_CASE( "sf_remove_vertex_3", "[sf] [memstore] [remove_vertex]" ){
    const uint64_t max_vertex_id = 100;
    uint64_t num_vertices = 0;
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    { // first create the vertices
        auto tx = teseo.start_transaction();
        for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
            tx.insert_vertex(vertex_id);
            num_vertices++;
        }
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
            REQUIRE(tx.has_vertex(vertex_id) == true);
        }

        tx.commit();
    }

    // remove the vertices one by one
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v >= vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }

        tx.remove_vertex(vertex_id);
        num_vertices--;

        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v > vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }
        REQUIRE(tx.num_vertices() == num_vertices);

        tx.commit();
    }
}

/**
 * Remove 10 vertices, with no edges attached from left to right.
 * Use multiple segments to validate both the LHS and RHS
 */
TEST_CASE( "sf_remove_vertex_4", "[sf] [memstore] [remove_vertex]" ){
    const uint64_t max_vertex_id = 100;
    uint64_t num_vertices = 0;
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    { // first create the vertices
        auto tx = teseo.start_transaction();
        for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
            tx.insert_vertex(vertex_id);
            num_vertices++;
        }
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
            REQUIRE(tx.has_vertex(vertex_id) == true);
        }

        tx.commit();
    }

    // rebalance
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        context.m_leaf = leaf;
        Segment* segment = leaf->get_segment(0);
        context.m_segment = segment;
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    // remove the vertices one by one
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v >= vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }

        tx.remove_vertex(vertex_id);
        num_vertices--;

        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v > vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }
        REQUIRE(tx.num_vertices() == num_vertices);

        tx.commit();
    }

    //memstore->dump();
}

/**
 * Follow up of sf_remove_vertex_4, check if after the deletions, it can clean up the segments when invoking prune.
 */
TEST_CASE( "sf_prune3", "[sf] [memstore] [prune] [remove_vertex]" ){
    const uint64_t max_vertex_id = 100;
    uint64_t num_vertices = 0;
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    { // first create the vertices
        auto tx = teseo.start_transaction();
        for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
            tx.insert_vertex(vertex_id);
            num_vertices++;
        }
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
            REQUIRE(tx.has_vertex(vertex_id) == true);
        }

        tx.commit();
    }

    // rebalance
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        context.m_leaf = leaf;
        Segment* segment = leaf->get_segment(0);
        context.m_segment = segment;
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    // remove the vertices one by one
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v >= vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }

        tx.remove_vertex(vertex_id);
        num_vertices--;

        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v > vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }
        REQUIRE(tx.num_vertices() == num_vertices);

        tx.commit();
    }

    // refresh the active list of transactions
    this_thread::sleep_for(2* context::StaticConfiguration::tctimer_txnlist_lifetime);

    // prune segment 0
    ScopedEpoch epoch;
    Context context { memstore };
    Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
    Segment* segment = context.m_segment = leaf->get_segment(0);
    Segment::prune(context);
    REQUIRE( segment->used_space() == 0 );

    // prune segment 1
    segment = context.m_segment = leaf->get_segment(1);
    Segment::prune(context);
    REQUIRE( segment->used_space() == 0 );
}


/**
 * Remove a vertex and its edges from the same segment, only the LHS
 */
TEST_CASE( "sf_remove_vertex_5", "[sf] [memstore] [remove_vertex]" ){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(20, 30, 2030);
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(10);
    tx.commit();

    tx = teseo.start_transaction();
    REQUIRE(tx.num_vertices() == 2);
    REQUIRE(tx.has_vertex(10) == false);
    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_vertex(30) == true);
    REQUIRE(tx.num_edges() == 1);
    REQUIRE(tx.has_edge(10, 20) == false);
    REQUIRE(tx.has_edge(10, 30) == false);
    REQUIRE(tx.has_edge(20, 10) == false);
    REQUIRE(tx.has_edge(30, 10) == false);
    REQUIRE(tx.has_edge(20, 30) == true);
    REQUIRE(tx.has_edge(30, 20) == true);


    // check that a rebalance ignores the removed entries
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    REQUIRE(tx.has_vertex(10) == false);
    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_vertex(30) == true);
    REQUIRE(tx.has_edge(10, 20) == false);
    REQUIRE(tx.has_edge(10, 30) == false);
    REQUIRE(tx.has_edge(20, 10) == false);
    REQUIRE(tx.has_edge(30, 10) == false);
    REQUIRE(tx.has_edge(20, 30) == true);
    REQUIRE(tx.has_edge(30, 20) == true);
}

/**
 * Remove a vertex and its edges from the same segment, only the RHS
 */
TEST_CASE( "sf_remove_vertex_6", "[sf] [memstore] [remove_vertex]" ){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_vertex(50);
    tx.insert_vertex(60);
    tx.insert_vertex(70);
    tx.commit();

    { // rebalance
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx = teseo.start_transaction();
    tx.insert_edge(30, 50, 3050); // 30 is in the first segment RHS, 50 is in the second segment LHS
    tx.insert_edge(30, 60, 3050); // 30 is in the first segment RHS, 60 is in the second segment LHS
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(30);
    tx.commit();

    tx = teseo.start_transaction();
    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_vertex(30) == false);
    REQUIRE(tx.has_vertex(40) == true);
    REQUIRE(tx.has_vertex(50) == true);
    REQUIRE(tx.has_vertex(60) == true);
    REQUIRE(tx.has_edge(30, 50) == false);
    REQUIRE(tx.has_edge(50, 30) == false);
    REQUIRE(tx.has_edge(30, 60) == false);
    REQUIRE(tx.has_edge(60, 30) == false);

    // refresh the active list of transactions
    this_thread::sleep_for(2* context::StaticConfiguration::tctimer_txnlist_lifetime);

    { // rebalance
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_vertex(30) == false);
    REQUIRE(tx.has_vertex(40) == true);
    REQUIRE(tx.has_vertex(50) == true);
    REQUIRE(tx.has_vertex(60) == true);
    REQUIRE(tx.has_edge(30, 50) == false);
    REQUIRE(tx.has_edge(50, 30) == false);
    REQUIRE(tx.has_edge(30, 60) == false);
    REQUIRE(tx.has_edge(60, 30) == false);
}

/**
 * Remove a vertex whose edges span both the LHS and RHS of the same segment
 */
TEST_CASE( "sf_remove_vertex_7", "[sf] [memstore] [remove_vertex]" ){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_vertex(50);
//    tx.insert_vertex(60);
//    tx.insert_vertex(70);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);

    { // rebalance
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(10);
    tx.commit();

    tx = teseo.start_transaction();
    REQUIRE(tx.has_vertex(10) == false);
    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_edge(10, 20) == false);
    REQUIRE(tx.has_edge(10, 30) == false);
    REQUIRE(tx.has_edge(10, 40) == false);
    REQUIRE(tx.has_edge(10, 50) == false);
    REQUIRE(tx.has_edge(20, 10) == false);
    REQUIRE(tx.has_edge(30, 10) == false);
    REQUIRE(tx.has_edge(40, 10) == false);
    REQUIRE(tx.has_edge(50, 10) == false);

    { // rebalance => nop, because it keeps the old transaction list
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    REQUIRE(tx.has_vertex(10) == false);
    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_edge(10, 20) == false);
    REQUIRE(tx.has_edge(10, 30) == false);
    REQUIRE(tx.has_edge(10, 40) == false);
    REQUIRE(tx.has_edge(10, 50) == false);
    REQUIRE(tx.has_edge(20, 10) == false);
    REQUIRE(tx.has_edge(30, 10) == false);
    REQUIRE(tx.has_edge(40, 10) == false);
    REQUIRE(tx.has_edge(50, 10) == false);

    // refresh the active list of transactions
    this_thread::sleep_for(2* context::StaticConfiguration::tctimer_txnlist_lifetime);

    { // rebalance, prune the old records
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    REQUIRE(tx.has_vertex(10) == false);
    REQUIRE(tx.has_vertex(20) == true);
    REQUIRE(tx.has_edge(10, 20) == false);
    REQUIRE(tx.has_edge(10, 30) == false);
    REQUIRE(tx.has_edge(10, 40) == false);
    REQUIRE(tx.has_edge(10, 50) == false);
    REQUIRE(tx.has_edge(20, 10) == false);
    REQUIRE(tx.has_edge(30, 10) == false);
    REQUIRE(tx.has_edge(40, 10) == false);
    REQUIRE(tx.has_edge(50, 10) == false);
}

/**
 * Remove a vertex whose edges span over two segments
 */
TEST_CASE( "sf_remove_vertex_8", "[sf] [memstore] [remove_vertex]" ){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_vertex(50);
    tx.insert_vertex(60);
    tx.insert_vertex(70);
    tx.insert_vertex(80);
    tx.insert_vertex(90);
    tx.insert_vertex(100);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);
    tx.insert_edge(10, 60, 1060);
    tx.insert_edge(10, 70, 1070);
    tx.insert_edge(10, 80, 1080);

    { // rebalance
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }
    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(10);
    REQUIRE(tx.has_vertex(10) == false);
    for(uint64_t vertex_id = 20; vertex_id <= 80; vertex_id += 10){
        REQUIRE(tx.has_edge(10, vertex_id) == false);
        REQUIRE(tx.has_edge(vertex_id, 10) == false);
    }
    REQUIRE(tx.has_vertex(20) == true);

    tx.commit();

    { // rebalance => nop, because it relies on the old transaction list
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx = teseo.start_transaction();
    REQUIRE(tx.has_vertex(10) == false);
    for(uint64_t vertex_id = 20; vertex_id <= 80; vertex_id += 10){
        REQUIRE(tx.has_edge(10, vertex_id) == false);
        REQUIRE(tx.has_edge(vertex_id, 10) == false);
    }
    REQUIRE(tx.has_vertex(20) == true);

    // refresh the active list of transactions
    this_thread::sleep_for(2* context::StaticConfiguration::tctimer_txnlist_lifetime);

    { // rebalance, prune the old records
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    REQUIRE(tx.has_vertex(10) == false);
    for(uint64_t vertex_id = 20; vertex_id <= 80; vertex_id += 10){
        REQUIRE(tx.has_edge(10, vertex_id) == false);
        REQUIRE(tx.has_edge(vertex_id, 10) == false);
    }
    REQUIRE(tx.has_vertex(20) == true);
}

/**
 * Remove a vertex whose edges span over two leaves
 */
TEST_CASE( "sf_remove_vertex_9", "[sf] [memstore] [remove_vertex]" ){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    const uint64_t max_vertex_id = 140;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    for(uint64_t vertex_id = 10; vertex_id < max_vertex_id; vertex_id += 10){
        tx.insert_edge(max_vertex_id, vertex_id, 1000 + vertex_id);
    }

    { // rebalance
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = leaf->get_segment(0);
        segment->set_state( Segment::State::WRITE );
        segment->incr_num_active_threads();
#if !defined(NDEBUG)
        segment->m_writer_id = util::Thread::get_thread_id();
#endif
        Crawler crawler { context };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad;
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx.commit();

    tx = teseo.start_transaction();
    tx.remove_vertex(max_vertex_id);
    tx.commit();

    tx = teseo.start_transaction();
    REQUIRE(tx.has_vertex(max_vertex_id) == false);
    for(uint64_t vertex_id = 10; vertex_id < max_vertex_id; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == true );
    }
    for(uint64_t vertex_id = 10; vertex_id < max_vertex_id; vertex_id += 10){
        REQUIRE( tx.has_edge(max_vertex_id, vertex_id) == false);
        REQUIRE( tx.has_edge(vertex_id, max_vertex_id) == false);
    }

    // let's merge the two leaves
    memstore->merger()->execute_now();

    { // check there is only leaf around
        ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        REQUIRE( leaf->get_hfkey() == KEY_MAX );
    }

    REQUIRE(tx.has_vertex(max_vertex_id) == false);
    for(uint64_t vertex_id = 10; vertex_id < max_vertex_id; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == true );
    }
    for(uint64_t vertex_id = 10; vertex_id < max_vertex_id; vertex_id += 10){
        REQUIRE( tx.has_edge(max_vertex_id, vertex_id) == false);
        REQUIRE( tx.has_edge(vertex_id, max_vertex_id) == false);
    }
}

