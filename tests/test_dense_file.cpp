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
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/rebalance/async_service.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;

/**
 * Insert some vertices in the sparse array, but don't trigger a rebalance
 */
TEST_CASE("df_vertex_insert", "[df] [memstore]"){
    Teseo teseo;
    global_context()->async()->stop(); // we'll do the rebalances manually
    uint64_t MAX_VERTEX_ID = 200;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id < MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(!tx.has_vertex(vertex_id));
        tx.insert_vertex(vertex_id);
        REQUIRE(tx.has_vertex(vertex_id));
    }
    for(uint64_t vertex_id = 5; vertex_id < MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(!tx.has_vertex(vertex_id));
        tx.insert_vertex(vertex_id);
        REQUIRE(tx.has_vertex(vertex_id));
    }
}

/**
 * Check that the dense file can insert immediately the first edge because the vertex exists, but not the second
 */
TEST_CASE("df_is_source_visible", "[df] [memstore]"){
    Teseo teseo;
    global_context()->async()->stop(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    // transform the first segment into a dense file
    ScopedEpoch epoch;
    Context context { memstore };
    context.m_leaf = memstore->index()->find(0).leaf();
    context.m_segment = context.m_leaf->get_segment(0);
    Segment::to_dense_file(context);

    // insert the first vertex with the interface
    auto tx = teseo.start_transaction();
    tx.insert_vertex(9); /* +1, the interface automatically increment the vertex id by 1 to skip the vertex ID 0 */

    // insert the edge manually, 10 -> 20 should succeed because the vertex 10 exists
    context.writer_enter(Key{0});
    auto tximpl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    context.m_transaction = tximpl;
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
    REQUIRE_THROWS_AS( Segment::update(context, update, /* source vertex exists ? */ false), NotSureIfItHasSourceVertex );

    context.writer_exit(); // clean up
}

/**
 * Validate rollback on a dense file. Invoke rollback selectively only on a few vertices
 */
TEST_CASE("df_rollback1", "[df] [memstore]"){
    Teseo teseo;
    global_context()->async()->stop(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    { // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx1 = teseo.start_transaction();
    tx1.insert_vertex(10);
    auto tx2 = teseo.start_transaction();
    tx2.insert_vertex(20);

    auto tx3 = teseo.start_transaction(); // TX 3
    tx3.insert_vertex(30);
    tx3.commit();
    auto tx4 = teseo.start_transaction(); // TX 4
    tx4.insert_vertex(40);
    tx4.commit();

    auto tx5 = teseo.start_transaction();
    REQUIRE( tx5.num_vertices() == 2 ); // TX 3 and TX4;
    REQUIRE_THROWS_AS( tx5.insert_vertex(10), TransactionConflict ); // tx 1
    REQUIRE_THROWS_AS( tx5.insert_vertex(20), TransactionConflict ); // tx 2
    REQUIRE_THROWS_AS( tx5.insert_vertex(30), LogicalError ); // already exists
    REQUIRE_THROWS_AS( tx5.insert_vertex(40), LogicalError ); // already exists
    tx5.insert_vertex(50); // TX 5
    REQUIRE ( tx5.num_vertices() == 3 );
    REQUIRE ( tx5.has_vertex(10) == false );
    REQUIRE ( tx5.has_vertex(20) == false );
    REQUIRE ( tx5.has_vertex(30) == true );
    REQUIRE ( tx5.has_vertex(40) == true );
    REQUIRE ( tx5.has_vertex(50) == true );
    tx5.rollback();

    auto tx6 = teseo.start_transaction();
    REQUIRE( tx6.num_vertices() == 2 ); // TX 3 and TX 4
    REQUIRE ( tx6.has_vertex(10) == false );
    REQUIRE ( tx6.has_vertex(20) == false );
    REQUIRE ( tx6.has_vertex(30) == true );
    REQUIRE ( tx6.has_vertex(40) == true );
    REQUIRE ( tx6.has_vertex(50) == false );

    REQUIRE_THROWS_AS( tx6.insert_vertex(20), TransactionConflict ); // tx 2
    tx2.rollback();

    //memstore->dump();

    REQUIRE( tx6.num_vertices() == 2 ); // TX 3 and TX 4
    REQUIRE ( tx6.has_vertex(10) == false );
    REQUIRE ( tx6.has_vertex(20) == false );
    REQUIRE ( tx6.has_vertex(30) == true );
    REQUIRE ( tx6.has_vertex(40) == true );
    REQUIRE ( tx6.has_vertex(50) == false );
    REQUIRE_NOTHROW( tx6.insert_vertex(20) );
    REQUIRE( tx6.num_vertices() == 3 );
    REQUIRE ( tx6.has_vertex(20) == true );


    REQUIRE_THROWS_AS( tx6.insert_vertex(10), TransactionConflict ); // tx 1
    tx1.rollback();
    REQUIRE_NOTHROW( tx6.insert_vertex(10) );
    REQUIRE( tx6.num_vertices() == 4 );
    REQUIRE ( tx6.has_vertex(10) == true );
    tx6.rollback();

    auto tx7 = teseo.start_transaction();
    REQUIRE( tx7.num_vertices() == 2 ); // TX 3 and TX 4
    REQUIRE ( tx7.has_vertex(10) == false );
    REQUIRE ( tx7.has_vertex(20) == false );
    REQUIRE ( tx7.has_vertex(30) == true );
    REQUIRE ( tx7.has_vertex(40) == true );
    REQUIRE ( tx7.has_vertex(50) == false );

    { // check the cardinality & used space are properly set
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = context.m_leaf->get_segment(0);
        REQUIRE(segment->used_space() == (OFFSET_ELEMENT + OFFSET_VERSION) * 2); // 2 elts
        REQUIRE(Segment::cardinality(context) == 2); // 2 elts
    }

    /**
     * Rebalance, check load properly skips the empty data items
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
        ScratchPad scratchpad { };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    auto tx8 = teseo.start_transaction();
    REQUIRE( tx8.num_vertices() == 2 ); // TX 3 and TX 4
    REQUIRE ( tx8.has_vertex(10) == false );
    REQUIRE ( tx8.has_vertex(20) == false );
    REQUIRE ( tx8.has_vertex(30) == true );
    REQUIRE ( tx8.has_vertex(40) == true );
    REQUIRE ( tx8.has_vertex(50) == false );
}

/**
 * Validate rollback on a dense file. Only vertices.
 */
TEST_CASE("df_rollback2", "[df] [memstore]"){
    Teseo teseo;
    global_context()->async()->stop(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();
    constexpr uint64_t MAX_VERTEX_ID = 1000;

    { // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.rollback();

    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id) == false);
    }
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.rollback();

    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id) == false);
    }

    { // check the cardinality & used space are properly set in the segment
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = context.m_leaf->get_segment(0);
        REQUIRE(segment->used_space() == 0);
        REQUIRE(Segment::cardinality(context) == 0);
    }

}

/**
 * Validate rollback on a dense file. Both vertices & edges
 */
TEST_CASE("df_rollback3", "[df] [memstore]"){
    Teseo teseo;
    global_context()->async()->stop(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();
    constexpr uint64_t MAX_VERTEX_ID = 100;

    { // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        if(vertex_id != 10){ tx.insert_edge(10, vertex_id, 1000 + vertex_id); }
    }
    tx.rollback();

    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id) == false);
        if(vertex_id != 10) { REQUIRE(tx.has_edge(10, vertex_id) == false); }
    }
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE_THROWS( tx.insert_edge(10, vertex_id, 1000 + vertex_id) );
        tx.insert_vertex(vertex_id);
        if(vertex_id != 10){ tx.insert_edge(10, vertex_id, 1000 + vertex_id); }
    }
    tx.rollback();

    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id) == false);
        if(vertex_id != 10) { REQUIRE(tx.has_edge(10, vertex_id) == false); }
    }

    { // check the cardinality & used space are properly set in the segment
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment = context.m_segment = context.m_leaf->get_segment(0);
        REQUIRE(segment->used_space() == 0);
        REQUIRE(Segment::cardinality(context) == 0);
    }

}

/**
 * Mix and match transactions, with multiple writers, inserting new vertices
 */
TEST_CASE("df_transactions", "[df] [memstore]"){
    Teseo teseo;
    global_context()->async()->stop(); // we'll do the rebalances manually
    Memstore* memstore = global_context()->memstore();

    { // transform the first segment into a dense file
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
    }


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


