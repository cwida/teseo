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



#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "../src/context.hpp"
#include "../src/memstore/sparse_array.hpp"
#include "teseo.hpp"
#include <iostream>

using namespace teseo;
using namespace teseo::internal::context;
using namespace teseo::internal::memstore;
using namespace std;

/**
 * Create & destroy a sparse array. GlobalContext already initialises an internal sparse array.
 */
TEST_CASE("init") {
    g_debugging_test = true;

    GlobalContext instance;
    TransactionImpl* tx_impl = new TransactionImpl(shptr_thread_context(), instance.next_transaction_id());
    tx_impl->incr_user_count();

    tx_impl->decr_user_count();
    tx_impl = nullptr; // do not invoke delete
}

/**
 * Insert some vertices in the sparse array, but don't trigger the rebalancer
 */
TEST_CASE("vertex_insert_raw"){
    g_debugging_test = true;

    GlobalContext instance;
    SparseArray* sa = instance.storage();

    TransactionImpl* tx = new TransactionImpl(shptr_thread_context(), instance.next_transaction_id());
    tx->incr_user_count();
    REQUIRE_NOTHROW( sa->insert_vertex(tx, 20) );
    REQUIRE_NOTHROW( tx->commit() );
    tx->decr_user_count();
    tx = nullptr; // do not invoke delete

    tx = new TransactionImpl(shptr_thread_context(), instance.next_transaction_id());
    tx->incr_user_count();
    REQUIRE_NOTHROW( sa->insert_vertex(tx, 10) );
    REQUIRE_NOTHROW( tx->commit() );
    tx->decr_user_count();
    tx = nullptr; // do not invoke delete


    tx = new TransactionImpl(shptr_thread_context(), instance.next_transaction_id());
    tx->incr_user_count();
    REQUIRE_NOTHROW( sa->insert_vertex(tx, 30) );
    REQUIRE_NOTHROW( tx->commit() );
    tx->decr_user_count();
    tx = nullptr; // do not invoke delete

    // sa->dump();
}


/**
 * Similarly to `vertex_insert', insert some vertices in the sparse array, but don't trigger the rebalancer.
 * Use the Teseo interface this time
 */
TEST_CASE("vertex_insert_tx"){
    g_debugging_test = true;

    Teseo teseo;

    {
        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(20) );
        REQUIRE_NOTHROW( tx.insert_vertex(10) );
        REQUIRE_NOTHROW( tx.insert_vertex(30) );
        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}

/**
 * Try to insert an edge in the sparse array
 */
TEST_CASE("edge_insert_lhs"){
    g_debugging_test = true;

    Teseo teseo;

    {
        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(20) );
        REQUIRE_NOTHROW( tx.insert_vertex(10) );
        REQUIRE_NOTHROW( tx.insert_edge(20, 10, 1020));
        REQUIRE_THROWS_WITH( tx.insert_edge(10, 20, 2010), Catch::Contains("The edge") && Catch::Contains("already exists") ); // already inserted
        REQUIRE_THROWS_WITH( tx.insert_edge(10, 30, 2010), Catch::Contains("vertex") && Catch::Contains("does not exist") ); // the vertex 30 does not exist
        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}

/**
 * Trigger the rebalancer, just a little bit
 */
TEST_CASE("rebalancer_baby"){
    g_debugging_test = true;

    Teseo teseo;

    for(uint64_t vertex_id = 10; vertex_id <= 100; vertex_id += 10){
        //global_context()->storage()->dump();
        //cout << "\n-------------------------------\n";
        //cout <<"> INSERT VERTEX " << vertex_id << "\n";
        //cout << "-------------------------------\n\n" << endl;

        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );

        // Check all previous insertions
        for(uint64_t candidate = 10; candidate <= 100; candidate += 10){
            bool expected_result = candidate <= vertex_id;
            REQUIRE( tx.has_vertex(candidate) == expected_result );
        }

        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}

/**
 * Insertions of edges in the right hand side of a segment. As it requires
 * a small rebalance, perform it only after `rebalancer_baby' passes.
 */
TEST_CASE("edge_insert_rhs"){
    g_debugging_test = true;

    Teseo teseo;
    for(uint64_t vertex_id = 10; vertex_id <= 60; vertex_id += 10){
        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );
        REQUIRE_NOTHROW( tx.commit() );
    }

    for(uint64_t vertex_id = 20; vertex_id <= 60; vertex_id += 10){
        //global_context()->storage()->dump();
        //cout << "\n-------------------------------\n";
        //cout <<"> INSERT EDGE 10 -> " << vertex_id << "\n";
        //cout << "-------------------------------\n\n" << endl;

        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_edge(10, vertex_id, 1000 + vertex_id ) );

        for(uint64_t candidate = 20; candidate <= 100; candidate += 10){
            bool expected_result = candidate <= vertex_id;
            REQUIRE( tx.has_edge(10, candidate) == expected_result );
            REQUIRE( tx.has_edge(candidate, 10) == expected_result ); // because the graph is undirected
        }

        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}


/**
 * Fill a chunk full of vertices. Keep triggering the rebalancer, possibly among multiple
 * gates, but do not cause a leaf (chunk) split.
 */
TEST_CASE("rebalancer_kid"){
    g_debugging_test = true;

    Teseo teseo;
    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 700; // after that, it fires a leaf split

    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        //global_context()->storage()->dump();
        //cout << "\n-------------------------------\n";
        //cout <<"> INSERT VERTEX " << vertex_id << "\n";
        //cout << "-------------------------------\n\n" << endl;

        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );

        // Check all previous insertions
        for(uint64_t candidate = vertex_min; candidate <= vertex_max; candidate += 10){
            bool expected_result = candidate <= vertex_id;
            REQUIRE( tx.has_vertex(candidate) == expected_result );
        }

        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}


/**
 * Keep inserting vertices, causing leaf splits
 */
TEST_CASE("rebalancer_teenager"){
    g_debugging_test = true;

    Teseo teseo;
    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 10000;

    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        //global_context()->storage()->dump();
        //cout << "\n-------------------------------\n";
        //cout <<"> INSERT VERTEX " << vertex_id << "\n";
        //cout << "-------------------------------\n\n" << endl;

        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );

        // Check all previous insertions
        for(uint64_t candidate = vertex_min; candidate <= vertex_max; candidate += 10){
            bool expected_result = candidate <= vertex_id;
            REQUIRE( tx.has_vertex(candidate) == expected_result );
        }

        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}

/**
 * Keep inserting vertices, causing leaf splits, in reverse order
 */
TEST_CASE("rebalancer_teenager_reverse"){
    g_debugging_test = true;

    Teseo teseo;
    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 10000;

    for(uint64_t vertex_id = vertex_max; vertex_id >= vertex_min; vertex_id -= 10){
        //global_context()->storage()->dump();
        //cout << "\n-------------------------------\n";
        //cout <<"> INSERT VERTEX " << vertex_id << "\n";
        //cout << "-------------------------------\n\n" << endl;

        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );

        // Check all previous insertions
        for(uint64_t candidate = vertex_min; candidate <= vertex_max; candidate += 10){
            bool expected_result = candidate >= vertex_id;
            REQUIRE( tx.has_vertex(candidate) == expected_result );
        }

        REQUIRE_NOTHROW( tx.commit() );
    }

    //global_context()->storage()->dump();
}

/**
 * Insert & remove a few edges, just a few
 */
TEST_CASE("edge_remove"){
    g_debugging_test = true;
    Teseo teseo;

    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 1000;

    /**
     * Insert the vertices
     */
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        //cout << "\n-------------------------------\n";
        //cout <<"> INSERT VERTEX " << vertex_id << "\n";
        //cout << "-------------------------------\n\n" << endl;

        Transaction tx = teseo.start_transaction();
        REQUIRE_NOTHROW( tx.insert_vertex(vertex_id) );
        REQUIRE_NOTHROW( tx.commit() );
    }

    /**
     * Check that all vertices are present
     */
    {
        Transaction tx = teseo.start_transaction();
        for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
            REQUIRE( tx.has_vertex(vertex_id) == true );
        }
    }

    /**
     * Insert the edges
     */
    {
        Transaction tx = teseo.start_transaction();
        for(uint64_t src = vertex_min; src < vertex_max; src += 10){
            for(uint64_t dst = src + 10; dst <= vertex_max; dst += 10){
                //cout << "\n-------------------------------\n";
                //cout <<"> INSERT EDGE " << src << " -> " << dst << "\n";
                //cout << "-------------------------------\n\n" << endl;

                REQUIRE( tx.has_edge(src, dst) == false );
                REQUIRE_NOTHROW( tx.insert_edge(src, dst, 10000 + dst) );
                REQUIRE( tx.has_edge(src, dst) == true );

                //global_context()->storage()->dump();

                // validate the current database
                for(uint64_t src1 = vertex_min; src1 <= vertex_max; src1 += 10){
                    for(uint64_t dst1 = vertex_min; dst1 <= vertex_max; dst1 += 10){
                        //cout << "-- check has_edge(" << src1 << ", " << dst1 << ")" << endl;
                        bool expected = (src1 < dst1 && ( (src1 < src) || (src1 == src && dst1 <= dst))) || (dst1 < src1 && (dst1 < src || (dst1 == src && src1 <= dst)));
                        REQUIRE( tx.has_edge(src1, dst1) == expected );
                    }
                }

            }
        }
        tx.commit();
    }

    /**
     * Now remove them
     */
    {
        Transaction tx = teseo.start_transaction();
        for(uint64_t src = vertex_min; src < vertex_max; src += 10){
            for(uint64_t dst = src + 10; dst <= vertex_max; dst += 10){
                //cout << "\n-------------------------------\n";
                //cout <<"> REMOVE EDGE " << src << " -> " << dst << "\n";
                //cout << "-------------------------------\n\n" << endl;

                REQUIRE( tx.has_edge(src, dst) == true );
                REQUIRE_NOTHROW( tx.remove_edge(src, dst) );
                REQUIRE( tx.has_edge(src, dst) == false );

                //global_context()->storage()->dump();

                // validate the current database
                for(uint64_t src1 = vertex_min; src1 <= vertex_max; src1 += 10){
                    for(uint64_t dst1 = vertex_min; dst1 <= vertex_max; dst1 += 10){
                        //cout << "-- check has_edge(" << src1 << ", " << dst1 << ")" << endl;
                        bool expected = (src1 < dst1 && !( (src1 < src) || (src1 == src && dst1 <= dst))) || (dst1 < src1 && !(dst1 < src || (dst1 == src && src1 <= dst)));
                        REQUIRE( tx.has_edge(src1, dst1) == expected );
                    }
                }

            }
        }
        tx.commit();
    }
}

/**
 * Check the counters for the total number of vertices and edges in the graph are properly
 * maintained
 */
TEST_CASE("global_properties_1"){
    g_debugging_test = true;
    Teseo teseo;

    { // insert a few vertices
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 0 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(10) );
        REQUIRE( tx.num_vertices() == 1 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_vertex(20) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 0 );
        tx.commit();
    }

    { // insert a few edges
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_edge(20, 10, 1020) );
        REQUIRE( tx.num_vertices() == 2 );
        REQUIRE( tx.num_edges() == 1 );
        REQUIRE_NOTHROW( tx.insert_vertex(30) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 1 );
        REQUIRE_NOTHROW( tx.remove_edge(10, 20) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 0 );
        REQUIRE_NOTHROW( tx.insert_edge(10, 20, 1020) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 1 );
        REQUIRE_NOTHROW ( tx.insert_edge(10, 30, 1030) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 2 );
        tx.commit();
    }

    { // remove one edge
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 2 );
        REQUIRE_NOTHROW( tx.remove_edge(30, 10) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 1 );
        tx.commit();
    }

    { // rollback the transaction
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 1 );
        REQUIRE_NOTHROW( tx.remove_edge(10, 20) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 0 );
        tx.rollback();
    }

    { // remove the other edge
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 1 );
        REQUIRE_NOTHROW( tx.remove_edge(20, 10) );
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 0 );
        tx.commit();
    }

    { // final check
        auto tx = teseo.start_transaction();
        REQUIRE( tx.num_vertices() == 3 );
        REQUIRE( tx.num_edges() == 0 );
    }
}

/**
 * Validate roll back for a small chain
 */
TEST_CASE("rollback_basic"){
    g_debugging_test = true;
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
 * Validate a very long roll back
 */
TEST_CASE("rollback_long"){
    g_debugging_test = true;
    Teseo teseo;

    uint64_t vertex_min = 10;
    uint64_t vertex_max = 100;

    Transaction tx = teseo.start_transaction();
    // insert some vertices
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
        tx.insert_vertex(vertex_id);
        REQUIRE( tx.has_vertex(vertex_id) == true );
    }

    // insert some edges
    uint64_t weight = 1;
    int64_t num_edges = 0;
    for(uint64_t src = vertex_min; src <= vertex_max; src += 10){
        for(uint64_t dst = vertex_min; dst <= vertex_max; dst += 10){
            if(src == dst) continue;
            if(tx.has_edge(src, dst)){
                tx.remove_edge(src, dst);
                REQUIRE(tx.has_edge(src, dst) == false);

                num_edges--;
                REQUIRE(num_edges >= 0);
            } else {
                tx.insert_edge(src, dst, weight++);

                REQUIRE(tx.has_edge(src, dst) == true);
                num_edges++;
            }

            REQUIRE(tx.num_edges() == num_edges);
        }
    }

    tx.rollback();
    //global_context()->storage()->dump();

    // validate
    tx = teseo.start_transaction();
    REQUIRE(tx.num_vertices() == 0);
    REQUIRE(tx.num_edges() == 0);
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
    }


    //global_context()->storage()->dump();
}

/**
 * Mix and match transactions, with multiple writers, inserting new vertices
 */
TEST_CASE("transactions1"){
    g_debugging_test = true;
    Teseo teseo;

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
 * Check that old transactions can still read their versions after newer transactions came
 */
TEST_CASE("transactions2"){
    g_debugging_test = true;
    Teseo teseo;

    uint64_t vertex_min = 10;
    uint64_t vertex_max = 10000;

    // add a few vertices
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_vertex(vertex_id) == false);
        tx.insert_vertex(vertex_id);
        REQUIRE(tx.has_vertex(vertex_id) == true);
        tx.commit();
    }

    // add a few edges
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();

        // check before insertion
        for(uint64_t j = vertex_min; j < i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }
        for(uint64_t j = i; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }

        tx.insert_edge(vertex_max, i, 1);

        // check after insertion
        for(uint64_t j = vertex_min; j <= i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }
        for(uint64_t j = i + 10; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }

        tx.commit();
    }
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();

        // check before deletion
        for(uint64_t j = vertex_min; j < i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }
        for(uint64_t j = i; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }

        tx.remove_edge(vertex_max, i);

        // check after deletion
        for(uint64_t j = vertex_min; j <= i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }
        for(uint64_t j = i + 10; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }

        tx.commit();
    }
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_edge(vertex_max, i) == false);
        tx.insert_edge(vertex_max, i, 1000 + i);
        tx.commit();
    }

    // create an old transaction
    auto tx_old = teseo.start_transaction();
    uint64_t weight = 0;
    for(uint64_t k = 0; k < 10; k++){
        for(uint64_t i = vertex_min; i <= vertex_max; i+= 10){
            for(uint64_t j = vertex_min; i <= vertex_max; i += 10){
                if(i == j) continue;
                auto tx = teseo.start_transaction();
                REQUIRE(tx_old.has_edge(i, j) == (i==vertex_max || j == vertex_max));
                if(tx.has_edge(i, j)){
                    tx.remove_edge(i, j);
                } else {
                    tx.insert_edge(i, j, weight++);

                }
                REQUIRE(tx_old.has_edge(i, j) == (i==vertex_max || j == vertex_max));
                tx.commit();
                REQUIRE(tx_old.has_edge(i, j) == (i==vertex_max || j == vertex_max));

                // validate the old transaction
                for(uint64_t v = vertex_min; v < vertex_max; v += 10){
                    REQUIRE(tx_old.has_edge(vertex_max, v) == true);
                    REQUIRE(tx_old.has_edge(v, vertex_max) == true);
                    REQUIRE(tx_old.get_weight(v, vertex_max) == 1000 + v);
                    REQUIRE(tx_old.get_weight(vertex_max, v) == 1000 + v);
                }
            }
        }
    }
}

/**
 * Validate old transactions on large sparse arrays
 */
TEST_CASE("transactions3"){
    g_debugging_test = false; // large sparse array
    Teseo teseo;

    uint64_t vertex_min = 10;
    uint64_t vertex_max = 20000;

    { // add a few vertices
        auto tx = teseo.start_transaction();
        for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
            REQUIRE(tx.has_vertex(vertex_id) == false);
            tx.insert_vertex(vertex_id);
            REQUIRE(tx.has_vertex(vertex_id) == true);
        }
        tx.commit();
    }

    // add a few edges
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();

        // check before insertion
        for(uint64_t j = vertex_min; j < i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }
        for(uint64_t j = i; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }

        tx.insert_edge(vertex_max, i, 1);

        // check after insertion
        for(uint64_t j = vertex_min; j <= i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }
        for(uint64_t j = i + 10; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }

        tx.commit();
    }
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();

        // check before deletion
        for(uint64_t j = vertex_min; j < i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }
        for(uint64_t j = i; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }

        tx.remove_edge(vertex_max, i);

        // check after deletion
        for(uint64_t j = vertex_min; j <= i; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == false);
        }
        for(uint64_t j = i + 10; j < vertex_max; j += 10){
            REQUIRE(tx.has_edge(vertex_max, j) == true);
        }

        tx.commit();
    }
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_edge(vertex_max, i) == false);
        tx.insert_edge(vertex_max, i, 1000 + i);
        tx.commit();
    }

    // create an old transaction
    auto tx_old1 = teseo.start_transaction();

    // a few more noise
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_edge(vertex_max, i) == true);
        tx.remove_edge(vertex_max, i);
        tx.insert_edge(vertex_max, i, 2000 + i);
        tx.commit();
    }

    // create another old transaction
    auto tx_old2 = teseo.start_transaction();


    // noise
    uint64_t weight = 0;
    for(uint64_t k = 0; k < 10; k++){
        for(uint64_t i = vertex_min; i <= vertex_max; i+= 10){
            for(uint64_t j = vertex_min; i <= vertex_max; i += 10){
                if(i == j) continue;
                auto tx = teseo.start_transaction();
                if(tx.has_edge(i, j)){
                    tx.remove_edge(i, j);
                } else {
                    tx.insert_edge(i, j, weight++);

                }
                tx.commit();
            }
        }
    }

    // validate the results
    for(uint64_t i = vertex_min; i < vertex_max; i += 10){
        REQUIRE(tx_old1.has_edge(i, vertex_max) == true);
        REQUIRE(tx_old1.has_edge(vertex_max, i) == true);
        REQUIRE(tx_old1.get_weight(i, vertex_max) == 1000 + i);
        REQUIRE(tx_old1.get_weight(vertex_max, i) == 1000 + i);
        REQUIRE(tx_old2.has_edge(i, vertex_max) == true);
        REQUIRE(tx_old2.has_edge(vertex_max, i) == true);
        REQUIRE(tx_old2.get_weight(i, vertex_max) == 2000 + i);
        REQUIRE(tx_old2.get_weight(vertex_max, i) == 2000 + i);
    }
    for(uint64_t i = vertex_min; i <= vertex_max -10; i+= 10){
        for(uint64_t j = vertex_min; i <= vertex_max -10; i += 10){
            if(i == j) continue;
            REQUIRE(tx_old1.has_edge(i, j) == false);
            REQUIRE(tx_old2.has_edge(i, j) == false);
        }
    }


}

