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

    global_context()->storage()->dump();
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
TEST_CASE("edge_remove_basic"){
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
