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
#include "teseo/memstore/memstore.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;

/**
 * Insert & remove a few edges, just a few
 * Let the async rebalancers do the dirty work
 */
TEST_CASE("memstore_edges", "[memstore] [sf] [df] [rebalance]"){
    Teseo teseo;

    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 1000;

    /**
     * Insert the vertices
     */
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
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
                REQUIRE( tx.has_edge(src, dst) == false );
                REQUIRE_NOTHROW( tx.insert_edge(src, dst, 10000 + dst) );
                REQUIRE( tx.has_edge(src, dst) == true );

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
                REQUIRE( tx.has_edge(src, dst) == true );
                REQUIRE_NOTHROW( tx.remove_edge(src, dst) );
                REQUIRE( tx.has_edge(src, dst) == false );

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
 * Check the counters for the total number of vertices and edges in the graph
 * are properly maintained
 */
TEST_CASE("memstore_global_properties", "[memstore]"){
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
 * Validate a long roll back, that spans multiple leaves
 * Let the async rebalancers do the dirty work
 */
TEST_CASE("memstore_rollback", "[memstore] [sf] [df] [rebalance]"){
    Teseo teseo;

    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 10000;

    Transaction tx = teseo.start_transaction();
    // insert some vertices
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
        tx.insert_vertex(vertex_id);
        REQUIRE( tx.has_vertex(vertex_id) == true );
        //this_thread::sleep_for(1ms); // give some time to the rebalancers to pick up
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

    // validate
    tx = teseo.start_transaction();
    REQUIRE(tx.num_vertices() == 0);
    REQUIRE(tx.num_edges() == 0);
    for(uint64_t vertex_id = vertex_min; vertex_id <= vertex_max; vertex_id += 10){
        REQUIRE( tx.has_vertex(vertex_id) == false );
    }
}

/**
 * Check that old transactions can still read their versions after newer transactions came
 */
TEST_CASE("memstore_transactions", "[sf] [df] [memstore]"){
    Teseo teseo;

    constexpr uint64_t vertex_min = 10;
    constexpr uint64_t vertex_max = 10000;

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
 * Remove 1k vertices, with no edges attaches, in mixed order
 */
TEST_CASE( "memstore_remove_vertex_1", "[sf] [df] [memstore] [remove_vertex]" ){
    const uint64_t max_vertex_id = 10000;
    uint64_t num_vertices = 0;
    Teseo teseo;

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

    // remove the vertices in strides
    uint64_t starting_points[] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    for(auto base : starting_points){
        for(uint64_t vertex_id = base; vertex_id <= max_vertex_id; vertex_id += 100){

            auto tx = teseo.start_transaction();
            REQUIRE(tx.num_vertices() == num_vertices);
            for(uint64_t v = 10; v <= max_vertex_id; v += 10){
                uint64_t vb = v % 100; if(vb == 0) { vb = 100; }
                bool expected = (vb > base || (vb==base && v >= vertex_id));

                REQUIRE(tx.has_vertex(v) == expected);
            }

            tx.remove_vertex(vertex_id);
            num_vertices--;

            for(uint64_t v = 10; v <= max_vertex_id; v += 10){
                uint64_t vb = v % 100; if(vb == 0) { vb = 100; }
                bool expected = (vb > base || (vb==base && v > vertex_id));
                REQUIRE(tx.has_vertex(v) == expected);
            }
            REQUIRE(tx.num_vertices() == num_vertices);

            tx.commit();

        }

    }

    // global_context()->memstore()->dump();
}

