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

#include "teseo.hpp"
#include "../src/context.hpp"
#include "../src/memstore/sparse_array.hpp"
#include "../src/memstore-old/merger.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::internal::context;

TEST_CASE("rmv_empty", "[remove_vertex]" ){ // Attempt to remove an non existing vertex from an empty sparse array
    g_debugging_test = true;
    Teseo teseo;
    auto tx = teseo.start_transaction();
    REQUIRE_THROWS_AS(tx.remove_vertex(20), LogicalError); // Vertex 20 does not exist
    REQUIRE(tx.num_vertices() == 0);
    tx.insert_vertex(10);
    REQUIRE(tx.num_vertices() == 1);
    REQUIRE_THROWS_AS(tx.remove_vertex(20), LogicalError); // Vertex 20 does not exist
    REQUIRE(tx.num_vertices() == 1);
}

TEST_CASE("rmv_single", "[remove_vertex]" ) { // Attempt to remove the only vertex in the sparse array
    g_debugging_test = true;
    Teseo teseo;

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

TEST_CASE( "rmv_l2r", "[remove_vertex]" ){ // Remove 10 vertices, with no edges attaches from left 2 right
    const uint64_t max_vertex_id = 100;
    uint64_t num_vertices = 0;
    g_debugging_test = true;
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

TEST_CASE( "rmv_r2l", "[remove_vertex]" ){ // Remove 10 vertices, with no edges attaches, from right to left
    const uint64_t max_vertex_id = 100;
    uint64_t num_vertices = 0;
    g_debugging_test = true;
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

    // remove the vertices one by one
    for(uint64_t vertex_id = max_vertex_id; vertex_id >= 10; vertex_id -= 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v <= vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }

        tx.remove_vertex(vertex_id);
        num_vertices--;

        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v < vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }
        REQUIRE(tx.num_vertices() == num_vertices);

        tx.commit();
    }
}

TEST_CASE( "rmv_l2rx", "[remove_vertex]" ){ // Remove 100 vertices, with no edges attaches from left 2 right
    const uint64_t max_vertex_id = 1000;
    uint64_t num_vertices = 0;
    g_debugging_test = true;
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

TEST_CASE( "rmv_r2lx", "[remove_vertex]" ){ // Remove 100 vertices, with no edges attaches, from right to left
    const uint64_t max_vertex_id = 1000;
    uint64_t num_vertices = 0;
    g_debugging_test = true;
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

    // remove the vertices one by one
    for(uint64_t vertex_id = max_vertex_id; vertex_id >= 10; vertex_id -= 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.num_vertices() == num_vertices);
        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v <= vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }

        tx.remove_vertex(vertex_id);
        num_vertices--;

        for(uint64_t v = 10; v <= max_vertex_id; v += 10){
            bool expected = v < vertex_id;
            REQUIRE(tx.has_vertex(v) == expected);
        }
        REQUIRE(tx.num_vertices() == num_vertices);

        tx.commit();
    }
}

TEST_CASE( "rmv_1k", "[remove_vertex]" ){ // Remove 1k vertices, with no edges attaches, in mixed order
    const uint64_t max_vertex_id = 10000;
    uint64_t num_vertices = 0;
    g_debugging_test = true;
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

    global_context()->storage()->merger()->execute_now();

    // remove the vertices in springs
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

        global_context()->storage()->merger()->execute_now();
    }
}

