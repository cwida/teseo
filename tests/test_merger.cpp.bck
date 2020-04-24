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

#include "../src/memstore-old/merger.hpp"

#include "catch.hpp"

#include <iostream>

#include "teseo.hpp"
#include "../src/context.hpp"
#include "../src/memstore/sparse_array.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::internal::context;
using namespace teseo::internal::memstore;

TEST_CASE("merger_run_daemon", "[merger]"){
    g_debugging_test = false;
    Teseo teseo;
    REQUIRE_NOTHROW( global_context()->storage()->merger()->execute_now() );
}

TEST_CASE("merger_start_and_stop", "[merger]"){
    g_debugging_test = false;
    Teseo teseo;
    global_context()->storage()->merger()->stop();
    global_context()->storage()->merger()->start();
    global_context()->storage()->merger()->stop();
    global_context()->storage()->merger()->start();
    global_context()->storage()->merger()->stop();
}

TEST_CASE("merger_prune", "[merger]"){ // check vertices have been pruned
    g_debugging_test = true;

    Teseo teseo;

    {
        auto tx = teseo.start_transaction();
        tx.insert_vertex(10);
        tx.commit();
    }

    {
        auto tx = teseo.start_transaction();
        tx.remove_vertex(10);
        tx.commit();
    }


    REQUIRE_NOTHROW( global_context()->storage()->merger()->execute_now() );
    //global_context()->storage()->dump(); // check there are no versions around
}

TEST_CASE("merger_merge", "[merger]"){
    g_debugging_test = true;
    Teseo teseo;
    uint64_t vertex_max = 1000;

    for(uint64_t vertex_id = 10; vertex_id <= vertex_max; vertex_id += 10){
        auto tx = teseo.start_transaction();
        tx.insert_vertex(vertex_id);
        tx.commit();
    }

    // remove half of the vertices
    uint64_t base_vertices[] = {20, 40, 60, 80};
    for(auto base : base_vertices){
        for(uint64_t vertex_id = base; vertex_id <= vertex_max; vertex_id += 100){
            auto tx = teseo.start_transaction();
            tx.remove_vertex(vertex_id);
            tx.commit();
        }
    }

    //global_context()->storage()->dump(); // expect two chunks present
    global_context()->storage()->merger()->execute_now();
    //global_context()->storage()->dump(); // expect only one chunk present
}


