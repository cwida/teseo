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

#include <iostream>
#include <thread>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/rebalance/merger_service.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;


TEST_CASE("merger_start_and_stop", "[merger]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();

    memstore->merger()->stop();
    memstore->merger()->start();
    memstore->merger()->stop();
    memstore->merger()->start();
    memstore->merger()->stop();
}

TEST_CASE("merger_prune", "[merger]"){ // check vertices have been pruned
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();

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


    memstore->merger()->execute_now();


    ScopedEpoch epoch; // to perform an index traversal
    Segment* segment = memstore->index()->find(0).leaf()->get_segment(0);
    REQUIRE(segment->used_space() == 0);
}

TEST_CASE("merger_merge", "[merger]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    MergerService* merger = memstore->merger();

    constexpr uint64_t vertex_max = 1000;

    for(uint64_t vertex_id = 10; vertex_id <= vertex_max; vertex_id += 10){
        auto tx = teseo.start_transaction();
        tx.insert_vertex(vertex_id);
        tx.commit();
        this_thread::sleep_for(10ms); // give time to the rebalancers to pick up
    }

    // remove half of the vertices
    uint64_t base_vertices[] = {20, 40, 50, 60, 80};
    for(auto base : base_vertices){
        for(uint64_t vertex_id = base; vertex_id <= vertex_max; vertex_id += 100){
            auto tx = teseo.start_transaction();
            tx.remove_vertex(vertex_id);
            tx.commit();
            this_thread::sleep_for(10ms); // give time to the rebalancers to pick up
        }
    }

    merger->execute_now();
    memstore->dump();
}


