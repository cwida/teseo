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

#include <iostream>

#include "../src/storage.hpp"

using namespace std;
using namespace teseo::internal;

TEST_CASE("gate"){
    constexpr uint64_t num_segments = 8;
    uint64_t memory_footprint = MemStore::Gate::memory_footprint(num_segments);
    cout << "[TEST_CASE(\"gate\")] memory footprint: " << memory_footprint << endl;

    void* heap = malloc(memory_footprint);
    REQUIRE(heap != nullptr);
    MemStore::Gate* gate = new (heap) MemStore::Gate(0, num_segments);

    gate->lock();

    // Fence keys
    REQUIRE( gate->check_fence_keys(1) == MemStore::Gate::Direction::INVALID );
    gate->set_fence_keys(2, 102);
    REQUIRE( gate->check_fence_keys(1) == MemStore::Gate::Direction::LEFT );
    REQUIRE( gate->check_fence_keys(2) == MemStore::Gate::Direction::GO_AHEAD );
    REQUIRE( gate->check_fence_keys(102) == MemStore::Gate::Direction::GO_AHEAD );
    REQUIRE( gate->check_fence_keys(103) == MemStore::Gate::Direction::RIGHT );

    // Separator keys
    for(uint64_t i = 0; i < num_segments; i++){
        gate->set_separator_key(i, (i + 2) * 10); // 0 -> 20, 1 -> 30, 2 -> 40, ... , 7 -> 90
    }

    REQUIRE( gate->find(2) == 0 ); // because it depends on the min fence key
    REQUIRE( gate->find(29) == 0 );
    REQUIRE( gate->find(30) == 1 );
    REQUIRE( gate->find(31) == 1 );
    REQUIRE( gate->find(39) == 1 );
    REQUIRE( gate->find(40) == 2 );
    REQUIRE( gate->find(41) == 2 );
    REQUIRE( gate->find(49) == 2 );
    REQUIRE( gate->find(50) == 3 );
    REQUIRE( gate->find(51) == 3 );
    REQUIRE( gate->find(51) == 3 );
    REQUIRE( gate->find(59) == 3 );
    REQUIRE( gate->find(60) == 4 );
    REQUIRE( gate->find(61) == 4 );
    REQUIRE( gate->find(69) == 4 );
    REQUIRE( gate->find(89) == 6 );
    REQUIRE( gate->find(90) == 7 );
    REQUIRE( gate->find(91) == 7 );
    REQUIRE( gate->find(101) == 7 );
    REQUIRE( gate->find(101) == 7 );
    REQUIRE( gate->find(102) == 7 ); // last value allowed by the upper fence key

    gate->unlock();

    gate->~Gate(); gate = nullptr;
    free(heap); heap = nullptr; // run the check with valgrind as well
}

TEST_CASE("leaf_alloc"){
    auto leaf = MemStore::Leaf::allocate();
    leaf->dump();
    MemStore::Leaf::deallocate(leaf);
}
