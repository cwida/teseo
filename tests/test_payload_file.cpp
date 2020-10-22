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

#include <cstdlib> // free
#include <iostream>

#include "teseo/context/global_context.hpp"
#include "teseo/memstore/payload_file.hpp"
#include "teseo/memstore/payload_iterator.hpp"

using namespace std;
using namespace teseo::context;
using namespace teseo::memstore;

/**
 * Insert some weights in the payload file, check they are properly stored
 */
TEST_CASE("pf_insert_lhs", "[pf] [memstore]"){
    auto file = create_payload_block();

    file->insert(0, 1.0);
    REQUIRE(file->get(0) == 1.0);

    file->insert(1, 2.0);
    REQUIRE(file->get(0) == 1.0);
    REQUIRE(file->get(1) == 2.0);

    file->insert(2, 3.0);
    REQUIRE(file->get(0) == 1.0);
    REQUIRE(file->get(1) == 2.0);
    REQUIRE(file->get(2) == 3.0);

    // test shifts
    file->insert(2, 2.5);
    REQUIRE(file->get(0) == 1.0);
    REQUIRE(file->get(1) == 2.0);
    REQUIRE(file->get(2) == 2.5);
    REQUIRE(file->get(3) == 3.0);

    file->insert(1, 1.5);
    REQUIRE(file->get(0) == 1.0);
    REQUIRE(file->get(1) == 1.5);
    REQUIRE(file->get(2) == 2.0);
    REQUIRE(file->get(3) == 2.5);
    REQUIRE(file->get(4) == 3.0);

    file->insert(0, 0.5);
    REQUIRE(file->get(0) == 0.5);
    REQUIRE(file->get(1) == 1.0);
    REQUIRE(file->get(2) == 1.5);
    REQUIRE(file->get(3) == 2.0);
    REQUIRE(file->get(4) == 2.5);
    REQUIRE(file->get(5) == 3.0);

    ::free(file);
}

/**
 * Test splits in an append only scenario
 */
TEST_CASE("pf_split1", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();
    constexpr uint64_t sz = 49;

    for(uint64_t i = 0; i < sz; i++){
        file->insert(i, i);

        for(uint64_t j = 0; j <= i; j++){
            REQUIRE(file->get(j) == j);
        }
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Test insertions in the RHS of a block
 */
TEST_CASE("pf_insert_rhs", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();
    constexpr uint64_t sz = 17; // create a split, so that we have two blocks with elts in the RHS

    for(uint64_t i = 0; i < sz; i++){
        file->insert(i, i);
    }
    for(uint64_t i = 0; i < sz; i++){
        REQUIRE(file->get(i) == i);
    }

    // insert at the start of the RHS. Technically the implementation always to attempt to append at
    // the end of the LHS rather than inserting at the very start of the RHS.
    file->insert(5, 4.5); // insert after the first element of the block
    file->insert(14, 12.5); // second block

    // append at the end of the rhs
    file->insert(9, 7.5); // first block
    file->insert(20, 16.5); // second block

    // insert at the middle
    file->insert(7, 5.5); // first block
    file->insert(18, 13.5); // second block

    //file->dump();

    // validate
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 4);
    REQUIRE(file->get(5) == 4.5);
    REQUIRE(file->get(6) == 5);
    REQUIRE(file->get(7) == 5.5);
    REQUIRE(file->get(8) == 6);
    REQUIRE(file->get(9) == 7);
    REQUIRE(file->get(10) == 7.5);
    REQUIRE(file->get(11) == 8);
    REQUIRE(file->get(12) == 9);
    REQUIRE(file->get(13) == 10);
    REQUIRE(file->get(14) == 11);
    REQUIRE(file->get(15) == 12);
    REQUIRE(file->get(16) == 12.5);
    REQUIRE(file->get(17) == 13);
    REQUIRE(file->get(18) == 13.5);
    REQUIRE(file->get(19) == 14);
    REQUIRE(file->get(20) == 15);
    REQUIRE(file->get(21) == 16);
    REQUIRE(file->get(22) == 16.5);

    // clean up
    file->clear();
    destroy_payload_block(file);
}

/**
 * Test deletions in the LHS of a block, with no RHS present
 */
TEST_CASE("pf_remove_lhs1", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();

    for(uint64_t i = 0; i < 8; i++){
        file->insert(i, i);
    }

    file->remove(7); // remove at the end of the first block
    file->remove(0); // remove at the start of the block
    file->remove(3); // remove at the middle of the block

    // validate
    REQUIRE(file->get(0) == 1);
    REQUIRE(file->get(1) == 2);
    REQUIRE(file->get(2) == 3);
    REQUIRE(file->get(3) == 5);
    REQUIRE(file->get(4) == 6);

    // clean up
    file->clear();
    destroy_payload_block(file);
}

/**
 * Test deletions in the LHS of two blocks
 */
TEST_CASE("pf_remove_lhs2", "[pf][memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();

    for(uint64_t i = 0; i < 16; i++){
        file->insert(i, i);
    }

    file->insert(0, -0.5); // insert at the start of the first block
    file->insert(5, 3.5); // append at the end of the first block
    file->insert(3, 1.5); // insert in the middle of the first block

    // we cannot insert at the start of the second block. The insertion becomes an append at the end of the first block
    file->insert(13, 9.5); // insert in the middle of the second block
    file->insert(16, 11.5); // append at the end of the second block

    // check that all insertions succeeded
    REQUIRE(file->get(0) == -0.5);
    REQUIRE(file->get(1) == 0);
    REQUIRE(file->get(2) == 1);
    REQUIRE(file->get(3) == 1.5);
    REQUIRE(file->get(4) == 2);
    REQUIRE(file->get(5) == 3);
    REQUIRE(file->get(6) == 3.5);
    REQUIRE(file->get(7) == 4);
    REQUIRE(file->get(8) == 5);
    REQUIRE(file->get(9) == 6);
    REQUIRE(file->get(10) == 7);
    REQUIRE(file->get(11) == 8);
    REQUIRE(file->get(12) == 9);
    REQUIRE(file->get(13) == 9.5);
    REQUIRE(file->get(14) == 10);
    REQUIRE(file->get(15) == 11);
    REQUIRE(file->get(16) == 11.5);
    REQUIRE(file->get(17) == 12);
    REQUIRE(file->get(18) == 13);
    REQUIRE(file->get(19) == 14);
    REQUIRE(file->get(20) == 15);

    // remove the first element from the LHS of the first block
    file->remove(0);
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 1.5);
    REQUIRE(file->get(3) == 2);
    REQUIRE(file->get(4) == 3);
    REQUIRE(file->get(5) == 3.5);
    REQUIRE(file->get(6) == 4);
    REQUIRE(file->get(7) == 5);
    REQUIRE(file->get(8) == 6);
    REQUIRE(file->get(9) == 7);
    REQUIRE(file->get(10) == 8);
    REQUIRE(file->get(11) == 9);
    REQUIRE(file->get(12) == 9.5);
    REQUIRE(file->get(13) == 10);
    REQUIRE(file->get(14) == 11);
    REQUIRE(file->get(15) == 11.5);
    REQUIRE(file->get(16) == 12);
    REQUIRE(file->get(17) == 13);
    REQUIRE(file->get(18) == 14);
    REQUIRE(file->get(19) == 15);

    // remove the element in the middle of the LHS in the first block
    file->remove(2); // value 1.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 3.5);
    REQUIRE(file->get(5) == 4);
    REQUIRE(file->get(6) == 5);
    REQUIRE(file->get(7) == 6);
    REQUIRE(file->get(8) == 7);
    REQUIRE(file->get(9) == 8);
    REQUIRE(file->get(10) == 9);
    REQUIRE(file->get(11) == 9.5);
    REQUIRE(file->get(12) == 10);
    REQUIRE(file->get(13) == 11);
    REQUIRE(file->get(14) == 11.5);
    REQUIRE(file->get(15) == 12);
    REQUIRE(file->get(16) == 13);
    REQUIRE(file->get(17) == 14);
    REQUIRE(file->get(18) == 15);

    // remove the element at the end of the LHS in the first block
    file->remove(4);
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 4);
    REQUIRE(file->get(5) == 5);
    REQUIRE(file->get(6) == 6);
    REQUIRE(file->get(7) == 7);
    REQUIRE(file->get(8) == 8);
    REQUIRE(file->get(9) == 9);
    REQUIRE(file->get(10) == 9.5);
    REQUIRE(file->get(11) == 10);
    REQUIRE(file->get(12) == 11);
    REQUIRE(file->get(13) == 11.5);
    REQUIRE(file->get(14) == 12);
    REQUIRE(file->get(15) == 13);
    REQUIRE(file->get(16) == 14);
    REQUIRE(file->get(17) == 15);

    // remove the element at the start of the LHS of the second block
    file->remove(8); // value 8
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 4);
    REQUIRE(file->get(5) == 5);
    REQUIRE(file->get(6) == 6);
    REQUIRE(file->get(7) == 7);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 9.5);
    REQUIRE(file->get(10) == 10);
    REQUIRE(file->get(11) == 11);
    REQUIRE(file->get(12) == 11.5);
    REQUIRE(file->get(13) == 12);
    REQUIRE(file->get(14) == 13);
    REQUIRE(file->get(15) == 14);
    REQUIRE(file->get(16) == 15);

    // remove in the middle of the LHS of the second block
    file->remove(9); // value 9.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 4);
    REQUIRE(file->get(5) == 5);
    REQUIRE(file->get(6) == 6);
    REQUIRE(file->get(7) == 7);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 10);
    REQUIRE(file->get(10) == 11);
    REQUIRE(file->get(11) == 11.5);
    REQUIRE(file->get(12) == 12);
    REQUIRE(file->get(13) == 13);
    REQUIRE(file->get(14) == 14);
    REQUIRE(file->get(15) == 15);

    // remove at the end of the LHS of the second block
    file->remove(11); // value 11.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 4);
    REQUIRE(file->get(5) == 5);
    REQUIRE(file->get(6) == 6);
    REQUIRE(file->get(7) == 7);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 10);
    REQUIRE(file->get(10) == 11);
    REQUIRE(file->get(11) == 12);
    REQUIRE(file->get(12) == 13);
    REQUIRE(file->get(13) == 14);
    REQUIRE(file->get(14) == 15);

    // clean up
    file->clear();
    destroy_payload_block(file);
}


/**
 * Test deletions in the RHS of two blocks
 */
TEST_CASE("pf_remove_rhs", "[pf][memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();

    for(uint64_t i = 0; i <= 16; i++){
        file->insert(i, i);
    }

    // insert in the middle and the end of the first LHS of the first block
    file->insert(8, 7.5); // at the end
    file->insert(6, 5.5); // in the middle
    // same for the second block
    file->insert(19, 16.5); // at the end
    file->insert(16, 13.5); // in the middle

    // remove the last element in the RHS of the first block
    file->remove(9); // value 7.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 4);
    REQUIRE(file->get(5) == 5);
    REQUIRE(file->get(6) == 5.5);
    REQUIRE(file->get(7) == 6);
    REQUIRE(file->get(8) == 7);
    REQUIRE(file->get(9) == 8);
    REQUIRE(file->get(10) == 9);
    REQUIRE(file->get(11) == 10);
    REQUIRE(file->get(12) == 11);
    REQUIRE(file->get(13) == 12);
    REQUIRE(file->get(14) == 13);
    REQUIRE(file->get(15) == 13.5);
    REQUIRE(file->get(16) == 14);
    REQUIRE(file->get(17) == 15);
    REQUIRE(file->get(18) == 16);
    REQUIRE(file->get(19) == 16.5);

    // remove the element of the start in the RHS of the first block
    file->remove(4); // value 4
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 5);
    REQUIRE(file->get(5) == 5.5);
    REQUIRE(file->get(6) == 6);
    REQUIRE(file->get(7) == 7);
    REQUIRE(file->get(8) == 8);
    REQUIRE(file->get(9) == 9);
    REQUIRE(file->get(10) == 10);
    REQUIRE(file->get(11) == 11);
    REQUIRE(file->get(12) == 12);
    REQUIRE(file->get(13) == 13);
    REQUIRE(file->get(14) == 13.5);
    REQUIRE(file->get(15) == 14);
    REQUIRE(file->get(16) == 15);
    REQUIRE(file->get(17) == 16);
    REQUIRE(file->get(18) == 16.5);

    // remove the element at the middle of the RHS of the first block
    file->remove(5); // value 5.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 5);
    REQUIRE(file->get(5) == 6);
    REQUIRE(file->get(6) == 7);
    REQUIRE(file->get(7) == 8);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 10);
    REQUIRE(file->get(10) == 11);
    REQUIRE(file->get(11) == 12);
    REQUIRE(file->get(12) == 13);
    REQUIRE(file->get(13) == 13.5);
    REQUIRE(file->get(14) == 14);
    REQUIRE(file->get(15) == 15);
    REQUIRE(file->get(16) == 16);
    REQUIRE(file->get(17) == 16.5);

    // remove the element at the start of the RHS in the second block
    file->remove(11); // value 12
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 5);
    REQUIRE(file->get(5) == 6);
    REQUIRE(file->get(6) == 7);
    REQUIRE(file->get(7) == 8);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 10);
    REQUIRE(file->get(10) == 11);
    REQUIRE(file->get(11) == 13);
    REQUIRE(file->get(12) == 13.5);
    REQUIRE(file->get(13) == 14);
    REQUIRE(file->get(14) == 15);
    REQUIRE(file->get(15) == 16);
    REQUIRE(file->get(16) == 16.5);

    // remove the last element of the second block
    file->remove(16); // value 16.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 5);
    REQUIRE(file->get(5) == 6);
    REQUIRE(file->get(6) == 7);
    REQUIRE(file->get(7) == 8);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 10);
    REQUIRE(file->get(10) == 11);
    REQUIRE(file->get(11) == 13);
    REQUIRE(file->get(12) == 13.5);
    REQUIRE(file->get(13) == 14);
    REQUIRE(file->get(14) == 15);
    REQUIRE(file->get(15) == 16);

    // remove an element at the middle of the second block
    file->remove(12); // value 13.5
    REQUIRE(file->get(0) == 0);
    REQUIRE(file->get(1) == 1);
    REQUIRE(file->get(2) == 2);
    REQUIRE(file->get(3) == 3);
    REQUIRE(file->get(4) == 5);
    REQUIRE(file->get(5) == 6);
    REQUIRE(file->get(6) == 7);
    REQUIRE(file->get(7) == 8);
    REQUIRE(file->get(8) == 9);
    REQUIRE(file->get(9) == 10);
    REQUIRE(file->get(10) == 11);
    REQUIRE(file->get(11) == 13);
    REQUIRE(file->get(12) == 14);
    REQUIRE(file->get(13) == 15);
    REQUIRE(file->get(14) == 16);

    // clean up
    file->clear();
    destroy_payload_block(file);
}


/**
 * Split when all elements loaded in the RHS of the block. Split the first
 * block by appending an element at the end.
 */
TEST_CASE("pf_split2", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();

    file->insert(0, 0);
    file->insert(0, -1);
    file->insert(0, -2);
    file->insert(0, -3);
    file->insert(0, -4);

    for(uint64_t i = 24 -1; i >= 8; i--){
        file->insert(5, i);
    }

    // remove the first four elements from the LHS
    file->remove(0);
    file->remove(0);
    file->remove(0);
    file->remove(0);
    REQUIRE(file->get(0) == 0);

    for(uint64_t i = 1; i <= 7; i++){
        file->insert(i, i);
    }

    // right now, all elements are in the RHS of the first block
    for(uint64_t i = 0; i < 24; i++){
        REQUIRE(file->get(i) == i);
    }

    // split by appending a new element at the end of the first block
    file->insert(16, 15.5);

    // validate
    REQUIRE(file->get(16) == 15.5);
    for(uint64_t i = 0; i < 16; i++){
        REQUIRE(file->get(i) == i);
    }
    for(uint64_t i = 16; i < 24; i++){
        REQUIRE(file->get(i +1) == i);
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Split when all elements loaded in the RHS of the block. Split the first
 * block by inserting an element at the start.
 */
TEST_CASE("pf_split3", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();

    file->insert(0, 0);
    file->insert(0, -1);
    file->insert(0, -2);
    file->insert(0, -3);
    file->insert(0, -4);

    for(uint64_t i = 24 -1; i >= 8; i--){
        file->insert(5, i);
    }

    // remove the first four elements from the LHS
    file->remove(0);
    file->remove(0);
    file->remove(0);
    file->remove(0);
    REQUIRE(file->get(0) == 0);

    for(uint64_t i = 1; i <= 7; i++){
        file->insert(i, i);
    }

    // right now, all elements are in the RHS of the first block
    for(uint64_t i = 0; i < 24; i++){
        REQUIRE(file->get(i) == i);
    }

    // split by inserting a new element at the start of the first block
    file->insert(0, -1);

    // validate
    for(int64_t i = 0; i <= 24; i++){
        REQUIRE(file->get(i) == i -1);
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Split when all elements loaded in the RHS of the block. Split the first
 * block by insert a new element in the middle.
 */
TEST_CASE("pf_split4", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();

    file->insert(0, 0);
    file->insert(0, -1);
    file->insert(0, -2);
    file->insert(0, -3);
    file->insert(0, -4);

    for(uint64_t i = 24 -1; i >= 8; i--){
        file->insert(5, i);
    }

    // remove the first four elements from the LHS
    file->remove(0);
    file->remove(0);
    file->remove(0);
    file->remove(0);
    REQUIRE(file->get(0) == 0);

    for(uint64_t i = 1; i <= 7; i++){
        file->insert(i, i);
    }

    // right now, all elements are in the RHS of the first block
    for(uint64_t i = 0; i < 24; i++){
        REQUIRE(file->get(i) == i);
    }

    // split by inserting a new element in the middle of the first block
    file->insert(5, 4.5);

    // validate
    REQUIRE(file->get(5) == 4.5);
    for(uint64_t i = 0; i < 5; i++){
        REQUIRE(file->get(i) == i);
    }
    for(uint64_t i = 5; i < 24; i++){
        REQUIRE(file->get(i +1) == i);
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Merge blocks together. Perform both insertions and removals at the end
 */
TEST_CASE("pf_merge1", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();
    int64_t sz = 64;

    // insertions
    for(int64_t i = 0; i < sz; i++){
        file->insert(i, i);
    }

    // removals
    for(int64_t i = sz -1; i >= 0; i--){
        for(int64_t j = 0; j <= i; j++){
            REQUIRE(file->get(j) == j);
        }

        file->remove(i);
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Merge blocks together. Perform deletions at the start.
 */
TEST_CASE("pf_merge2", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();
    int64_t sz = 64;

    // insertions
    for(int64_t i = 0; i < sz; i++){
        file->insert(i, i);
    }

    // removals
    for(int64_t i = 0; i < sz; i++){
        for(int64_t j = 0; j < sz - i; j++){
            REQUIRE(file->get(j) == j + i);
        }

        file->remove(0);
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Check the results retrieved with the iterator
 */
TEST_CASE("pf_iterator", "[pf] [memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();
    uint64_t sz = 64;

    for(uint64_t i = 0; i < sz; i++){
        file->insert(i, i);

        auto it = file->iterator();
        for(uint64_t j = 0; j <= i; j++){
            REQUIRE(it.has_next() == true);
            double retrieved = it.next();
            REQUIRE(retrieved == j);
        }
        REQUIRE(it.has_next() == false);
    }

    file->clear();
    destroy_payload_block(file);
}

/**
 * Validate the method skip in the iterator
 */
TEST_CASE("pf_iterator_skip", "[pf][memstore]"){
    GlobalContext context; // for the gc
    auto file = create_payload_block();
    uint64_t sz = 64;

    for(uint64_t i = 0; i < sz; i++){
        file->insert(i, i);

        for(uint64_t j = 0; j <= i; j++){
            auto it = file->iterator();
            it.skip(j);

            for(uint64_t k = 0; k <= (i -j); k++){
                REQUIRE(it.has_next() == true);
                double retrieved = it.next();
                REQUIRE(retrieved == j + k);
            }

            REQUIRE(it.has_next() == false);
        }

    }

    file->clear();
    destroy_payload_block(file);
}
