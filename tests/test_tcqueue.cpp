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


#include "teseo/gc/item.hpp"
#include "teseo/gc/simple_queue.hpp"

using namespace std;
using namespace teseo::gc;

void tcqueue_deleter(void*){ /* nop */ }


TEST_CASE("tcqueue_simple", "[tcqueue]"){
    SimpleQueue queue { 6 };

    REQUIRE(queue.empty() == true);
    REQUIRE(queue.full() == false);

    REQUIRE( queue.push(Item{(void*) 0x1, tcqueue_deleter}) == true );
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );

    REQUIRE( queue.push(Item{(void*) 0x2, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x3, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x4, tcqueue_deleter}) == true );
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue.size() == 4 );

    REQUIRE( queue.push(Item{(void*) 0x5, tcqueue_deleter}) == true );
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == true );
    REQUIRE( queue.size() == 5 );

    REQUIRE( queue.push(Item{(void*) 0x6, tcqueue_deleter}) == false );

    queue.pop(3);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue.size() == 2 );
    REQUIRE( queue[0].pointer() == (void*) 0x4 );
    REQUIRE( queue[1].pointer() == (void*) 0x5 );

    REQUIRE( queue.push(Item{(void*) 0x6, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x7, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x8, tcqueue_deleter}) == true );
    REQUIRE( queue.size() == 5);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == true );
    REQUIRE( queue.push(Item{(void*) 0x9, tcqueue_deleter}) == false );
    REQUIRE( queue.size() == 5);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == true );

    REQUIRE( queue[0].pointer() == (void*) 0x4 );
    REQUIRE( queue[1].pointer() == (void*) 0x5 );
    REQUIRE( queue[2].pointer() == (void*) 0x6 );
    REQUIRE( queue[3].pointer() == (void*) 0x7 );
    REQUIRE( queue[4].pointer() == (void*) 0x8 );

    queue.pop(3);
    // [ 7, 8, x, x, x, x]

    REQUIRE( queue.size() == 2);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue[0].pointer() == (void*) 0x7 );
    REQUIRE( queue[1].pointer() == (void*) 0x8 );

    queue.pop(1);
    REQUIRE( queue.size() == 1);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue[0].pointer() == (void*) 0x8 );

    queue.pop(1);
    REQUIRE( queue.size() == 0 );
    REQUIRE( queue.empty() == true );
    REQUIRE( queue.full() == false );


    REQUIRE( queue.push(Item{(void*) 0x9, tcqueue_deleter}) == true );
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );

    REQUIRE( queue.push(Item{(void*) 0x10, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x11, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x12, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x13, tcqueue_deleter}) == true );
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == true );
    REQUIRE( queue.push(Item{(void*) 0x14, tcqueue_deleter}) == false );

    // [ 13, x, 9, 10, 11, 12]

    queue.pop(5);
    REQUIRE( queue.empty() == true );
    REQUIRE( queue.full() == false );
    REQUIRE( queue.size() == 0 );

    REQUIRE( queue.push(Item{(void*) 0x14, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x15, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x16, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x17, tcqueue_deleter}) == true );

    // read, standard case, left to right
    // [ x, 14, 15, 16, 17, x]
    REQUIRE( queue.size() == 4 );
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue[0].pointer() == (void*) 0x14 );
    REQUIRE( queue[1].pointer() == (void*) 0x15 );
    REQUIRE( queue[2].pointer() == (void*) 0x16 );
    REQUIRE( queue[3].pointer() == (void*) 0x17 );

    // read up to the end
    queue.pop(3);
    REQUIRE( queue.push(Item{(void*) 0x18, tcqueue_deleter}) == true );
    // [ x, x, x, x, 17, 18]
    REQUIRE(queue.size() == 2);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue[0].pointer() == (void*) 0x17 );
    REQUIRE( queue[1].pointer() == (void*) 0x18 );

    // circle back
    REQUIRE( queue.push(Item{(void*) 0x19, tcqueue_deleter}) == true );
    // [19, x, x, x, 17, 18]
    REQUIRE(queue.size() == 3);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == false );
    REQUIRE( queue[0].pointer() == (void*) 0x17 );
    REQUIRE( queue[1].pointer() == (void*) 0x18 );
    REQUIRE( queue[2].pointer() == (void*) 0x19 );

    // fill it completely
    REQUIRE( queue.push(Item{(void*) 0x20, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x21, tcqueue_deleter}) == true );
    // [19, 20, 21, x, 17, 18]
    REQUIRE(queue.size() == 5);
    REQUIRE( queue.empty() == false );
    REQUIRE( queue.full() == true );
    REQUIRE( queue[0].pointer() == (void*) 0x17 );
    REQUIRE( queue[1].pointer() == (void*) 0x18 );
    REQUIRE( queue[2].pointer() == (void*) 0x19 );
    REQUIRE( queue[3].pointer() == (void*) 0x20 );
    REQUIRE( queue[4].pointer() == (void*) 0x21 );

    // and finally empty it again
    queue.pop(5);
    REQUIRE( queue.size() == 0 );
    REQUIRE( queue.empty() == true );
    REQUIRE( queue.full() == false );
}

TEST_CASE("tcqueue_resize", "[tcqueue]"){
    SimpleQueue queue { 5 };

    REQUIRE( queue.push(Item{(void*) 0x1, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x2, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x3, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x4, tcqueue_deleter}) == true );

    queue.pop(3);
    REQUIRE( queue.push(Item{(void*) 0x5, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x6, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x7, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x8, tcqueue_deleter}) == false );

    // [6, 7, x, 4, 5]
    REQUIRE( queue.full() == true );
    REQUIRE( queue.size() == 4 );
    REQUIRE( queue[0].pointer() == (void*) 0x4 );
    REQUIRE( queue[1].pointer() == (void*) 0x5 );
    REQUIRE( queue[2].pointer() == (void*) 0x6 );
    REQUIRE( queue[3].pointer() == (void*) 0x7 );

    queue.resize();
    // [4, 5, 6, 7, x, x, x, ...]
    REQUIRE( queue.full() == false );
    REQUIRE( queue.size() == 4 );
    REQUIRE( queue[0].pointer() == (void*) 0x4 );
    REQUIRE( queue[1].pointer() == (void*) 0x5 );
    REQUIRE( queue[2].pointer() == (void*) 0x6 );
    REQUIRE( queue[3].pointer() == (void*) 0x7 );


    REQUIRE( queue.push(Item{(void*) 0x8, tcqueue_deleter}) == true );
    REQUIRE( queue.push(Item{(void*) 0x9, tcqueue_deleter}) == true );
    REQUIRE( queue.size() == 6);
    REQUIRE( queue[0].pointer() == (void*) 0x4 );
    REQUIRE( queue[1].pointer() == (void*) 0x5 );
    REQUIRE( queue[2].pointer() == (void*) 0x6 );
    REQUIRE( queue[3].pointer() == (void*) 0x7 );
    REQUIRE( queue[4].pointer() == (void*) 0x8 );
    REQUIRE( queue[5].pointer() == (void*) 0x9 );
}


