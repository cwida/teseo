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

#include <cstdint>
#include <iostream>

#include "teseo/util/timer.hpp"
#include "teseo/util/circular_array.hpp"

using namespace std;
using namespace teseo::util;

TEST_CASE("ca_resize1", "[circular_array]") {
    CircularArray<int> queue {4};

    REQUIRE(queue.size() == 0);
    REQUIRE(queue.empty() == true);

    queue.append(0);
    REQUIRE(queue.size() == 1);
    REQUIRE(queue.empty() == false);

    queue.append(1);
    queue.append(2);
    queue.append(3);

    REQUIRE(queue.size() == 4);
    REQUIRE(queue.empty() == false);
    REQUIRE(queue[0] == 0);
    REQUIRE(queue[1] == 1);
    REQUIRE(queue[2] == 2);
    REQUIRE(queue[3] == 3);

    queue.append(4);
    REQUIRE(queue.capacity() == 8);
    REQUIRE(queue.size() == 5);
    REQUIRE(queue.empty() == false);
    REQUIRE(queue[0] == 0);
    REQUIRE(queue[1] == 1);
    REQUIRE(queue[2] == 2);
    REQUIRE(queue[3] == 3);
    REQUIRE(queue[4] == 4);
}
