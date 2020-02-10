
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

#include "../src/index.hpp"

using namespace teseo::internal;
using namespace std;

TEST_CASE( "Index", "sanity" ) {
    Index index;

    // Random permutation generated with Mathematica: `RandomSample[Range[10, 1000, 10]]'
    // It consists of all multiples of 10, starting from 10 up to 1000
    uint64_t randomPermutation[] = {340, 980, 860, 900, 190, 110, 410, 490, 440, 330, 20, 680, 210, 970,
      100, 240, 230, 140, 870, 350, 50, 270, 370, 60, 940, 620, 80, 700,
      610, 150, 30, 90, 70, 1000, 770, 550, 290, 600, 930, 180, 810, 120,
      310, 400, 920, 670, 220, 10, 470, 430, 820, 170, 840, 790, 560, 390,
      250, 420, 260, 480, 660, 520, 590, 730, 40, 690, 510, 990, 650, 500,
      280, 720, 450, 710, 160, 910, 540, 300, 380, 460, 880, 200, 580, 130,
      780, 800, 570, 530, 630, 830, 960, 640, 360, 850, 760, 890, 950, 750,
      740, 320};
    const size_t randomPermutation_sz = sizeof(randomPermutation) / sizeof(randomPermutation[0]);

    /**
     * Insert
     */
    for(size_t i = 0; i < 2/*randomPermutation_sz*/; i++){
        auto key = randomPermutation[i];
        std::cout << "Insert: " << key << ", " << key * 10 << std::endl;
        index.insert(key, 0, (void*) (key * 10));
        index.dump();
        return;
    }

//        REQUIRE(btree.size() == randomPermutation_sz);
//
//        /**
//         * Find
//         */
//        for(int i = 1; i < 1002; i++){
//            auto v = btree.find(i);
//            if(i % 10 == 0){ // all the existing keys are multiples of 10
//                REQUIRE(v == i * 10);
//            } else {
//                REQUIRE(v == -1);
//            }
//        }
//
//        /**
//         * Iteration
//         */
//        {
//            int i = 0;
//            int expected = 10;
//            auto it = btree.iterator();
//            while(it->hasNext()){
//                i++;
//                auto pair = it->next();
//                REQUIRE(pair.first == i * 10);
//                REQUIRE(pair.second == i * 100);
//            }
//            REQUIRE(i == randomPermutation_sz);
//        }
//
//        /**
//         * Range query
//         */
//        for(int i = 1; i <= 1001; i++){
//            for(int j = i; j <= 1002; j++){
//                auto it = btree.find(i, j);
//                int64_t min = -1, max = -1;
//                if(it->hasNext()) min = max = it->next().first;
//                while(it->hasNext()) max = it->next().first;
//
//                // check min
//                if(i < 10) {
//                    if(j < 10){
//                        REQUIRE(min == -1);
//                    } else {
//                        REQUIRE(min == 10);
//                    }
//                } else if (i > 1000){ // as the first case
//                    REQUIRE(min == -1);
//                } else {
//                    int upper_threshold = ceil(static_cast<double>(i) /10) * 10;
//                    if (i % 10 == 0){
//                        REQUIRE(min == i);
//                    } else if ( j >= upper_threshold ) {
//                        REQUIRE(min == upper_threshold);
//                    } else { // empty interval
//                        REQUIRE(min == -1);
//                    }
//                }
//
//                // check max
//                if(j < 10){ // 10 is the first key
//                    REQUIRE(max == -1);
//                } else if ( i > 1000 ){ // as above
//                    REQUIRE(max == -1);
//                } else {
//                    int lower_threshold = (j/10) * 10;
//                    if(i <= lower_threshold){
//                        REQUIRE(max == lower_threshold);
//                    } else { // empty interval
//                        REQUIRE(max == -1);
//                    }
//                }
//            }
//        }
//    }
}
