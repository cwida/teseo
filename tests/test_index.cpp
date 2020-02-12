
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

#include <thread>
#include <vector>

#include "test_index_data.hpp"
#include "../src/context.hpp"
#include "../src/index.hpp"

using namespace teseo::internal;
using namespace std;

#define COUT_DEBUG(msg) { std::scoped_lock lock(g_debugging_mutex); std::cout << msg << std::endl; }

TEST_CASE("sorted") {
    // We need to initialise a context instance to start the Garbage Collector
    GlobalContext instance;
    ScopedEpoch epoch; // Epoch for the GC

    Index index;
    constexpr uint64_t KEY_MAX = 1020;

    /**
     * Insert
     */
    uint64_t num_keys = 0;
    for(uint64_t key = 10; key <= KEY_MAX; key += 10){
//        COUT_DEBUG("Insert: " << key << ", " << key * 10);
        index.insert(key, 0, (void*) (key * 10));
//        index.dump();

        for(uint64_t i = 10; i <= key; i += 10){
            for(uint64_t j = i -1; j <= i +1; j++){ // j = 9, 10, 11, 19, 20, 21, so on
                uint64_t value = reinterpret_cast<uint64_t>(index.find(j));
//                COUT_DEBUG("Find: " << j << ", value: " << value);
                if( j < 10 ){
                    REQUIRE(value == 0); // key not found
                } else { // min or equal value less than j
                    auto expected_key = (j / 10) * 100; // 10, ..., 19 => 100, 20, ..., 29 => 200, so on
                    REQUIRE(value == expected_key);
                }
            }
        }

        num_keys++;
        REQUIRE(index.size() == num_keys);
    }

    /**
     * Remove
     */
    for(uint64_t key = 10; key <= KEY_MAX; key += 10){
        index.remove(key, 0);
//        index.dump();

        for(uint64_t i = 10; i <= KEY_MAX; i += 10){
            for(uint64_t j = i -1; j <= i +1; j++){ // j = 9, 10, 11, 19, 20, 21, so on
                uint64_t value = reinterpret_cast<uint64_t>(index.find(j));
//                COUT_DEBUG("Find: " << j << ", value: " << value);
                if( (j < (key +10)) || key == KEY_MAX ){
                    REQUIRE(value == 0); // key not found
                } else { // min or equal value less than j
                    auto expected_key = (j / 10) * 100; // 10, ..., 19 => 100, 20, ..., 29 => 200, so on
                    REQUIRE(value == expected_key);
                }
            }
        }

        num_keys--;
        REQUIRE(index.size() == num_keys);
    }


    REQUIRE(index.empty() == true);
}

TEST_CASE("random1"){ // random permutation, insert only
    // We need to initialise an Database instance to start the Garbage Collector
    GlobalContext instance;
    ScopedEpoch epoch; // Epoch for the GC

    Index index;

    /**
     * Insert
     */
    for(size_t i = 0; i < g_randomPermutation1_sz; i++){
        auto key = g_randomPermutation1[i];
        index.insert(key, 0, (void*) (key * 10));
//        index.dump();
    }

    /**
     * Find
     */
    for(uint64_t i = 1; i < 1002; i++){
        uint64_t value = reinterpret_cast<uint64_t>(index.find(i));
//        COUT_DEBUG( "Find: " << i << ", value: " << value);
        if( i < 10 ){
            REQUIRE(value == 0); // key not found
        } else { // min or equal value less than i
            auto expected_key = (i / 10) * 100; // 10, ..., 19 => 100, 20, ..., 29 => 200, so on
            REQUIRE(value == expected_key);
        }
    }
}

TEST_CASE("random2"){ // random permutation (bigger sample), insert & remove
    // We need to initialise an Database instance to start the Garbage Collector
    GlobalContext instance;
    ScopedEpoch epoch; // Epoch for the GC

    Index index;


    /**
     * Insert
     */
    for(uint64_t i = 0; i < g_randomPermutation2_sz; i++){
        auto key = g_randomPermutation2[i];
        index.insert(key, 0, (void*) (key * 10));
//        index.dump();
    }

    REQUIRE(index.size() == g_randomPermutation2_sz);

    /**
     * Find
     */
    for(uint64_t i = 1; i < 100002; i++){
        uint64_t value = reinterpret_cast<uint64_t>(index.find(i));
//        COUT_DEBUG( "Find: " << i << ", value: " << value);
        if( i < 10 ){
            REQUIRE(value == 0); // key not found
        } else { // min or equal value less than i
            auto expected_key = (i / 10) * 100; // 10, ..., 19 => 100, 20, ..., 29 => 200, so on
            REQUIRE(value == expected_key);
        }
    }


    /**
     * Remove
     */
    for(uint64_t i = 0; i < g_randomPermutation2_sz; i++){
        index.remove(g_randomPermutation2[i], 0);
        uint64_t expected_size = g_randomPermutation2_sz - i -1;
        REQUIRE(index.size() == expected_size);
    }
    for(uint64_t i = 1; i < 100002; i++){
        uint64_t value = reinterpret_cast<uint64_t>(index.find(i));
        REQUIRE(value == 0); // key not found
    }

}

TEST_CASE("random2_par"){ // random permutation (bigger sample), parallel execution
    // We need to initialise an Database instance to start the Garbage Collector
    GlobalContext instance;
    Index index;

    const uint64_t num_threads = 8;
    const uint64_t items_per_thread = g_randomPermutation2_sz / num_threads;
    const uint64_t odd_threads = g_randomPermutation2_sz % num_threads;
    vector<thread> threads;
    int64_t partition_start = 0;
    for(int64_t i = 0; i < num_threads; i++){
        int64_t partition_end = partition_start + items_per_thread + (i < odd_threads);
        threads.emplace_back([&instance, &index](int64_t start, int64_t end){
            instance.register_thread();
            for(int64_t j = start; j < end; j++){
                ScopedEpoch epoch;
                auto key = g_randomPermutation2[j];
                index.insert(key, 0, (void*) (key * 10));
            }
            instance.unregister_thread();
        }, partition_start, partition_end);
        partition_start = partition_end;
    }
    for(auto& t: threads) { t.join(); } threads.clear();
    REQUIRE(index.size() == g_randomPermutation2_sz);

    /**
     * Find
     */
    {
        ScopedEpoch epoch; // Epoch for the GC
        for(uint64_t i = 1; i < 100002; i++){
            uint64_t value = reinterpret_cast<uint64_t>(index.find(i));
    //        COUT_DEBUG( "Find: " << i << ", value: " << value);
            if( i < 10 ){
                REQUIRE(value == 0); // key not found
            } else { // min or equal value less than i
                auto expected_key = (i / 10) * 100; // 10, ..., 19 => 100, 20, ..., 29 => 200, so on
                REQUIRE(value == expected_key);
            }
        }
    }

    /**
     * Remove
     */
    threads.clear(); partition_start = 0;
    for(int64_t t = 0; t < num_threads; t++){
        int64_t partition_end = partition_start + items_per_thread + (t < odd_threads);
        threads.emplace_back([&instance, &index](int64_t start, int64_t end){
            instance.register_thread();
            for(int64_t i = end -1; i >= start; i--){

                {
                    ScopedEpoch epoch; // Protect from the GC
                    auto key = g_randomPermutation2[i];
                    index.remove(key, 0);
                }

                // check that all keys are still in place
                for(int64_t j = start; j < end; j++){
                    ScopedEpoch epoch; // Protect from the GC
                    auto search_key = g_randomPermutation2[j];
                    int64_t value = reinterpret_cast<int64_t>(index.find(search_key));
                    if(j < i){ // those keys have not been removed yet
                        REQUIRE(value == (search_key * 10));
                    } else {
                        // these keys have been removed. We don't know what's the previous key in the sorted order,
                        // but the value retrieved must be different from the one built from the key
                        REQUIRE(value != ((search_key) * 10));
                    }
                }
            }
            instance.unregister_thread();
        }, partition_start, partition_end);
        partition_start = partition_end;
    }
    for(auto& t: threads) { t.join(); } threads.clear();
    REQUIRE(index.empty());

    ScopedEpoch epoch; // epoch for the GC
    for(uint64_t i = 1; i < 100002; i++){
        uint64_t value = reinterpret_cast<uint64_t>(index.find(i));
        REQUIRE(value == 0); // key not found
    }

}
