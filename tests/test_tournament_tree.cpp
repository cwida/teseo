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

#include <cstdint>
#include <iostream>

#include "../src/util/permutation.hpp"
#include "../src/util/timer.hpp"
#include "../src/util/tournament_tree.hpp"

using namespace std;
using namespace teseo::internal::util;

struct Queue {
    uint64_t* m_queue;
    uint64_t m_size;
};

/**
 * Basic test case, check whether the tournament tree can be initialised, built, operated safely
 * with #pop_and_unset and destroyed safely.
 */
TEST_CASE("sanity") {
    const uint64_t capacity = 32;
    TournamentTree<uint64_t, void*> tree ( capacity, 3 );

    for(uint64_t i = 0; i < capacity; i++ ){
        tree.set(i, i, (void*) i);
    }
    tree.rebuild();

    uint64_t expected_value = 0;
    while(!tree.done()){
        std::pair<uint64_t, void*> item = tree.top();
        tree.pop_and_unset();


        REQUIRE(item.first == expected_value);
        REQUIRE(item.second == (void*) expected_value);

        expected_value++;
    }

    REQUIRE(expected_value == capacity); // have we extracted all values from the tournament tree?
}


/**
 * Like sanity, but it's tests #rebuild and #pop_and_unset in a more brute force way for many capacity and fan outs
 */
TEST_CASE("pop_and_unset") {
    constexpr uint64_t fanouts[] = {2, 3, 4, 5, 11, 13, 63, 64, 65, 128, 256};

    for(auto fanout : fanouts){
        cout << "pop_and_unset, fanout: " << fanout << "\n";
        for(uint64_t capacity = 7; capacity < 1000; capacity += 7){

            TournamentTree<uint64_t, void*, less<uint64_t>, 20> tree ( capacity, fanout );
            for(uint64_t i = 0; i < capacity; i++ ){
                tree.set(i, i, (void*) i);
            }
            tree.rebuild();

            uint64_t expected_value = 0;
            while(!tree.done()){

                std::pair<uint64_t, void*> item = tree.top();
                tree.pop_and_unset();

                REQUIRE(item.first == expected_value);
                REQUIRE(item.second == (void*) expected_value);

                expected_value++;
            }

            REQUIRE(expected_value == capacity); // have we extracted all values from the tournament tree?
        }
    }
}


/**
 * Create a set of artificial queues to test the usage of #pop_and_replace
 */
TEST_CASE("pop_and_replace"){

    // Create a random permutation of the keys
    constexpr uint64_t num_keys = 1ull << 20;
    unique_ptr<uint64_t[]> ptr_keys = random_permutation(num_keys, 42);

    // Create the queues
    constexpr uint64_t num_queues = 1ull << 10; // 1024 queues
    vector<Queue> queues;
    {
        uint64_t elements_per_queue = num_keys / num_queues;
        uint64_t odd_queues = num_keys % num_queues;
        uint64_t keys_offset = 0;

        for(uint64_t i = 0; i < num_queues; i++){
            uint64_t queue_sz = elements_per_queue + (i < odd_queues);
            Queue q;
            q.m_queue = ptr_keys.get() + keys_offset;
            q.m_size = queue_sz;
            std::sort(q.m_queue, q.m_queue + q.m_size);
            queues.push_back(q);

            keys_offset += queue_sz;
        }
    }

    // Init the tournament tree
    TournamentTree<uint64_t, Queue*> tree(num_queues, 3);
    for(uint64_t i = 0; i < num_queues; i++){
        if(queues[i].m_size > 0){
            tree.set(i, queues[i].m_queue[0], &queues[i]);
            queues[i].m_queue++;
            queues[i].m_size--;
        }
    }
    tree.rebuild();

//    tree.dump();

    // Run the extraction
    uint64_t expected_key = 0;
    while(!tree.done()){
//        cout << "\nexpected key: " << expected_key << endl;

        auto pair = tree.top();
        REQUIRE(expected_key == pair.first);

        uint64_t queue_id = pair.second - queues.data();
        if(queues[queue_id].m_size > 0){
            tree.pop_and_replace(queues[queue_id].m_queue[0]);
            queues[queue_id].m_queue++;
            queues[queue_id].m_size--;
        } else {
            tree.pop_and_unset(); // queue exhausted
        }

//        tree.dump();
        expected_key++; // next iteration
    }

    REQUIRE(expected_key == num_keys);// did we extract all values from the tournament tree?
}

TEST_CASE("benchmark"){

    // Create a random permutation of the keys
    constexpr uint64_t num_keys = 1ull<<20;
    constexpr uint64_t num_queues = 1024;
    constexpr uint64_t num_repetitions = 5;
    // the fanout to benchmark
    constexpr uint64_t fanouts[] = {2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };

    cout << "benchmark, num keys: " << num_keys << ", num queues: " << num_queues << ", repetitions: " << num_repetitions << "\n";

    // Sort the values
    unique_ptr<uint64_t[]> ptr_keys = random_permutation(num_keys, 42);
    vector<Queue> queues;
    {
        uint64_t elements_per_queue = num_keys / num_queues;
        uint64_t odd_queues = num_keys % num_queues;
        uint64_t keys_offset = 0;

        for(uint64_t i = 0; i < num_queues; i++){
            uint64_t* data = ptr_keys.get() + keys_offset;
            uint64_t queue_sz = elements_per_queue + (i < odd_queues);
            std::sort(data, data + queue_sz);

            keys_offset += queue_sz;
        }
    }

    constexpr uint64_t num_fanaouts = sizeof(fanouts) / sizeof(fanouts[0]);
    unique_ptr<uint64_t[]> results { new uint64_t[num_fanaouts * num_repetitions]() };
    for(uint64_t r = 0; r < num_repetitions; r++){
        cout << "benchmark, execution " << (r+1) << "/" << num_repetitions << " ..." << endl;
        for(uint64_t f = 0; f < num_fanaouts; f++){
            uint64_t fanout = fanouts[f];

            // Create the queues
            vector<Queue> queues;
            {
                uint64_t elements_per_queue = num_keys / num_queues;
                uint64_t odd_queues = num_keys % num_queues;
                uint64_t keys_offset = 0;

                for(uint64_t i = 0; i < num_queues; i++){
                    uint64_t queue_sz = elements_per_queue + (i < odd_queues);
                    Queue q;
                    q.m_queue = ptr_keys.get() + keys_offset;
                    q.m_size = queue_sz;
                    queues.push_back(q);

                    keys_offset += queue_sz;
                }
            }

            // Start the execution
            Timer timer;
            timer.start();

            // Init the tournament tree
            TournamentTree<uint64_t, Queue*, less<uint64_t>, 20> tree(num_queues, fanout);
            for(uint64_t i = 0; i < num_queues; i++){
                if(queues[i].m_size > 0){
                    tree.set(i, queues[i].m_queue[0], &queues[i]);
                    queues[i].m_queue++;
                    queues[i].m_size--;
                }
            }
            tree.rebuild();

            uint64_t expected_key = 0;
            while(!tree.done()){
                auto pair = tree.top();

                uint64_t queue_id = pair.second - queues.data();
                if(queues[queue_id].m_size > 0){
                    tree.pop_and_replace(queues[queue_id].m_queue[0]);
                    queues[queue_id].m_queue++;
                    queues[queue_id].m_size--;
                } else {
                    tree.pop_and_unset(); // queue exhausted
                }
                expected_key++; // next iteration
            }


            timer.stop();

            results[r + f * num_repetitions] = timer.microseconds();

            REQUIRE(expected_key == num_keys);// did we extract all values from the tournament tree?
        }
    }

    cout << "\nbenchmark, results:\n";
    const char* time_unit = "microsecs";
    for(uint64_t f = 0; f < num_fanaouts; f++){
        uint64_t* __restrict data = results.get() + f * num_repetitions;
        std::sort(data, data + num_repetitions);
        uint64_t min = data[0];
        uint64_t median = (num_repetitions % 2 == 0) ? ((data[num_repetitions/2] + data[(num_repetitions /2) +1])/2) : data[num_repetitions/2];
        uint64_t max = data[num_repetitions -1];

        cout << "benchmark, fanout: " << fanouts[f] << ", ";
        cout << "median: " << median << " " << time_unit << ", ";
        cout << "min: " << min << " " << time_unit << ", ";
        cout << "max: " << max << " " << time_unit << "\n";
    }
}




