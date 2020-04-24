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

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "../src/context.hpp"
#include "teseo.hpp"
#include "../src/memstore-old/sparse_array.hpp"

using namespace teseo;
using namespace teseo::internal::context;
using namespace teseo::internal::memstore;
using namespace std;


/**
 * Create & destroy a sparse array with multiple threads around
 */
TEST_CASE("parallel_init", "[parallel]") {
    g_debugging_test = true;
    uint64_t try_num_threads[] = {1, 2, 4, 8, 16, 32, 64, 128};
    constexpr uint64_t try_num_threads_sz = sizeof(try_num_threads) / sizeof(try_num_threads[0]);

    for(uint64_t i = 0; i < try_num_threads_sz; i++){
       const uint64_t NUM_THREADS = try_num_threads[i];
       Teseo teseo;


       std::mutex mutex_;
       condition_variable condvar;
       int64_t threads_done = NUM_THREADS;
       bool threads_resume = false;

       auto thread_main = [&](){
           teseo.register_thread();
           auto tx = teseo.start_transaction();

           {
               unique_lock<mutex> lock(mutex_);
               threads_done --;
               condvar.notify_all();
               condvar.wait(lock, [&](){ return !threads_resume; }); // while (!pred()) wait(lck);
           }

           teseo.unregister_thread();
       };

       vector<thread> threads;
       for(uint64_t i = 0; i < NUM_THREADS; i++){
           threads.emplace_back(thread_main);
       }

       {
           unique_lock<mutex> lock(mutex_);
           condvar.wait(lock, [&](){ return threads_done == 0; }); // while (!pred()) wait(lck);
           threads_resume = true;
       }
       condvar.notify_all();

       for(auto& t: threads) t.join();
    }

    // done
}

/**
 * Create & destroy several items with multiple threads around
 */
TEST_CASE("parallel_rw1", "[parallel]") { // Readers & writers
    g_debugging_test = true;
    uint64_t try_num_threads[] = {1, 2, 4, 8, 16, 32, 64, 128};
    constexpr uint64_t try_num_threads_sz = sizeof(try_num_threads) / sizeof(try_num_threads[0]);

    for(uint64_t i = 0; i < try_num_threads_sz; i++){
       const uint64_t NUM_THREADS = try_num_threads[i];
       Teseo teseo;

       {
           auto tx = teseo.start_transaction();
           tx.insert_vertex(10);
           REQUIRE(tx.num_vertices() == 1);
           REQUIRE(tx.num_edges() == 0);
           tx.commit();
       }

       auto thread_main = [&](uint64_t vertex_id){
           teseo.register_thread();
           {
               auto tx = teseo.start_transaction();
               tx.insert_vertex(vertex_id);
               tx.commit();
           }

           bool previous_result = false;
           double previous_weight = 0;
           for(uint64_t k = 0; k < 1024; k++){
               auto tx = teseo.start_transaction();
               REQUIRE(tx.has_vertex(10));
               REQUIRE(tx.has_vertex(vertex_id));

               REQUIRE(tx.has_edge(10, vertex_id) == previous_result);
               REQUIRE(tx.has_edge(vertex_id, 10) == previous_result);
               if(previous_result){
                   REQUIRE(tx.get_weight(10, vertex_id) == previous_weight);
                   REQUIRE(tx.get_weight(vertex_id, 10) == previous_weight);
               } else {
                   // sometimes catch, the test framework, crashes here, we'll use a workaround
                   //REQUIRE_THROWS_AS(tx.get_weight(10, vertex_id), LogicalError);
                   //REQUIRE_THROWS_AS(tx.get_weight(vertex_id, 10), LogicalError);
                   bool passed = false;
                   try { tx.get_weight(10, vertex_id); } catch(LogicalError& e){ passed = true; }
                   REQUIRE(passed == true); // tx.get_weight(10, vertex_id) => LogicalError
                   passed = false;
                   try { tx.get_weight(vertex_id, 10); } catch(LogicalError& e){ passed = true; }
                   REQUIRE(passed == true); // tx.get_weight(vertex_id, 10) => LogicalError
               }

               if(!previous_result){ // the edge does not exist
                   //REQUIRE_NOTHROW(tx.insert_edge(vertex_id, 10, ++previous_weight)); // catch2 crash
                   tx.insert_edge(vertex_id, 10, ++previous_weight);
               } else {
                   //REQUIRE_NOTHROW(tx.remove_edge(10, vertex_id)); // catch2 crash
                   tx.remove_edge(10, vertex_id);
               }

               previous_result = !previous_result;

               tx.commit();
           }

           teseo.unregister_thread();
       };

       vector<thread> threads;
       for(uint64_t i = 0; i < NUM_THREADS; i++){
           threads.emplace_back(thread_main, 20 + i * 10); // 20, 30, ...
       }

       for(auto& t: threads) t.join();
    }

    // done
}
