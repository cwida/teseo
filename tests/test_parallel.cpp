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

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "teseo.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/util/thread.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;

/**
 * Create & destroy a sparse array with multiple threads around
 */
TEST_CASE("parallel_init", "[parallel]") {
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
    uint64_t try_num_threads[] = {1, 2, 4, 8, 16, 32, 64, 128};
    constexpr uint64_t try_num_threads_sz = sizeof(try_num_threads) / sizeof(try_num_threads[0]);

    for(uint64_t i = 0; i < try_num_threads_sz; i++){
        const uint64_t NUM_THREADS = try_num_threads[i];
        cout << "num threads: " << NUM_THREADS << endl;
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


/**
 * Check that the degree of vertices resolved by a read-only transaction is not
 * altered by concurrent updates from other transactions
 */
TEST_CASE("parallel_degree_ro", "[parallel][degree_parallel]"){
    Teseo teseo;
    const uint64_t num_concurrent_threads = 2;
    const uint64_t max_vertex_id = 1000;
    const uint64_t num_iterations = 10000;
    atomic<bool> done = false;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 10000 + vertex_id);
    }
    tx.commit();

    auto thread_main = [&](uint64_t start_vertex_id, uint64_t step){
        teseo.register_thread();
        while(!done){
            for(uint64_t vertex_id = start_vertex_id; vertex_id <= max_vertex_id; vertex_id += step){
                auto tx = teseo.start_transaction();
                if(tx.has_edge(10, vertex_id)){
                    tx.remove_edge(10, vertex_id);
                } else {
                    tx.insert_edge(10, vertex_id, 10000 + vertex_id);
                }
                tx.commit();
            }
        }
        teseo.unregister_thread();
    };

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    uint64_t expected_result = max_vertex_id / 10 - 1;

    vector<thread> threads;
    uint64_t step = 10 * num_concurrent_threads;
    uint64_t start_vertex_id = 20;
    for(uint64_t i = 0; i < num_concurrent_threads; i++){
        threads.emplace_back(thread_main, start_vertex_id, step);
        start_vertex_id += 10;
    }

    for(uint64_t i = 0; i < num_iterations; i++){
        REQUIRE(tx_ro.degree(10) == expected_result);
    }

    done = true;
    for(auto& t: threads) t.join();
}

/**
 * With read-write transactions is a bit more complex, let's make it simpler at the start with only
 * one segment, sparse file
 */
TEST_CASE("parallel_degree_rw1", "[parallel][degree_parallel]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t num_iterations = 10000;
    atomic<bool> done = false;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);

    thread concurrent_writer { [&](){
        teseo.register_thread();
        while(!done){
            for(uint64_t vertex_id = 20; vertex_id <= 40; vertex_id += 10){
                auto tx = teseo.start_transaction();
                if(tx.has_edge(10, vertex_id)){
                    tx.remove_edge(10, vertex_id);
                } else {
                    tx.insert_edge(10, vertex_id, 2000 + vertex_id);
                }
                tx.commit();
            }
        }
        teseo.unregister_thread();
    } };


    for(uint64_t i = 0 ; i < num_iterations; i++){
        REQUIRE(tx_rw.get_weight(10, 20) == 1020);
        REQUIRE(tx_rw.get_weight(10, 30) == 1030);
        REQUIRE(tx_rw.get_weight(10, 40) == 1040);
        REQUIRE(tx_rw.degree(10) == 3);
    }

    done = true;
    concurrent_writer.join();
}

/**
 * With read-write transactions is a bit more complex, let's make it simplre at the start with only
 * one segment, dense file
 */
TEST_CASE("parallel_degree_rw2", "[parallel][degree_parallel]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t max_verted_id = 100;
    const uint64_t num_iterations = 100000;
    atomic<bool> done = false;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();


    // it will implicitly convert into a dense file as there is not enough room to
    // store all the vertices & edge in the same segment
    //memstore->dump();

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);

    thread concurrent_writer { [&](){
        teseo.register_thread();
        while(!done){
            for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
                auto tx = teseo.start_transaction();
                if(tx.has_edge(10, vertex_id)){
                    tx.remove_edge(10, vertex_id);
                } else {
                    tx.insert_edge(10, vertex_id, 2000 + vertex_id);
                }
                tx.commit();
            }
        }
        teseo.unregister_thread();
    } };


    uint64_t expected_result = max_verted_id / 10 - 1;
    for(uint64_t i = 0 ; i < num_iterations; i++){
        REQUIRE(tx_rw.degree(10) == expected_result);
    }

    done = true;
    concurrent_writer.join();
}

/**
 * Two consecutive dense files, no rebalances
 */
TEST_CASE("parallel_degree_rw3", "[parallel][degree_parallel]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t max_verted_id = 100;
    const uint64_t num_iterations = 100000;
    atomic<bool> done = false;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();

    global_context()->runtime()->rebalance_first_leaf(memstore, 0);

    { // transform the first and second segments into dense files
        ScopedEpoch epoch;
        Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(1);
        Segment::to_dense_file(context);
    }

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);

    thread concurrent_writer { [&](){
        teseo.register_thread();
        while(!done){
            for(uint64_t vertex_id = 20; vertex_id <= max_verted_id; vertex_id += 10){
                auto tx = teseo.start_transaction();
                if(tx.has_edge(10, vertex_id)){
                    tx.remove_edge(10, vertex_id);
                } else {
                    tx.insert_edge(10, vertex_id, 2000 + vertex_id);
                }
                tx.commit();
            }
        }
        teseo.unregister_thread();
    } };


    uint64_t expected_result = max_verted_id / 10 - 1;
    for(uint64_t i = 0 ; i < num_iterations; i++){
        REQUIRE(tx_rw.degree(10) == expected_result);
    }

    done = true;
    concurrent_writer.join();
}

/**
 * Retrieve the degree, with 2 writers operating in the meanwhile and concurrent rebalances allowed
 */
TEST_CASE("parallel_degree_rw4", "[parallel][degree_parallel]"){
    Teseo teseo;
    const uint64_t num_concurrent_threads = 2;
    const uint64_t max_vertex_id = 1000;
    const uint64_t num_iterations = 10000;
    atomic<bool> done = false;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 10000 + vertex_id);
    }
    tx.commit();

    auto thread_main = [&](uint64_t start_vertex_id, uint64_t step){
        teseo.register_thread();
        while(!done){
            for(uint64_t vertex_id = start_vertex_id; vertex_id <= max_vertex_id; vertex_id += step){
                auto tx = teseo.start_transaction();
                if(tx.has_edge(10, vertex_id)){
                    tx.remove_edge(10, vertex_id);
                } else {
                    tx.insert_edge(10, vertex_id, 20000 + vertex_id);
                }
                tx.commit();
            }
        }
        teseo.unregister_thread();
    };

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);
    uint64_t expected_result = max_vertex_id / 10 - 1;

    vector<thread> threads;
    uint64_t step = 10 * num_concurrent_threads;
    uint64_t start_vertex_id = 20;
    for(uint64_t i = 0; i < num_concurrent_threads; i++){
        threads.emplace_back(thread_main, start_vertex_id, step);
        start_vertex_id += 10;
    }

    for(uint64_t i = 0; i < num_iterations; i++){
        REQUIRE(tx_rw.degree(10) == expected_result);
    }

    done = true;
    for(auto& t: threads) t.join();
}

/**
 * Retrieve the degree, with 8 writers operating in the meanwhile and concurrent rebalances allowed
 */
TEST_CASE("parallel_degree_rw5", "[parallel][degree_parallel]"){
    Teseo teseo;
    const uint64_t num_concurrent_threads = 8;
    const uint64_t max_vertex_id = 1000;
    const uint64_t num_iterations = 10000;
    atomic<bool> done = false;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 10000 + vertex_id);
    }
    tx.commit();

    auto thread_main = [&](uint64_t start_vertex_id, uint64_t step){
        teseo.register_thread();
        while(!done){
            for(uint64_t vertex_id = start_vertex_id; vertex_id <= max_vertex_id; vertex_id += step){
                auto tx = teseo.start_transaction();
                if(tx.has_edge(10, vertex_id)){
                    tx.remove_edge(10, vertex_id);
                } else {
                    tx.insert_edge(10, vertex_id, 20000 + vertex_id);
                }
                tx.commit();
            }
        }
        teseo.unregister_thread();
    };

    auto tx_ro = teseo.start_transaction(/* read only ? */ true);
    auto tx_rw = teseo.start_transaction(/* read only ? */ false);

    uint64_t expected_result = max_vertex_id / 10 - 1;

    vector<thread> threads;
    uint64_t step = 10 * num_concurrent_threads;
    uint64_t start_vertex_id = 20;
    for(uint64_t i = 0; i < num_concurrent_threads; i++){
        threads.emplace_back(thread_main, start_vertex_id, step);
        start_vertex_id += 10;
    }

    for(uint64_t i = 0; i < num_iterations; i++){
        REQUIRE(tx_ro.degree(10) == expected_result);
        REQUIRE(tx_rw.degree(10) == expected_result);
    }

    done = true;
    for(auto& t: threads) t.join();
}
