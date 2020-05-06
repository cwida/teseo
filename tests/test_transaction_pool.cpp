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
#include <mutex>
#include <thread>
#include <vector>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/memory_pool.hpp"
#include "teseo/transaction/transaction_impl.hpp"

#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::transaction;


TEST_CASE("txn_mempool_create", "[transaction]"){
    // Create more transactions that can be held by a single memory pool, to check where multiple memory pool are created as well

    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;

    Teseo teseo;
    vector<Transaction> transactions;
    for(uint64_t i = 0; i < 20; i ++){
        transactions.push_back( teseo.start_transaction() );
    }
}

TEST_CASE("txn_mempool_reuse1", "[transaction]"){
    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;

    Teseo teseo;
    vector<Transaction> transactions;

    transactions.push_back( teseo.start_transaction() );
    auto txpool1 = thread_context()->transaction_pool();
    for(uint64_t i = 1; i < txpool1->capacity(); i ++){ // this should fill two memory pools
        transactions.push_back( teseo.start_transaction() );
    }

    REQUIRE(txpool1->is_full() == true);

    auto tx_addr1 = transactions.back().handle_impl();
    transactions.pop_back();
    this_thread::sleep_for(context::StaticConfiguration::runtime_gc_frequency *2);

    REQUIRE(txpool1->is_full() == true);
    auto ptr_scratchpad = make_unique<uint32_t[]>( context::StaticConfiguration::transaction_memory_pool_size );
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_full() == false);

    auto tx_new = teseo.start_transaction();
    auto tx_addr2 = tx_new.handle_impl();

    REQUIRE(tx_addr1 == tx_addr2);
}

TEST_CASE("txn_mempool_reuse2", "[transaction]"){
    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;
    REQUIRE(StaticConfiguration::transaction_memory_pool_list_cache_size == 8);

    Teseo teseo;
    vector<Transaction> transactions;
    vector<void*> handles;


    for(uint64_t i = 0; i < StaticConfiguration::transaction_memory_pool_list_cache_size; i ++){ // this should fill two memory pools
        auto tx = teseo.start_transaction();
        transactions.push_back( tx );
        handles.push_back( tx.handle_impl() );
    }
    auto txpool1 = thread_context()->transaction_pool();

    // The memory pool now assigns the slots in reverse order, that is the first allocation will be slot #7, the
    // second one will be slot #6, and so on...
    std::reverse(transactions.begin(), transactions.end());
    std::reverse(handles.begin(), handles.end());

    REQUIRE(txpool1->is_full() == true);

    transactions.erase(transactions.begin() +7); // h: 7
    transactions.erase(transactions.begin() +5); // h: 5
    transactions.erase(transactions.begin() +1); // h: 1

    this_thread::sleep_for(context::StaticConfiguration::runtime_gc_frequency *2);

    REQUIRE(txpool1->is_full() == true);
    auto ptr_scratchpad = make_unique<uint32_t[]>( context::StaticConfiguration::transaction_memory_pool_size );
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_full() == false);

    // slots are now assigned in reverse order ...
    auto tx7 = teseo.start_transaction();
    REQUIRE(tx7.handle_impl() == handles[7]);
    auto tx5 = teseo.start_transaction();
    REQUIRE(tx5.handle_impl() == handles[5]);
    auto tx1 = teseo.start_transaction();
    REQUIRE(tx1.handle_impl() == handles[1]);
}

// Test multiple invocations of #rebuild_free_list
TEST_CASE("txn_mempool_reuse3", "[transaction]"){
    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;
    REQUIRE(StaticConfiguration::transaction_memory_pool_list_cache_size == 8);

    Teseo teseo;
    vector<Transaction> transactions;
    vector<void*> handles;

    for(uint64_t i = 0; i < StaticConfiguration::transaction_memory_pool_list_cache_size; i ++){ // this should fill two memory pools
        auto tx = teseo.start_transaction();
        transactions.push_back( tx );
        handles.push_back( tx.handle_impl() );
    }
    auto txpool1 = thread_context()->transaction_pool();

    // The memory pool now assigns the slots in reverse order, that is the first allocation will be slot #7, the
    // second one will be slot #6, and so on...
    std::reverse(transactions.begin(), transactions.end());
    std::reverse(handles.begin(), handles.end());

    REQUIRE(txpool1->is_full() == true);

    transactions.erase(transactions.begin() +7); // h: 7
    transactions.erase(transactions.begin() +5); // h: 5
    transactions.erase(transactions.begin() +1); // h: 1

    // First invocation
    this_thread::sleep_for(context::StaticConfiguration::runtime_gc_frequency *2);
    REQUIRE(txpool1->is_full() == true);
    auto ptr_scratchpad = make_unique<uint32_t[]>( context::StaticConfiguration::transaction_memory_pool_size );
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_full() == false);

    // Second invocation
    // handles alive: [0, 2, 3, 4, 6]
    transactions.erase(transactions.begin() +3); // h: 4
    transactions.erase(transactions.begin() +0); // h: 0
    this_thread::sleep_for(context::StaticConfiguration::runtime_gc_frequency *2);
    txpool1->rebuild_free_list(ptr_scratchpad.get());

    // slots are now assigned in reverse order ...
    auto tx7 = teseo.start_transaction();
    REQUIRE(tx7.handle_impl() == handles[7]);
    auto tx5 = teseo.start_transaction();
    REQUIRE(tx5.handle_impl() == handles[5]);
    auto tx4 = teseo.start_transaction();
    REQUIRE(tx4.handle_impl() == handles[4]);
    auto tx1 = teseo.start_transaction();
    REQUIRE(tx1.handle_impl() == handles[1]);
    auto tx0 = teseo.start_transaction();
    REQUIRE(tx0.handle_impl() == handles[0]);
}

// Test a nop invocation of #rebuild_free_list(), with the memory pool completely filled
TEST_CASE("txn_mempool_reuse4", "[transaction]"){
    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;
    REQUIRE(StaticConfiguration::transaction_memory_pool_list_cache_size == 8);

    Teseo teseo;
    vector<Transaction> transactions;

    for(uint64_t i = 0; i < StaticConfiguration::transaction_memory_pool_list_cache_size; i ++){ // this should fill two memory pools
        transactions.push_back( teseo.start_transaction() );
    }
    auto txpool1 = thread_context()->transaction_pool();


    REQUIRE(txpool1->is_full() == true);
    auto ptr_scratchpad = make_unique<uint32_t[]>( context::StaticConfiguration::transaction_memory_pool_size );
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_full() == true);
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_full() == true);
    REQUIRE(txpool1->fill_factor() == 1);
}

// Test a nop invocation of #rebuild_free_list(), with the memory pool completely empty
TEST_CASE("txn_mempool_reuse5", "[transaction]"){
    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;
    REQUIRE(StaticConfiguration::transaction_memory_pool_list_cache_size == 8);

    Teseo teseo;
    {
        // create & destroy a transaction
        teseo.start_transaction();
    }
    auto txpool1 = thread_context()->transaction_pool();

    this_thread::sleep_for(context::StaticConfiguration::runtime_gc_frequency *2);

    REQUIRE(txpool1->is_empty() == false);
    auto ptr_scratchpad = make_unique<uint32_t[]>( context::StaticConfiguration::transaction_memory_pool_size );
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_empty() == true);
    txpool1->rebuild_free_list(ptr_scratchpad.get());
    REQUIRE(txpool1->is_empty() == true);
    REQUIRE(txpool1->fill_factor() == 0);
}
