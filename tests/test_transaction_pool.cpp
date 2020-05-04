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

TEST_CASE("txn_mempool_reuse", "[transaction]"){
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
    txpool1->rebuild_free_list();
    REQUIRE(txpool1->is_full() == false);

    auto tx_new = teseo.start_transaction();
    auto tx_addr2 = tx_new.handle_impl();

    REQUIRE(tx_addr1 == tx_addr2);
}
