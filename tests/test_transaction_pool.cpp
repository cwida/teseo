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
    // Check whether memory pools can be reused once they get depleted

    // this test is useless if we're not in test mode, as the memory pools would be too big
    if(!StaticConfiguration::test_mode) return;

    Teseo teseo;
    vector<Transaction> transactions;
    transactions.push_back( teseo.start_transaction() );
    auto txpool1 = thread_context()->transaction_pool();
    for(uint64_t i = 1; i < 16; i ++){ // this should fill two memory pools
        transactions.push_back( teseo.start_transaction() );
    }

    // remove the first six transactions
    transactions.erase(transactions.begin(), transactions.begin() + 6);

    // next transaction should be created on the very first transaction pool, modulo GC
    this_thread::sleep_for(1s);
    transactions.push_back( teseo.start_transaction() );
    auto txpool2 = thread_context()->transaction_pool();

    REQUIRE(txpool1 == txpool2);
}
