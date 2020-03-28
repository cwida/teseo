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

#include "../src/context.hpp"
#include "../src/memstore/sparse_array.hpp"
#include <iostream>

using namespace teseo::internal::context;
using namespace teseo::internal::memstore;
using namespace std;

/**
 * Create & destroy a sparse array. GlobalContext already initialises an internal sparse array.
 */
TEST_CASE("init") {
    GlobalContext instance;
    TransactionImpl* tx_impl = new TransactionImpl(shptr_thread_context(), instance.next_transaction_id());
    tx_impl->incr_user_count();

    tx_impl->decr_user_count();
    tx_impl = nullptr; // do not invoke delete
}
