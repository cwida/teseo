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

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "teseo.hpp"
#include "../src/context.hpp"

using namespace std;
using namespace teseo::internal::context;

#define COUT_DEBUG(msg) { std::scoped_lock lock(g_debugging_mutex); std::cout << msg << std::endl; }

TEST_CASE( "global_context_init" ) {
    GlobalContext instance;
    instance.dump();
}

TEST_CASE( "thread_context_init" ) {
    // Init 8 (+1, the main thread) thread context, check whether they can enter an epoch, mark an object for the GC, and deallocate safely

    GlobalContext instance;

    atomic<int64_t> sync_flag = 0; // > 0 => number of threads init, -1 => ask the threads to terminate
    condition_variable cvar;
    mutex cmutex;
    vector<thread> threads;
    for(uint64_t i = 0; i < 8; i++){
        threads.emplace_back([&]{
            REQUIRE_THROWS_AS(thread_context(), teseo::LogicalError); // no context registered

            // init
            instance.register_thread();
            thread_context()->epoch_enter();
            instance.gc()->mark(new int(i));

            // sync with the main thread
            sync_flag ++;
            cvar.notify_all();
            {
                unique_lock<mutex> lock(cmutex);
                cvar.wait(lock, [&]{ return sync_flag == -1; });
            }

            // resume execution
            instance.unregister_thread(); // done

            REQUIRE_THROWS_AS(thread_context(), teseo::LogicalError); // no context registered
        });
    }

    {
        unique_lock<mutex> lock(cmutex);
        cvar.wait(lock, [&]{ return sync_flag == 8; });
    }

    instance.dump();

    // resume execution
    sync_flag = -1;
    cvar.notify_all();

    for(auto& t: threads) t.join();

    instance.dump();
}
//
//TEST_CASE( "transaction_init" ){
//    GlobalContext instance;
//    ThreadContext* context = ThreadContext::context();
//    TransactionContext* txn1 = context->txn_start();
//    UndoEntryVertex* entry1 = txn1->create_undo_entry<UndoEntryVertex>(nullptr, UndoType::VERTEX_ADD, 42);
//    txn1->commit();
//
//    TransactionContext* txn2 = context->txn_start();
//    txn2->create_undo_entry<UndoEntryVertex>(entry1, UndoType::VERTEX_REMOVE, 42);
//    instance.dump();
//
//    txn2->commit();
//}
//


