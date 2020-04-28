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

#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo::context;
using namespace teseo::transaction;

#define COUT_DEBUG(msg) { std::scoped_lock lock(g_debugging_mutex); std::cout << msg << std::endl; }

TEST_CASE( "contex_global_init", "[context]" ) {
    GlobalContext instance;
    instance.dump();
}


TEST_CASE( "context_thread_init", "[context]" ) {
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

    //instance.dump();

    // resume execution
    sync_flag = -1;
    cvar.notify_all();

    for(auto& t: threads) t.join();

    //instance.dump();
}

TEST_CASE( "context_transaction_list", "[context]" ){
    GlobalContext instance;

    { // Init, at least one item in the list is present
        ScopedEpoch e;
        TransactionSequence* seq = instance.active_transactions();
        REQUIRE(seq->size() == 1);
        REQUIRE((*seq)[0] <= 0 );
        delete(seq);
    }

    {
        TransactionImpl* tx1_impl = ThreadContext::create_transaction();
        tx1_impl->incr_user_count();
        TransactionImpl* tx2_impl = ThreadContext::create_transaction();
        tx2_impl->incr_user_count();

        REQUIRE(tx2_impl->ts_read() > tx1_impl->ts_read());

        ScopedEpoch e;
        TransactionSequence* seq = instance.active_transactions();
        REQUIRE(seq->size() == 3);
        REQUIRE((*seq)[0] == 2 ); /* transaction id for the next upcoming transaction, not yet present */
        REQUIRE((*seq)[1] == tx2_impl->ts_read() );
        REQUIRE((*seq)[2] == tx1_impl->ts_read() );
        delete(seq);

        tx1_impl->commit();

        seq = instance.active_transactions();
        REQUIRE(seq->size() == 2);
        REQUIRE((*seq)[0] == 3 ); /* transaction id for the next upcoming transaction, not yet present */
        REQUIRE((*seq)[1] == tx2_impl->ts_read() );
        delete seq;

        uint64_t max_transaction_id = tx2_impl->ts_read();
        tx2_impl->commit();
        seq = instance.active_transactions();
        REQUIRE(seq->size() == 1);
        REQUIRE((*seq)[0] > max_transaction_id );
        delete seq;

        tx1_impl->decr_user_count();
        tx2_impl->decr_user_count();
    }

    //uint64_t seq_num_threads[] = {2, 4, 8, 16, 32, 64, 128}; // with valgrind
    uint64_t seq_num_threads[] = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}; // without valgrind
    uint64_t seq_num_threads_sz = sizeof(seq_num_threads) / sizeof(seq_num_threads[0]);
    for(uint64_t i = 0; i < seq_num_threads_sz; i++){
        int64_t NUM_THREADS = seq_num_threads[i];
        bool thread_condition1 = false;
        bool thread_condition2 = false;
        int64_t active_threads = 0;
        std::mutex t_mutex;
        std::condition_variable t_condvar;
        std::vector<TransactionImpl*> transactions;

        auto worker = [&](){
            instance.register_thread();

            TransactionImpl* tx1_impl = ThreadContext::create_transaction();
            tx1_impl->incr_user_count();
            this_thread::sleep_for(100ms);
            TransactionImpl* tx2_impl = ThreadContext::create_transaction();
            tx2_impl->incr_user_count();

            // register in the vector `transactions' both tx1 and tx2
            {
                unique_lock<mutex> lock(t_mutex);
                transactions.push_back(tx1_impl);
                transactions.push_back(tx2_impl);

                if(--active_threads == 0) t_condvar.notify_all();
                t_condvar.wait(lock, [&](){ return thread_condition1; }); // until the pred is false wait
            }


            // commit tx2
            tx2_impl->commit();
            {
                unique_lock<mutex> lock(t_mutex);
                if(--active_threads == 0) t_condvar.notify_all();
                t_condvar.wait(lock, [&](){ return thread_condition2; }); // until the pred is false wait
            }


            tx2_impl->decr_user_count();
            tx1_impl->decr_user_count();
            instance.unregister_thread();
        };

        vector<thread> threads;
        active_threads = NUM_THREADS;
        for(int64_t i = 0; i < NUM_THREADS; i++){
            threads.emplace_back(worker);
        }

        // first check, all transaction should appear
        uint64_t max_transaction_id = 0;
        {
            unique_lock<mutex> lock(t_mutex);
            t_condvar.wait(lock, [&](){ return active_threads == 0; });
            sort(transactions.begin(), transactions.end(), [](const TransactionImpl* t1, const TransactionImpl* t2){
                return t1->ts_read() > t2->ts_read();
            });

            ScopedEpoch epoch;
            unique_ptr<TransactionSequence> seq { instance.active_transactions() };
            REQUIRE(seq->size() == NUM_THREADS * 2 +1);
            for(uint64_t i = 1; i < seq->size(); i++){
                REQUIRE((*seq)[i] == transactions[i -1]->ts_read());
            }
            REQUIRE( (*seq)[1] == (*seq)[0] -1 ); // seq[0] is the tx id for the next upcoming transaction
            max_transaction_id = (*seq)[0];
        }

        active_threads = NUM_THREADS;
        thread_condition1 = true;
        t_condvar.notify_all();

        // second check, only the non terminated transactions should appear
        {
            unique_lock<mutex> lock(t_mutex);
            t_condvar.wait(lock, [&](){ return active_threads == 0; });

            ScopedEpoch epoch;
            unique_ptr<TransactionSequence> seq { instance.active_transactions() };
            REQUIRE(seq->size() == NUM_THREADS +1); // +1 because it contains the TX for the next upcoming transaction
            for(uint64_t i = 1, j = 0; i < seq->size(); i++, j++){
                while(transactions[j]->is_terminated()) j++;
                REQUIRE((*seq)[i] == transactions[j]->ts_read());
            }
        }

        active_threads = NUM_THREADS;
        thread_condition2 = true;
        t_condvar.notify_all();
        for(auto& t: threads) t.join();

        // third, check the new transaction list contains an ID larger than any seen transaction seen so far
        {
            ScopedEpoch epoch;
            unique_ptr<TransactionSequence> seq { instance.active_transactions() };
            REQUIRE(seq->size() == 1);
            REQUIRE((*seq)[0] > max_transaction_id );
        }

        // done
    }
}

TEST_CASE( "context_high_water_mark", "[context]" ){
    GlobalContext instance;

    { // Init, watermark == 0
        ScopedEpoch epoch;
        REQUIRE( instance.high_water_mark() == 0 );
    }

    { // 2 transactions around
        TransactionImpl* tx1_impl = ThreadContext::create_transaction(); // ts: 0
        tx1_impl->incr_user_count();
        TransactionImpl* tx2_impl = ThreadContext::create_transaction(); // ts: 1
        tx2_impl->incr_user_count();

        REQUIRE(tx2_impl->ts_read() > tx1_impl->ts_read());

        { // first attempt
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == tx1_impl->ts_read() );
            REQUIRE( instance.high_water_mark() == 0 );
        }

        tx1_impl->commit(); // ts: 2

        { // second attempt
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == tx2_impl->ts_read() );
            REQUIRE( instance.high_water_mark() == 1 );
        }

        tx2_impl->commit(); // ts: 3

        { // third attempt
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() > tx2_impl->ts_read() /* transaction ID after commit */ );
            REQUIRE( instance.high_water_mark() == 4);
        }

        tx1_impl->decr_user_count();
        tx2_impl->decr_user_count();
    }

    { // No transactions around == 0
        ScopedEpoch epoch;
        REQUIRE( instance.high_water_mark() == 4 );
    }

    { // few more transactions around
        TransactionImpl* tx1_impl = ThreadContext::create_transaction(); // ts: 4
        tx1_impl->incr_user_count();

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 4 );
            REQUIRE( instance.high_water_mark() == tx1_impl->ts_read() );
        }

        TransactionImpl* tx2_impl = ThreadContext::create_transaction(); // ts: 5
        tx2_impl->incr_user_count();
        REQUIRE(tx2_impl->ts_read() == 5);
        TransactionImpl* tx3_impl = ThreadContext::create_transaction(); // ts: 6
        tx3_impl->incr_user_count();
        REQUIRE(tx3_impl->ts_read() == 6);
        TransactionImpl* tx4_impl = ThreadContext::create_transaction(); // ts: 7
        tx4_impl->incr_user_count();
        REQUIRE(tx4_impl->ts_read() == 7);
        TransactionImpl* tx5_impl = ThreadContext::create_transaction(); // ts: 8
        tx5_impl->incr_user_count();
        REQUIRE(tx5_impl->ts_read() == 8);


        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 4 );
            REQUIRE( instance.high_water_mark() == tx1_impl->ts_read() );
        }

        tx3_impl->rollback(); // ts not changed
        tx3_impl->decr_user_count(); tx3_impl = nullptr;
        tx4_impl->commit(); // ts: 9
        REQUIRE(tx4_impl->ts_read() == 9);
        tx4_impl->decr_user_count(); tx4_impl = nullptr;

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 4 );
            REQUIRE( instance.high_water_mark() == tx1_impl->ts_read() );
        }

        tx1_impl->commit(); // ts: 10
        REQUIRE(tx1_impl->ts_read() == 10);
        tx1_impl->decr_user_count(); tx1_impl = nullptr;

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 5 ); // tx2
            REQUIRE( instance.high_water_mark() == tx2_impl->ts_read() );
        }

        tx2_impl->rollback(); // ts not changed
        REQUIRE(tx2_impl->ts_read() == 5);
        tx2_impl->decr_user_count(); tx2_impl = nullptr;

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 8 ); // tx5
            REQUIRE( instance.high_water_mark() == tx5_impl->ts_read() );
        }


        tx5_impl->rollback(); // ts not changed
        REQUIRE(tx5_impl->ts_read() == 8);
        tx5_impl->decr_user_count(); tx5_impl = nullptr;
        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 11 );
        }
    }

    { // final check
        TransactionImpl* tx1_impl = ThreadContext::create_transaction(); // ts: 11
        tx1_impl->incr_user_count();

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == tx1_impl->ts_read() );
            REQUIRE( instance.high_water_mark() == 11 );
        }

        tx1_impl->rollback(); // ts not changed

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 12 ); // next transaction ID
        }
        tx1_impl->decr_user_count(); tx1_impl = nullptr;

        TransactionImpl* tx2_impl = ThreadContext::create_transaction(); // ts: 12
        tx2_impl->incr_user_count();

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == tx2_impl->ts_read() );
            REQUIRE( instance.high_water_mark() == 12 );
        }

        tx2_impl->commit(); // ts: 13
        REQUIRE(tx2_impl->ts_read() == 13);

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 14 ); // next transaction ID
        }

        tx2_impl->decr_user_count(); tx2_impl = nullptr;

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 14 ); // next transaction ID
        }
    }
}
