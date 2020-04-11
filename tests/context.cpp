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

#include "teseo.hpp"
#include "../src/context.hpp"

using namespace std;
using namespace teseo::internal::context;

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

TEST_CASE( "context_transaction_init", "[context]" ){
    GlobalContext instance;
    TransactionImpl* tx_impl = new TransactionImpl(shptr_thread_context());
    tx_impl->incr_user_count();


    tx_impl->decr_user_count();
    tx_impl = nullptr; // do not invoke delete
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
        TransactionImpl* tx1_impl = new TransactionImpl(shptr_thread_context());
        tx1_impl->incr_user_count();
        TransactionImpl* tx2_impl = new TransactionImpl(shptr_thread_context());
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

    uint64_t seq_num_threads[] = {2, 4, 8, 16, 32, 64, 128}; // with valgrind
    //uint64_t seq_num_threads[] = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}; // without valgrind
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

            TransactionImpl* tx1_impl = new TransactionImpl(shptr_thread_context());
            tx1_impl->incr_user_count();
            this_thread::sleep_for(100ms);
            TransactionImpl* tx2_impl = new TransactionImpl(shptr_thread_context());
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
        TransactionImpl* tx1_impl = new TransactionImpl(shptr_thread_context()); // ts: 0
        tx1_impl->incr_user_count();
        TransactionImpl* tx2_impl = new TransactionImpl(shptr_thread_context()); // ts: 1
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
        TransactionImpl* tx1_impl = new TransactionImpl(shptr_thread_context()); // ts: 4
        tx1_impl->incr_user_count();

        {
            ScopedEpoch epoch;
            REQUIRE( instance.high_water_mark() == 4 );
            REQUIRE( instance.high_water_mark() == tx1_impl->ts_read() );
        }

        TransactionImpl* tx2_impl = new TransactionImpl(shptr_thread_context()); // ts: 5
        tx2_impl->incr_user_count();
        REQUIRE(tx2_impl->ts_read() == 5);
        TransactionImpl* tx3_impl = new TransactionImpl(shptr_thread_context()); // ts: 6
        tx3_impl->incr_user_count();
        REQUIRE(tx3_impl->ts_read() == 6);
        TransactionImpl* tx4_impl = new TransactionImpl(shptr_thread_context()); // ts: 7
        tx4_impl->incr_user_count();
        REQUIRE(tx4_impl->ts_read() == 7);
        TransactionImpl* tx5_impl = new TransactionImpl(shptr_thread_context()); // ts: 8
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
        TransactionImpl* tx1_impl = new TransactionImpl(shptr_thread_context()); // ts: 11
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

        TransactionImpl* tx2_impl = new TransactionImpl(shptr_thread_context()); // ts: 12
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

namespace {
struct DummyTransactionCallback : public TransactionRollbackImpl {
    void do_rollback(void* object, Undo* next) override { } // nop
    string str_undo_payload(const void* object) const override {
       return to_string(*reinterpret_cast<const uint64_t*>(object));
    }
};
}

/**
 * Validate Undo::prune, remove only the last entry in the undo chain
 */
TEST_CASE( "context_prune1", "[context] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    { // dummy invocation
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> seq { instance.active_transactions() };
        REQUIRE( Undo::prune(nullptr, seq.get()).first == nullptr ); // head
        REQUIRE( Undo::prune(nullptr, seq.get()).second == 0 ); // list length
    }

    TransactionImpl* tx0_impl = new TransactionImpl(shptr_thread_context()); // ts: 0
    tx0_impl->incr_user_count();
    REQUIRE(tx0_impl->ts_read() == 0);

    { // dummy invocation
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> seq { instance.active_transactions() };
        REQUIRE( Undo::prune(nullptr, seq.get()).first == nullptr ); // head
        REQUIRE( Undo::prune(nullptr, seq.get()).second == 0 ); // list length
    }


    uint64_t payload = tx0_impl->ts_read();
    Undo* head = tx0_impl->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);


    { // nop
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> seq { instance.active_transactions() };
        REQUIRE( Undo::prune(head, seq.get()).first == head ); // head
        REQUIRE( Undo::prune(head, seq.get()).second == 1 ); // list length
    }


    tx0_impl->commit(); // ts: 1
    REQUIRE(tx0_impl->ts_read() == 1);
    tx0_impl->decr_user_count(); tx0_impl = nullptr;

    TransactionImpl* tx2_impl = new TransactionImpl(shptr_thread_context()); // ts: 2
    tx2_impl->incr_user_count();
    REQUIRE(tx2_impl->ts_read() == 2);

    TransactionImpl* tx3_impl = new TransactionImpl(shptr_thread_context()); // ts: 3
    tx3_impl->incr_user_count();
    REQUIRE(tx3_impl->ts_read() == 3);
    payload = tx3_impl->ts_read();
    head = tx3_impl->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx3_impl->commit(); // ts: 4
    REQUIRE(tx3_impl->ts_read() == 4);
    tx3_impl->decr_user_count(); tx3_impl = nullptr;


    TransactionImpl* tx5_impl = new TransactionImpl(shptr_thread_context()); // ts: 5
    tx5_impl->incr_user_count();
    REQUIRE(tx5_impl->ts_read() == 5);


    TransactionImpl* tx6_impl = new TransactionImpl(shptr_thread_context()); // ts: 6
    tx6_impl->incr_user_count();
    REQUIRE(tx6_impl->ts_read() == 6);
    payload = tx6_impl->ts_read();
    head = tx6_impl->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx6_impl->commit(); // ts: 7
    REQUIRE(tx6_impl->ts_read() == 7);
    tx6_impl->decr_user_count(); tx6_impl = nullptr;


    TransactionImpl* tx8_impl = new TransactionImpl(shptr_thread_context()); // ts: 8
    tx8_impl->incr_user_count();
    REQUIRE(tx8_impl->ts_read() == 8);


    { // invoke prune & validate the result
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> ptr_sequence { instance.active_transactions() };
        TransactionSequence* sequence = ptr_sequence.get();
        REQUIRE( ptr_sequence->size() == 4 ); // [ 9, 8, 5, 2 ]
        REQUIRE( (*sequence)[0] == 9 ); // 9 is the ID of the next upcoming transaction ID
        REQUIRE( (*sequence)[1] == 8 );
        REQUIRE( (*sequence)[2] == 5 );
        REQUIRE( (*sequence)[3] == 2 );

        // before:
        //Undo::dump_chain(head, 0);

        auto result = Undo::prune(head, sequence);

        // after:
        //Undo::dump_chain(head, 0);

        // expected result: tx8 -> read original, tx5 -> read 6, tx2 -> read 3
        REQUIRE(result.first == head);
        REQUIRE(result.second == 2);
        Undo* undo = result.first;
        REQUIRE(undo != nullptr);
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 6);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 3);
        REQUIRE(undo->next() == nullptr);
    }

    tx2_impl->decr_user_count();
    tx5_impl->decr_user_count();
    tx8_impl->decr_user_count();
    // done
}

/**
 * Validate Undo::prune on a sequence with pruning involved
 */
TEST_CASE( "context_prune2", "[context] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = tx_tmp->ts_read();
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes done by tx (4,5) should still be visible
    TransactionImpl* tx1 = new TransactionImpl(shptr_thread_context()); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = new TransactionImpl(shptr_thread_context()); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from 14, 15 should still be visible
    TransactionImpl* tx3 = new TransactionImpl(shptr_thread_context()); // ts: 16
    tx3->incr_user_count();
    REQUIRE(tx3->ts_read() == 16);

    { // invoke prune & validate the result
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> ptr_sequence { instance.active_transactions() };
        TransactionSequence* sequence = ptr_sequence.get();
        REQUIRE( ptr_sequence->size() == 4 ); // [ 17, 16, 9, 6 ]
        REQUIRE( (*sequence)[0] == 17 ); // next upcoming transaction ID
        REQUIRE( (*sequence)[1] == 16 );
        REQUIRE( (*sequence)[2] == 9 );
        REQUIRE( (*sequence)[3] == 6 );

        // before:
        //Undo::dump_chain(head, 0);

        auto result = Undo::prune(head, sequence);

        // after:
        //Undo::dump_chain(head, 0);

        // expected result: [10, 7], all the other undos should have been removed
        REQUIRE(result.first != head); // head chopped !
        REQUIRE(result.second == 2);
        Undo* undo = result.first;
        REQUIRE(undo != nullptr);
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 10);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 7);
        REQUIRE(undo->next() == nullptr);
    }

    tx1->decr_user_count();
    tx2->decr_user_count();
    tx3->decr_user_count();
    // done
}

/**
 * Validate Undo::prune on a sequence with pruning involved. This test is similar to prune2 with the exception
 * that the last transaction has an uncommitted change
 */
TEST_CASE( "context_prune3", "[context] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = tx_tmp->ts_read();
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 4,5 should still be visible
    TransactionImpl* tx1 = new TransactionImpl(shptr_thread_context()); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = new TransactionImpl(shptr_thread_context()); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, with an uncommited change
    TransactionImpl* tx3 = new TransactionImpl(shptr_thread_context()); // ts: 16
    tx3->incr_user_count();
    REQUIRE(tx3->ts_read() == 16);
    payload = tx3->ts_read();
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);


    { // invoke prune & validate the result
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> ptr_sequence { instance.active_transactions() };
        TransactionSequence* sequence = ptr_sequence.get();
        REQUIRE( ptr_sequence->size() == 4 ); // [ 17, 16, 9, 6 ]
        REQUIRE( (*sequence)[0] == 17 ); // next upcoming transaction ID
        REQUIRE( (*sequence)[1] == 16 );
        REQUIRE( (*sequence)[2] == 9 );
        REQUIRE( (*sequence)[3] == 6 );

        // before:
        //Undo::dump_chain(head, 0);

        auto result = Undo::prune(head, sequence);

        // after:
        //Undo::dump_chain(head, 0);

        // expected result: [16 (uncommited), 10, 7], all the other undos should have been removed
        REQUIRE(result.first == head);
        REQUIRE(result.second == 3);
        Undo* undo = result.first;
        REQUIRE(undo != nullptr);
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 16);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 10);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 7);
        REQUIRE(undo->next() == nullptr);
    }

    tx1->decr_user_count();
    tx2->decr_user_count();
    tx3->decr_user_count();
    // done
}


/**
 * Validate Undo::prune on a sequence with pruning involved. This test is similar to prune2 with the exception
 * that the last transaction has multiple uncommitted changes
 */
TEST_CASE( "context_prune4", "[context] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = tx_tmp->ts_read();
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 4,5 should still be visible
    TransactionImpl* tx1 = new TransactionImpl(shptr_thread_context()); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = new TransactionImpl(shptr_thread_context()); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, with an uncommited change
    TransactionImpl* tx3 = new TransactionImpl(shptr_thread_context()); // ts: 16
    tx3->incr_user_count();
    REQUIRE(tx3->ts_read() == 16);
    payload = 160;
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 161;
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 162;
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);


    { // invoke prune & validate the result
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> ptr_sequence { instance.active_transactions() };
        TransactionSequence* sequence = ptr_sequence.get();
        REQUIRE( ptr_sequence->size() == 4 ); // [ 17, 16, 9, 6 ]
        REQUIRE( (*sequence)[0] == 17 ); // next upcoming transaction ID
        REQUIRE( (*sequence)[1] == 16 );
        REQUIRE( (*sequence)[2] == 9 );
        REQUIRE( (*sequence)[3] == 6 );

        // before:
        //Undo::dump_chain(head, 0);

        auto result = Undo::prune(head, sequence);

        // after:
        //Undo::dump_chain(head, 0);

        // expected result: [162 (uncommited), 161 (uncommited), 160 (uncommited), 10, 7], all the other undos should have been removed
        REQUIRE(result.first == head);
        REQUIRE(result.second == 5);
        Undo* undo = result.first;
        REQUIRE(undo != nullptr);
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 162);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 161);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 160);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 10);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 7);
        REQUIRE(undo->next() == nullptr);
    }

    tx1->decr_user_count();
    tx2->decr_user_count();
    tx3->decr_user_count();
    // done
}

/**
 * Validate Undo::prune on a sequence with pruning involved. This test is similar to prune2 with the exception
 * that each transaction has multiple changes
 */
TEST_CASE( "context_prune5", "[context] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = 100 + tx_tmp->ts_read() * 10 + 0; // 100
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 101
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 102
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = 100 + tx_tmp->ts_read() * 10 + 0; // 120
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 121
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 122
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = 100 + tx_tmp->ts_read() * 10 + 0; // 140
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 141
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 142
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 4,5 should still be visible
    TransactionImpl* tx1 = new TransactionImpl(shptr_thread_context()); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = 100 + tx_tmp->ts_read() * 10 + 0; // 170
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 171
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 172
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = new TransactionImpl(shptr_thread_context()); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = 100 + tx_tmp->ts_read() * 10 + 0; // 200
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 201
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 202
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = 100 + tx_tmp->ts_read() * 10 + 0; // 220
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 221
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 222
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = new TransactionImpl(shptr_thread_context()); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    payload = 100 + tx_tmp->ts_read() * 10 + 0; // 240
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 1; // 241
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 100 + tx_tmp->ts_read() * 10 + 2; // 242
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, with an uncommited change
    TransactionImpl* tx3 = new TransactionImpl(shptr_thread_context()); // ts: 16
    tx3->incr_user_count();
    REQUIRE(tx3->ts_read() == 16);
    payload = 260;
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 261;
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);
    payload = 262;
    head = tx3->add_undo(&tx_callback, head, sizeof(payload), &payload);


    { // invoke prune & validate the result
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> ptr_sequence { instance.active_transactions() };
        TransactionSequence* sequence = ptr_sequence.get();
        REQUIRE( ptr_sequence->size() == 4 ); // [ 17, 16, 9, 6 ]
        REQUIRE( (*sequence)[0] == 17 ); // next upcoming transaction ID
        REQUIRE( (*sequence)[1] == 16 );
        REQUIRE( (*sequence)[2] == 9 );
        REQUIRE( (*sequence)[3] == 6 );

        // before:
        //Undo::dump_chain(head, 0);

        auto result = Undo::prune(head, sequence);

        // after:
        //Undo::dump_chain(head, 0);

        // expected result: [262 (uncommited), 261 (uncommited), 260 (uncommited), 200, 170], all the other undos should have been removed
        REQUIRE(result.first == head);
        REQUIRE(result.second == 5);
        Undo* undo = result.first;
        REQUIRE(undo != nullptr);
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 262);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 261);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 260);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 200);
        REQUIRE(undo->next() != nullptr);
        undo = undo->next();
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 170);
        REQUIRE(undo->next() == nullptr);
    }

    tx1->decr_user_count();
    tx2->decr_user_count();
    tx3->decr_user_count();
    // done
}

/**
 * Validate Undo::prune on old transactions
 */
TEST_CASE( "context_prune6", "[context] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback; // for #dump()

    TransactionImpl* tx0 = new TransactionImpl(shptr_thread_context()); // ts: 0
    tx0->incr_user_count();
    TransactionImpl* tx1 = new TransactionImpl(shptr_thread_context()); // ts: 1
    tx1->incr_user_count();
    TransactionImpl* tx2 = new TransactionImpl(shptr_thread_context()); // ts: 2
    tx2->incr_user_count();
    uint64_t payload = 2;
    Undo* head = tx2->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx2->commit(); // ts: 3
    tx2->decr_user_count(); tx2 = nullptr;

    { // The change from tx2 should still be maintained
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> ptr_sequence { instance.active_transactions() };
        TransactionSequence* sequence = ptr_sequence.get();
        REQUIRE( ptr_sequence->size() == 3 ); // [ 4, 1,0 ]
        REQUIRE( (*sequence)[0] == 4 ); // transaction ID for the next upcoming transaction
        REQUIRE( (*sequence)[1] == 1 );
        REQUIRE( (*sequence)[2] == 0 );

        // before:
        //Undo::dump_chain(head, 0);

        auto result = Undo::prune(head, sequence);

        // after:
        //Undo::dump_chain(head, 0);

        REQUIRE(result.first == head);
        REQUIRE(result.second == 1);
        Undo* undo = result.first;
        REQUIRE(undo != nullptr);
        REQUIRE(undo->payload() != nullptr);
        REQUIRE(*((uint64_t*)undo->payload()) == 2);
        REQUIRE(undo->next() == nullptr);
    }


    tx0->decr_user_count(); tx0 = nullptr;
    tx1->decr_user_count(); tx1 = nullptr;
}

