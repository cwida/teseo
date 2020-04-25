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
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/transaction/rollback_interface.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::transaction;

namespace {
struct DummyTransactionCallback : public RollbackInterface {
    void do_rollback(void* object, Undo* next) override { } // nop
    string str_undo_payload(const void* object) const override {
       return to_string(*reinterpret_cast<const uint64_t*>(object));
    }
};
}

/**
 * Validate Undo::prune, remove only the last entry in the undo chain
 */
TEST_CASE( "txn_prune1", "[transaction] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    { // dummy invocation
        ScopedEpoch epoch;
        unique_ptr<TransactionSequence> seq { instance.active_transactions() };
        REQUIRE( Undo::prune(nullptr, seq.get()).first == nullptr ); // head
        REQUIRE( Undo::prune(nullptr, seq.get()).second == 0 ); // list length
    }

    TransactionImpl* tx0_impl = ThreadContext::create_transaction(); // ts: 0
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

    TransactionImpl* tx2_impl = ThreadContext::create_transaction(); // ts: 2
    tx2_impl->incr_user_count();
    REQUIRE(tx2_impl->ts_read() == 2);

    TransactionImpl* tx3_impl = ThreadContext::create_transaction(); // ts: 3
    tx3_impl->incr_user_count();
    REQUIRE(tx3_impl->ts_read() == 3);
    payload = tx3_impl->ts_read();
    head = tx3_impl->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx3_impl->commit(); // ts: 4
    REQUIRE(tx3_impl->ts_read() == 4);
    tx3_impl->decr_user_count(); tx3_impl = nullptr;


    TransactionImpl* tx5_impl = ThreadContext::create_transaction(); // ts: 5
    tx5_impl->incr_user_count();
    REQUIRE(tx5_impl->ts_read() == 5);


    TransactionImpl* tx6_impl = ThreadContext::create_transaction(); // ts: 6
    tx6_impl->incr_user_count();
    REQUIRE(tx6_impl->ts_read() == 6);
    payload = tx6_impl->ts_read();
    head = tx6_impl->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx6_impl->commit(); // ts: 7
    REQUIRE(tx6_impl->ts_read() == 7);
    tx6_impl->decr_user_count(); tx6_impl = nullptr;


    TransactionImpl* tx8_impl = ThreadContext::create_transaction(); // ts: 8
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

    // decr system count of the existing transactions, this is protection mechanism for
    // optimistic readers
    Undo::clear(head);

    // done
}

/**
 * Validate Undo::prune on a sequence with pruning involved
 */
TEST_CASE( "txn_prune2", "[transaction] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = ThreadContext::create_transaction(); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = tx_tmp->ts_read();
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes done by tx (4,5) should still be visible
    TransactionImpl* tx1 = ThreadContext::create_transaction(); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = ThreadContext::create_transaction(); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = ThreadContext::create_transaction(); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = ThreadContext::create_transaction(); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from 14, 15 should still be visible
    TransactionImpl* tx3 = ThreadContext::create_transaction(); // ts: 16
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

        head = result.first;
    }

    tx1->decr_user_count();
    tx2->decr_user_count();
    tx3->decr_user_count();

    // decr system count of the existing transactions, this is protection mechanism for
    // optimistic readers
    Undo::clear(head);

    // done
}

/**
 * Validate Undo::prune on a sequence with pruning involved. This test is similar to prune2 with the exception
 * that the last transaction has an uncommitted change
 */
TEST_CASE( "txn_prune3", "[transaction] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = ThreadContext::create_transaction(); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = tx_tmp->ts_read();
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 4,5 should still be visible
    TransactionImpl* tx1 = ThreadContext::create_transaction(); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = ThreadContext::create_transaction(); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = ThreadContext::create_transaction(); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = ThreadContext::create_transaction(); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, with an uncommited change
    TransactionImpl* tx3 = ThreadContext::create_transaction(); // ts: 16
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

    // decr system count of the existing transactions, this is protection mechanism for
    // optimistic readers
    Undo::clear(head);

    // done
}


/**
 * Validate Undo::prune on a sequence with pruning involved. This test is similar to prune2 with the exception
 * that the last transaction has multiple uncommitted changes
 */
TEST_CASE( "txn_prune4", "[transaction] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = ThreadContext::create_transaction(); // ts: 0
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 0);
    uint64_t payload = tx_tmp->ts_read();
    Undo* head = tx_tmp->add_undo(&tx_callback, nullptr, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 1
    REQUIRE(tx_tmp->ts_read() == 1);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 2
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 2);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 3
    REQUIRE(tx_tmp->ts_read() == 3);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 4
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 4);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 5
    REQUIRE(tx_tmp->ts_read() == 5);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 4,5 should still be visible
    TransactionImpl* tx1 = ThreadContext::create_transaction(); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = ThreadContext::create_transaction(); // ts: 7
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 7);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 8
    REQUIRE(tx_tmp->ts_read() == 8);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, the changes from transaction 7,8 should still be visible
    TransactionImpl* tx2 = ThreadContext::create_transaction(); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = ThreadContext::create_transaction(); // ts: 10
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 10);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 11
    REQUIRE(tx_tmp->ts_read() == 11);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 12
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 12);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 13
    REQUIRE(tx_tmp->ts_read() == 13);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    tx_tmp = ThreadContext::create_transaction(); // ts: 14
    tx_tmp->incr_user_count();
    REQUIRE(tx_tmp->ts_read() == 14);
    payload = tx_tmp->ts_read();
    head = tx_tmp->add_undo(&tx_callback, head, sizeof(payload), &payload);
    tx_tmp->commit(); // ts: 15
    REQUIRE(tx_tmp->ts_read() == 15);
    tx_tmp->decr_user_count(); tx_tmp = nullptr;

    // permanent transaction, with an uncommited change
    TransactionImpl* tx3 = ThreadContext::create_transaction(); // ts: 16
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

    // decr system count of the existing transactions, this is protection mechanism for
    // optimistic readers
    Undo::clear(head);

    // done
}

/**
 * Validate Undo::prune on a sequence with pruning involved. This test is similar to prune2 with the exception
 * that each transaction has multiple changes
 */
TEST_CASE( "txn_prune5", "[transaction] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback;

    TransactionImpl* tx_tmp = ThreadContext::create_transaction(); // ts: 0
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

    tx_tmp = ThreadContext::create_transaction(); // ts: 2
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

    tx_tmp = ThreadContext::create_transaction(); // ts: 4
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
    TransactionImpl* tx1 = ThreadContext::create_transaction(); // ts: 6
    tx1->incr_user_count();
    REQUIRE(tx1->ts_read() == 6);

    tx_tmp = ThreadContext::create_transaction(); // ts: 7
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
    TransactionImpl* tx2 = ThreadContext::create_transaction(); // ts: 9
    tx2->incr_user_count();
    REQUIRE(tx2->ts_read() == 9);

    tx_tmp = ThreadContext::create_transaction(); // ts: 10
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

    tx_tmp = ThreadContext::create_transaction(); // ts: 12
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

    tx_tmp = ThreadContext::create_transaction(); // ts: 14
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
    TransactionImpl* tx3 = ThreadContext::create_transaction(); // ts: 16
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

    // decr system count of the existing transactions, this is protection mechanism for
    // optimistic readers
    Undo::clear(head);

    // done
}

/**
 * Validate Undo::prune on old transactions
 */
TEST_CASE( "txn_prune6", "[transaction] [prune]" ){
    GlobalContext instance;
    DummyTransactionCallback tx_callback; // for #dump()

    TransactionImpl* tx0 = ThreadContext::create_transaction(); // ts: 0
    tx0->incr_user_count();
    TransactionImpl* tx1 = ThreadContext::create_transaction(); // ts: 1
    tx1->incr_user_count();
    TransactionImpl* tx2 = ThreadContext::create_transaction(); // ts: 2
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

    // decr system count of the existing transactions, this is protection mechanism for
    // optimistic readers
    Undo::clear(head);
}
