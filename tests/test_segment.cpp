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
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/latch_state.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/rebalance/crawler.hpp" // RebalanceNotNecessary
#include "teseo/runtime/runtime.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;

/**
 * Check the segment's state is properly set (FREE/READ/WRITE/REBAL) upon access
 */
TEST_CASE("segment_state", "[segment]" ) {
    Teseo teseo;
    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());

    segment->reader_enter();
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(!segment->has_requested_rebalance());
    segment->reader_exit();

    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());

    segment->writer_enter();
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(!segment->has_requested_rebalance());
    segment->writer_exit();

    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());

    // repeat the same test with read, as we should have bumped the version by 1
    REQUIRE(segment->latch_state().m_version == 1);
    segment->reader_enter();
    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(!segment->has_requested_rebalance());
    segment->reader_exit();
    REQUIRE(segment->latch_state().m_version == 1);

    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());

    memstore::Context context{ global_context()->memstore() };
    context.m_leaf = leaf;
    context.m_segment = segment;
    REQUIRE_THROWS_AS( Segment::async_rebalancer_enter(context, KEY_MIN, nullptr), rebalance::RebalanceNotNecessary );

    segment->set_flag_rebal_requested();
    REQUIRE(segment->has_requested_rebalance());

    Segment::async_rebalancer_enter(context, KEY_MIN, nullptr);
    REQUIRE(segment->get_state() == Segment::State::REBAL);
    segment->async_rebalancer_exit();

    REQUIRE(segment->is_sparse());
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(!segment->has_requested_rebalance());
}


/**
 * Check that an excessive number of readers that can cause an overflow but do not corrupt the segment's state.
 */
TEST_CASE("segment_num_readers_overflow", "[segment]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance();

    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    // set the segment's version to 3
    for(int i = 0; i < 3; i++){
        segment->writer_enter();
        segment->writer_exit();
    }

    REQUIRE(segment->latch_state().m_version == 3);
    REQUIRE(segment->get_state() == Segment::State::FREE);

    for(uint64_t i = 1; i <= segment->max_num_readers(); i++){
        REQUIRE_NOTHROW( segment->reader_enter() );
        REQUIRE(segment->get_state() == Segment::State::READ );
        REQUIRE(segment->latch_state().m_readers == i);
        REQUIRE(segment->latch_state().m_version == 3);
    }

    // explicitly check the state before the overflow
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == segment->max_num_readers());
    REQUIRE(segment->latch_state().m_version == 3);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);

    // cause an overflow
    REQUIRE_THROWS_AS(segment->reader_enter(), memstore::Error);

    // same state as before
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == segment->max_num_readers());
    REQUIRE(segment->latch_state().m_version == 3);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);

    // clean up
    for(int64_t i = segment->max_num_readers() -1; i >= 0; i--){
        REQUIRE(segment->get_state() == Segment::State::READ );
        REQUIRE(segment->latch_state().m_readers == i +1);
        REQUIRE_NOTHROW( segment->reader_exit() );
        REQUIRE(segment->latch_state().m_readers == i);
        REQUIRE(segment->latch_state().m_version == 3);
    }

    // explicitly check the state
    REQUIRE(segment->get_state() == Segment::State::FREE );
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
}

/**
 * Check that the readers respect the protocol to wait in the queue and properly observe the bit MASK_WAIT.
 * The logic of the test is:
 * 1. Create t1 (read #1) and access the segment in read mode.
 * 2. Create t2 (write #1) and try to access the segment in write mode. The writer should enter the wait list.
 * 3. Create t3 (read #2). When attempting to access the segment, it should be redirected into the wait list because of t2.
 * 4. Create t4 (read #3), access the the segment directly without respecting the wait list (fair_lock = false).
 * 5. Release t1 and t4, the next accessor should be t2 (write #1).
 * 6. Release t2, the next thread should be t3 (read #2).
 */
TEST_CASE("segment_wait_readers", "[segment]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance();

    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    mutex mutex_;
    condition_variable condvar;
    bool tM_continue = false; // main thread
    bool t1_continue = false; // read #1
    bool t2_continue = false; // write #1
    bool t3_continue = false; // read #3
    bool t4_continue = false; // read #4;

    thread t1 { [&](){
        util::Thread::set_name("read #1");
        segment->reader_enter();

        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t1_continue; });
        }

        segment->reader_exit();
    }};

    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1);
    REQUIRE(segment->latch_state().m_wait == false);

    thread t2 { [&](){
        util::Thread::set_name("write #1");
        segment->writer_enter(); // blocked because of t1
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t2_continue; });
        }
        segment->writer_exit();
    } };

    this_thread::sleep_for(10ms); // as t2 should be blocked
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1);
    REQUIRE(segment->latch_state().m_wait == true); // t2

    thread t3 { [&](){
        util::Thread::set_name("read #2");
        segment->reader_enter(); // it should be blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t3_continue; });
        }
        segment->reader_exit();
    }};

    this_thread::sleep_for(10ms); // as t3 should be blocked
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]

    thread t4 { [&](){
        util::Thread::set_name("read #3");
        segment->reader_enter(/* fair lock */ false); // it should be able to proceed because it's not using the fair lock
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t4_continue; });
        }
        segment->reader_exit();
    }};

    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 2); // t1 and t4
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_version == 0);

    // release t1
    t1_continue = true;
    condvar.notify_all();
    t1.join();

    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1); // t4
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_version == 0);

    // release t4
    t4_continue = true;
    condvar.notify_all();
    t4.join();
    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }

    REQUIRE(segment->get_state() == Segment::State::WRITE); // t2
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_writer == true);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t3]
    REQUIRE(segment->latch_state().m_version == 0);

    // release t2
    t2_continue = true;
    condvar.notify_all();
    t2.join();
    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }

    REQUIRE(segment->get_state() == Segment::State::READ); // t3
    REQUIRE(segment->latch_state().m_readers == 1);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_version == 1); // due to t2#writer_exit()

    // release t3
    t3_continue = true;
    condvar.notify_all();
    t3.join();

    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_version == 1);
}

/**
 * Check that multiple writers respect the protocol to access a segment in mutual exclusion and
 * properly manage the waiting list
 */
TEST_CASE("segment_wait_writers", "[segment]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance();

    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    mutex mutex_;
    condition_variable condvar;
    bool tM_continue = false; // main thread
    bool t1_continue = false; // write #1
    bool t2_continue = false; // write #2
    bool t3_continue = false; // write #3

    // clean state
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_invalid == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t1 { [&](){
        util::Thread::set_name("write #1");
        segment->writer_enter();

        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t1_continue; });
        }

        segment->writer_exit();
    }};

    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_invalid == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t2 { [&](){
        util::Thread::set_name("write #2");
        segment->writer_enter(); // blocked because of t1
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t2_continue; });
        }
        segment->writer_exit();
    } };

    this_thread::sleep_for(10ms); // as t2 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // t2
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t3 { [&](){
        util::Thread::set_name("write #3");
        segment->writer_enter(); // blocked because of t1
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t3_continue; });
        }
        segment->writer_exit();
    } };

    this_thread::sleep_for(10ms); // as t3 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    // release t1
    t1_continue = true;
    condvar.notify_all();
    t1.join();
    this_thread::sleep_for(10ms);

    REQUIRE(segment->get_state() == Segment::State::WRITE); // t2
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t3]
    REQUIRE(segment->latch_state().m_writer == true); // t2
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1); // bumped by t1

    // release t2
    t2_continue = true;
    condvar.notify_all();
    t2.join();
    this_thread::sleep_for(10ms);

    REQUIRE(segment->get_state() == Segment::State::WRITE); // t3
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false); // Q is empty now
    REQUIRE(segment->latch_state().m_writer == true); // t3
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 2); // bumped by t1 and t2

    // release t3
    t3_continue = true;
    condvar.notify_all();
    t3.join();

    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false); // Q is empty now
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 3); // bumped by t1, t2 and t3

    // done
}

// Check that the optimistic readers are properly released from the queue
// Order to test: w, r, o, r, w
// Where w = writer, r = reader, o = optimistic reader
// Expected: after the first writer is released, the the group [r, o, r] should be released together
TEST_CASE("segment_wait_optimistic1", "[segment]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance();

    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    mutex mutex_;
    condition_variable condvar;
    bool tM_continue = false; // main thread
    bool t1_continue = false; // write #1
    bool t2_continue = false; // read #1
    bool t3_continue = false; // optimistic #1
    bool t4_continue = false; // read #2
    bool t5_continue = false; // write #2

    // clean state
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t1 { [&](){
        util::Thread::set_name("write #1");
        segment->writer_enter();

        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t1_continue; });
        }

        segment->writer_exit();
    }};

    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t2 { [&](){
        util::Thread::set_name("read #1");
        segment->reader_enter(); // it should be blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t2_continue; });
        }
        segment->reader_exit();
    }};

    this_thread::sleep_for(10ms); // as t2 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t3 { [&](){
        util::Thread::set_name("optimistic #1");
        uint64_t version = segment->optimistic_enter(); // it should be blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t3_continue; });
        }
        REQUIRE_NOTHROW(segment->optimistic_validate(version));
    }};

    this_thread::sleep_for(10ms); // as t3 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t4 { [&](){
        util::Thread::set_name("read #2");
        // the fair lock should not have any effect in this case. The segment is already busy with a writer.
        segment->reader_enter(/* fair lock */ false);
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t4_continue; });
        }
        segment->reader_exit();
    }};

    this_thread::sleep_for(10ms); // as t4 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3, t4]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t5 { [&](){
        util::Thread::set_name("write #2");
        segment->writer_enter(); // already busy -> block the access
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t5_continue; });
        }
        segment->writer_exit();
    }};

    this_thread::sleep_for(10ms); // as t5 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3, t4, t5]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    // release t1
    t1_continue = true;
    condvar.notify_all();
    t1.join();
    this_thread::sleep_for(10ms);

    // t2, t3 and t4 should have been able to access the segment
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 2); // t2, t3 (optimistic) and t4
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t5]
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    // release t2 and t3
    t2_continue = true;
    t3_continue = true;
    condvar.notify_all();
    t2.join();
    t3.join();

    // the segment should be still occupied by t4
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1); // t4
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t5]
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    // release t4
    t4_continue = true;
    condvar.notify_all();
    t4.join();
    this_thread::sleep_for(10ms);

    REQUIRE(segment->get_state() == Segment::State::WRITE); // t5
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == true); // t5
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    // release t5
    t5_continue = true;
    condvar.notify_all();
    t5.join();

    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 2);

    // done
}

// Check that the optimistic readers are properly released from the queue. Similar to optimistic #1, but
// we are going to use a different order of the threads:
// Order to test: w, o, r, o, w
// Where w = writer, r = reader, o = optimistic reader
// Expected: after the first writer is released, the the group [o, r, o] should be released together
TEST_CASE("segment_wait_optimistic2", "[segment]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance();

    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    mutex mutex_;
    condition_variable condvar;
    bool tM_continue = false; // main thread
    bool t1_continue = false; // write #1
    bool t2_continue = false; // optimistic #1
    bool t3_continue = false; // read #1
    bool t4_continue = false; // optimistic #2
    bool t5_continue = false; // write #2

    // clean state
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t1 { [&](){
        util::Thread::set_name("write #1");
        segment->writer_enter();

        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t1_continue; });
        }

        segment->writer_exit();
    }};

    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t2 { [&](){
        util::Thread::set_name("optimistic #1");
        uint64_t version = segment->optimistic_enter(); // it should be blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t2_continue; });
        }
        REQUIRE_NOTHROW(segment->optimistic_validate(version));
    }};

    this_thread::sleep_for(10ms); // as t2 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t3 { [&](){
        util::Thread::set_name("read #1");
        segment->reader_enter(false); // access blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t3_continue; });
        }
        segment->reader_exit();
    }};

    this_thread::sleep_for(10ms); // as t3 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t4 { [&](){
        uint64_t version = segment->optimistic_enter(); // access blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t4_continue; });
        }
       REQUIRE_THROWS_AS(segment->optimistic_validate(version), Abort);
    }};

    this_thread::sleep_for(10ms); // as t4 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3, t4]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t5 { [&](){
        util::Thread::set_name("write #2");
        segment->writer_enter(); // already busy -> block the access
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t5_continue; });
        }
        segment->writer_exit();
    }};

    this_thread::sleep_for(10ms); // as t5 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3, t4, t5]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    // release t1
    t1_continue = true;
    condvar.notify_all();
    t1.join();
    this_thread::sleep_for(10ms);

    // t2, t3 and t4 should have been able to access the segment
    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1); // t2(optimistic), t3 and t4 (optimistic)
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t5]
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    // release t2
    t2_continue = true;
    condvar.notify_all();
    t2.join();

    REQUIRE(segment->get_state() == Segment::State::READ);
    REQUIRE(segment->latch_state().m_readers == 1); //  t3 and t4 (optimistic)
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t5]
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    // release t3
    t3_continue = true;
    condvar.notify_all();
    t3.join();

    // t3 should release the next writer in the queue because t4 is optimistic (it will fail on its own)
    this_thread::sleep_for(10ms);

    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == true); // t5
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    t4_continue = true;
    condvar.notify_all();
    t4.join(); // t4 is the optimistic reader that should have failed the validation stage

    // release t5
    t5_continue = true;
    condvar.notify_all();
    t5.join();

    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 2);

    // done
}

// Check that the optimistic readers are properly released from the queue. Similar to optimistic #1, but
// we are going to use a different order of the threads:
// Order to test: w, o, o, w
// Where w = writer, r = reader, o = optimistic reader
// Expected: after the first writer is released, then the second writer is released and only at the end the
// the two optimistic readers are released
TEST_CASE("segment_wait_optimistic3", "[segment]" ) {
    Teseo teseo;
    global_context()->runtime()->disable_rebalance();

    ScopedEpoch epoch;
    Leaf* leaf = global_context()->memstore()->index()->find(0, 0).leaf();
    Segment* segment = leaf->get_segment(0);

    mutex mutex_;
    condition_variable condvar;
    bool tM_continue = false; // main thread
    bool t1_continue = false; // write #1
    bool t2_continue = false; // optimistic #1
    bool t3_continue = false; // optimistic #2
    bool t4_continue = false; // write #2

    // clean state
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t1 { [&](){
        util::Thread::set_name("write #1");
        segment->writer_enter();

        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t1_continue; });
        }

        segment->writer_exit();
    }};

    { // sync
        unique_lock<mutex> xlock(mutex_);
        condvar.wait(xlock, [&](){ return tM_continue; });
        tM_continue = false;
    }
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t2 { [&](){
        util::Thread::set_name("optimistic #1");
        uint64_t version = segment->optimistic_enter(); // it should be blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t2_continue; });
        }
        REQUIRE_NOTHROW(segment->optimistic_validate(version));
    }};

    this_thread::sleep_for(10ms); // as t2 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t3 { [&](){
        util::Thread::set_name("optimistic #2");
        uint64_t version = segment->optimistic_enter(); // it should be blocked
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t3_continue; });
        }
        REQUIRE_NOTHROW(segment->optimistic_validate(version));
    }};

    this_thread::sleep_for(10ms); // as t2 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    thread t4 { [&](){
        util::Thread::set_name("write #2");
        segment->writer_enter(); // already busy -> block the access
        { // sync, restrict the scope
            tM_continue = true;
            condvar.notify_all();
            unique_lock<mutex> xlock(mutex_);
            condvar.wait(xlock, [&](){ return t4_continue; });
        }
        segment->writer_exit();
    }};

    this_thread::sleep_for(10ms); // as t4 should be blocked
    REQUIRE(segment->get_state() == Segment::State::WRITE); // t1
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3, t4]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 0);

    // release t1
    t1_continue = true;
    condvar.notify_all();
    t1.join();
    this_thread::sleep_for(10ms);

    REQUIRE(segment->get_state() == Segment::State::WRITE); // t4 !
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == true); // Q: [t2, t3]
    REQUIRE(segment->latch_state().m_writer == true); // t1
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 1);

    // release t4
    t4_continue = true;
    condvar.notify_all();
    t4.join();
    this_thread::sleep_for(10ms);

    REQUIRE(segment->get_state() == Segment::State::FREE); // t2 and t3, they are optimistic readers
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 2);

    t2_continue = true;
    condvar.notify_all();
    t2.join();

    // no change
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 2);

    t3_continue = true;
    condvar.notify_all();
    t3.join();

    // no change
    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
    REQUIRE(segment->latch_state().m_wait == false);
    REQUIRE(segment->latch_state().m_writer == false);
    REQUIRE(segment->latch_state().m_rebalancer == false);
    REQUIRE(segment->latch_state().m_xlock == false);
    REQUIRE(segment->latch_state().m_version == 2);

    // done ...
}


