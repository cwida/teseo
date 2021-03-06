
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

#include "teseo/util/latch.hpp"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

using namespace std;
using namespace teseo;
using namespace teseo::util;

TEST_CASE( "latch_OptimisticLatch", "[latch]" ) {
    OptimisticLatch<3> latch1;

    // init
    REQUIRE(latch1.get_payload() == 0);
    REQUIRE(latch1.read_version() == 0);

    // check the field payload
    latch1.set_payload(1);
    REQUIRE(latch1.get_payload() == 1);
    REQUIRE(latch1.read_version() == 0);
    latch1.set_payload(7);
    REQUIRE(latch1.get_payload() == 7);
    REQUIRE(latch1.read_version() == 0);

    // update from shared to xlock
    latch1.update(0); // payload = 7, version 0 -> X, xlock
    latch1.set_payload(6);
    REQUIRE(latch1.get_payload() == 6);
    latch1.unlock();
    REQUIRE(latch1.read_version() == 1); // check that unlock updated the version & released the lock
    REQUIRE(latch1.get_payload() == 6); // check that unlock didn't alter the payload of the atomic

    // validate the lock/unlock API
    latch1.lock(); // payload = 6, version 1 -> X, xlock
    latch1.set_payload(5);
    REQUIRE(latch1.get_payload() == 5);
    latch1.unlock();
    REQUIRE(latch1.read_version() == 2); // check that unlock updated the version & released the lock
    REQUIRE(latch1.get_payload() == 5); // check that unlock didn't alter the payload of the atomic

    // reset the payload to 7
    latch1.set_payload(7);
    REQUIRE(latch1.get_payload() == 7);

    // validate the Abort mechanism
    REQUIRE_NOTHROW( latch1.validate_version(2) );
    REQUIRE_THROWS_AS( latch1.validate_version(1), Abort );
    REQUIRE_THROWS_AS( latch1.validate_version(3), Abort );
    REQUIRE_THROWS_AS( latch1.update(3), Abort ); // because the current version is 2
    REQUIRE_NOTHROW( latch1.update(2) ); // payload = 7, version 2 -> X, xlock
    REQUIRE(latch1.get_payload() == 7);
    REQUIRE_THROWS_AS( latch1.validate_version(2), Abort ); // because the latch is acquired
    REQUIRE_THROWS_AS( latch1.validate_version(3), Abort ); // because the latch is acquired
    latch1.unlock();
    REQUIRE(latch1.read_version() == 3); // check that unlock updated the version & released the lock
    REQUIRE(latch1.get_payload() == 7); // check that unlock didn't alter the payload of the atomic

    // phantom lock, it doesn't modify the version of the latch
    REQUIRE( latch1.read_version() == 3); // as set before
    latch1.phantom_lock();
    REQUIRE(latch1.get_payload() == 7); // check that tlock didn't alter the payload of the atomic
    latch1.set_payload(6);
    REQUIRE_NOTHROW( latch1.validate_version(3) ); // the version must still be 3
    REQUIRE( latch1.phantom_unlock() == 3 );
    REQUIRE_NOTHROW( latch1.validate_version(3) ); // even after the latch has been released, the version should still be 3
    REQUIRE(latch1.get_payload() == 6); // check that unlock didn't alter the payload of the atomic

    // check the invalidate() mechanism
    latch1.set_payload(5);
    latch1.invalidate();
    REQUIRE( latch1.is_invalid() == true );
    REQUIRE_THROWS_AS( latch1.read_version(), Abort ); // because the latch is now invalid
    REQUIRE_THROWS_AS( latch1.update(3), Abort );
    REQUIRE_THROWS_AS( latch1.lock(), Abort );
    REQUIRE(latch1.get_payload() == 5); // but the value of the payload has not been changed
}

TEST_CASE( "latch_PhantomLatch1", "[latch]" ) {
    bool modified = false;
    bool ready = false;

    condition_variable condvar;
    mutex mutex_;
    OptimisticLatch<0> latch;

    uint64_t version = latch.read_version();
    latch.phantom_lock(); // Because the phantom lock is acquired
    REQUIRE(latch.is_version(version));

    modified = false;
    auto main_thread = [&]{
        auto v = latch.read_version(); // this is allowed
        REQUIRE(latch.is_version(v));

        {
            unique_lock<mutex> lock(mutex_);
            ready = true;
        }
        condvar.notify_all();

        latch.lock();
        modified = true;
        latch.unlock();
    };

    unique_lock<mutex> lock(mutex_);
    thread t(main_thread);

    condvar.wait(lock, [&]{ return ready == true; });

    this_thread::sleep_for(3s); // wait a bit
    REQUIRE(modified == false);
    REQUIRE(latch.is_version(version));

    latch.phantom_unlock();

    t.join();
    REQUIRE(modified == true);
}

TEST_CASE( "latch_PhantomLatch2", "[latch]" ) {
    bool modified = false;
    bool ready = false;

    condition_variable condvar;
    mutex mutex_;
    OptimisticLatch<0> latch;

    latch.lock();

    modified = false;
    auto main_thread = [&]{
        {
            unique_lock<mutex> lock(mutex_);
            ready = true;
        }
        condvar.notify_all();

        latch.phantom_lock();
        modified = true;
        latch.phantom_unlock();
    };

    unique_lock<mutex> lock(mutex_);
    thread t(main_thread);

    condvar.wait(lock, [&]{ return ready == true; });

    this_thread::sleep_for(3s); // wait a bit
    REQUIRE(modified == false);

    latch.unlock();

    t.join();
    REQUIRE(modified == true);
}

TEST_CASE( "latch_TryLock", "[latch]" ) {
    Latch latch;

    latch.lock_read();
    REQUIRE(latch.try_lock_write() == false);
    latch.unlock_read();

    latch.lock_write();
    REQUIRE(latch.try_lock_write() == false);
    latch.unlock_write();

    REQUIRE(latch.try_lock_write() == true);
    REQUIRE(latch.try_lock_write() == false);
    latch.unlock_write();

    latch.lock_write();
    REQUIRE(latch.try_lock_write() == false);
    latch.unlock_write();
}
