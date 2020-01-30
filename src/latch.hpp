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

#pragma once

#include <atomic>
#include <cassert>
#include <limits>

#include "error.hpp"

namespace teseo::internal {

// if the latch check did not pass, the method validate_lock() throws an Abort{ }
class Abort{ };


template<int PAYLOAD_BITS = 0>
class OptimisticLatch {
    std::atomic<uint64_t> m_version; // the first PAYLOAD_BITS are used as user payload, the following bit is the xlock, the rest is the version number the MSB is used as xlock, the rest for the current version
    constexpr static uint64_t MASK_LATCH = std::numeric_limits<uint64_t>::max() >> PAYLOAD_BITS;
    constexpr static uint64_t MASK_PAYLOAD = ~MASK_LATCH;
    constexpr static uint64_t MASK_XLOCK = 1ull << (63 - PAYLOAD_BITS);
    constexpr static uint64_t MASK_VERSION = MASK_XLOCK -1;

public:





    uint64_t read_version() const {
        uint64_t version { 0 };
        do { // spin lock while the latch is acquired
            version = m_version.load(std::memory_order_acquire);
            if(is_invalid(version)) throw Abort{};
        } while(version & MASK_XLOCK);
        return version;
    }

    void validate_version(uint64_t version) const {
        if((m_version.load(std::memory_order_acquire) & MASK_LATCH) != version){ throw Abort{}; }
    }


    uint64_t get_payload() const  {
        if(PAYLOAD_BITS == 0){
            RAISE(InternalError, "No payload stored in the version (PAYLOAD_BITS == 0)");
        } else {
            return m_version.load(std::memory_order_acquire) >> (64 - PAYLOAD_BITS);
        }
    }

    void set_payload(uint64_t value){
        uint64_t expected = m_version.load(std::memory_order_acquire);
        uint64_t new_value = 0;

        do {
            if(is_invalid(expected)) { throw Abort{}; }

            new_value = /* latch content */ (expected & MASK_LATCH) |
                        /* payload */ value << (64 - PAYLOAD_BITS);
        } while(!m_version.compare_exchange_weak(/* by ref, out */ expected, /* new xlock version */ new_value,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed));
    }

    // Acquire exclusive (writer) access to the underlying latch
    void lock(){
        uint64_t expected = m_version.load(std::memory_order_acquire), new_value (0);
        do {
            if(is_invalid(expected)) {
                throw Abort{};
            } else if(expected & MASK_XLOCK){ // already locked ?
                expected = ((expected & (MASK_VERSION)) +1) | (expected & MASK_LATCH);
            }
            new_value = expected | MASK_XLOCK;
        } while(!m_version.compare_exchange_weak(/* by ref, out */ expected, /* new xlock version */ new_value,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed));
    }

    // Acquire an xlock on the latch only iff the current version is the equal to the one given
    void update(uint64_t version){
        uint64_t expected = m_version.load(std::memory_order_acquire), new_value (0);
        do {
            if(expected & MASK_VERSION != version) {
                throw Abort{};
            } else if(expected & MASK_XLOCK){ // already locked ?
                expected = ((expected & (MASK_VERSION)) +1) | (expected & MASK_LATCH);
            }
            new_value = expected | MASK_XLOCK;
        } while(!m_version.compare_exchange_weak(/* by ref, out */ expected, /* new xlock version */ new_value,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed));
    }

    // Release the exclusive (writer) access
    void unlock() {
        uint64_t version = m_version.load(std::memory_order_acquire);
        assert((version & MASK_XLOCK) == true && "The latch was not acquired");
        m_version.store(((version & MASK_VERSION) +1) | (version & MASK_LATCH), std::memory_order_release);
    }

    bool is_invalid(uint64_t version) const {
        return (version & MASK_LATCH) == MASK_LATCH;
    }

    // Invalidate the current latch/node
    void invalidate() {
        uint64_t expected = m_version.load(std::memory_order_acquire);
        uint64_t new_value = 0;
        do {
            new_value = /* keep the payload as it was */ (expected & MASK_PAYLOAD) | MASK_LATCH;
        } while(!m_version.compare_exchange_weak(/* by ref, out */ expected, /* new xlock version */ new_value,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed));
    }
};

//template<int N>
//class ScopedOXLock {
//    OptimisticLatch<N>* m_latch { nullptr };
//public:
//
//    ScopedOXLock(OptimisticLatch<N>& latch){
//        latch.lock();
//        m_latch = &latch;
//    }
//
//    ScopedOXLock(OptimisticLatch<N>& latch, uint64_t version) {
//        latch.update(version); // it might throw an Abort{}
//        m_latch = &latch;
//    }
//
//    ~ScopedOXLock(){
//        if(m_latch != nullptr) m_latch->unlock();
//    }
//
//};




}
