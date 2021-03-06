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
#include <emmintrin.h>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace teseo {

// If the latch version check did not pass or the itself became invalid, the method validate_lock() throws an Abort{ }
class Abort{ };

} // namespace

namespace teseo::util {

/**
 * An OptimisticLatch can either be acquired in mutual exclusion, with a single writer operating, or checked for its version
 * after having read the content of the protected region associated.
 * It works as follows:
 * - The latch carries a version
 * - In write mode, the version is altered every time the latch is released.
 * - In read mode, the reader:
 *    1. reads the version v of the latch
 *    2. reads the content of the protected region
 *    3. checks whether the current version of the latch is still v
 *      - If yes, it means that the content read from the protected region was correct and the logical operation can proceed
 *      - If no, an Abort{} is thrown and the whole logical operation needs to be restarted from scratch
 *
 * An instance of this class can also store some additional user information in the form of PAYLOAD_BITS. This information is embedded
 * in the atomic implementing the latch.
 */
template<int PAYLOAD_BITS>
class OptimisticLatch {
    OptimisticLatch(const OptimisticLatch<PAYLOAD_BITS>&) = delete;
    OptimisticLatch<PAYLOAD_BITS> operator=(const OptimisticLatch<PAYLOAD_BITS>&) = delete;

    std::atomic<uint64_t> m_version; // the first PAYLOAD_BITS are used as user payload, the following bit is the xlock, the rest is the version number the MSB is used as xlock, the rest for the current version
    constexpr static uint64_t MASK_LATCH = std::numeric_limits<uint64_t>::max() >> PAYLOAD_BITS;
    constexpr static uint64_t MASK_PAYLOAD = ~MASK_LATCH;
    constexpr static uint64_t MASK_PLOCK = 1ull << (63 - PAYLOAD_BITS); // phantom lock, like xlock, but it doesn't alter the version
    constexpr static uint64_t MASK_XLOCK = MASK_PLOCK >> 1;
    constexpr static uint64_t MASK_VERSION = MASK_XLOCK -1;

    bool is_invalid0(uint64_t version) const {
        return (version & MASK_LATCH) == MASK_LATCH;
    }

public:
    OptimisticLatch() : m_version(0) { }

    // Shared lock, simply read the version of the current latch
    uint64_t read_version() const {
        uint64_t version { 0 };
        do { // spin lock while the latch is acquired
            version = m_version.load(std::memory_order_acquire) & MASK_LATCH;
            if(is_invalid0(version)) throw Abort{};
        } while((version & MASK_XLOCK) != 0);  /* != 0, lock acquired by someone else */
        return version & MASK_VERSION;
    }

    bool is_version(uint64_t version) const {
        return (m_version.load(std::memory_order_acquire) & (MASK_XLOCK | MASK_VERSION)) == version;
    }

    void validate_version(uint64_t version) const {
        if(!is_version(version)){ throw Abort{}; }
    }

    uint64_t get_payload() const  {
        if(PAYLOAD_BITS == 0){
            throw std::logic_error("No payload stored in the version (PAYLOAD_BITS == 0)");
        } else {
            return m_version.load(std::memory_order_acquire) >> (64 - PAYLOAD_BITS);
        }
    }

    void set_payload(uint64_t value){
        uint64_t expected = m_version.load(std::memory_order_acquire);
        uint64_t new_value = 0;

        do {
            if(is_invalid0(expected)) { throw Abort{}; }

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
            if(is_invalid0(expected)) {
                throw Abort{};
            } else if(expected & MASK_PLOCK){ // spin lock, already locked by a writer but it is not changing the version
                expected = expected & (MASK_PAYLOAD | MASK_VERSION);
            } else if(expected & MASK_XLOCK){ // already locked ?
                expected = ((expected & MASK_VERSION) +1) | (expected & MASK_PAYLOAD);
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
            if((expected & (MASK_XLOCK | MASK_VERSION)) != version) {
                throw Abort{};
            } else if (expected & MASK_PLOCK){ // spin lock, already locked by a writer but it is not changing the version
                expected = expected & (MASK_PAYLOAD | MASK_VERSION);
            }
            new_value = expected | MASK_XLOCK;
        } while(!m_version.compare_exchange_weak(/* by ref, out */ expected, /* new xlock version */ new_value,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed));
    }

    // Release the exclusive (writer) access
    void unlock() {
        uint64_t version = m_version.load(std::memory_order_acquire);
        assert(((version & MASK_XLOCK) != 0) && "The latch was not acquired in x-mode");
        assert(((version & MASK_PLOCK) == 0) && "The latch was acquired in t-mode");
        assert(!is_invalid0(version) && "The latch is invalid");
        m_version.store(((version & MASK_VERSION) +1) | (version & MASK_PAYLOAD), std::memory_order_release); // the bit for the xlock is implicitly 0
    }

    // Phantom lock, acquire the latch in exclusive mode, but don't alter its version
    void phantom_lock(){
        uint64_t expected = m_version.load(std::memory_order_acquire), new_value (0);
        do {
            if(is_invalid0(expected)) {
                throw Abort{};
            } else if(expected & MASK_PLOCK){ // spin lock, already locked by a writer but it is not changing the version
                expected = expected & (MASK_PAYLOAD | MASK_VERSION);
            } else if(expected & MASK_XLOCK){ // already locked ?
                expected = ((expected & (MASK_VERSION)) +1) | (expected & MASK_PAYLOAD);
            }
            new_value = expected | MASK_PLOCK;
        } while(!m_version.compare_exchange_weak(/* by ref, out */ expected, /* new xlock version */ new_value,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed));
    }

    // Phantom unlock, release the latch, but don't alter its version
    uint64_t phantom_unlock(){
        uint64_t version = m_version.load(std::memory_order_acquire);
        assert(((version & MASK_PLOCK) != 0) && "The latch was not acquired in t-mode");
        assert(((version & MASK_XLOCK) == 0) && "The latch was acquired in x-mode");
        assert(!is_invalid0(version) && "The latch is invalid");
        m_version.store(version & (MASK_VERSION | MASK_PAYLOAD), std::memory_order_release); // the bit for the tlock is implicitly 0
        return version & MASK_VERSION;
    }

    // Check whether the latch has been acquired by some thread
    bool is_locked() const {
        return m_version & (MASK_XLOCK | MASK_PLOCK);
    }

    // Check whether the latch has been marked as invalid with the method #invalidate()
    bool is_invalid() const {
        return is_invalid0(m_version);
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

/**
 * A scoped lock, to acquire & release an optimistic latch in `phantom mode'. Phantom mode
 * implies that the version of the latch is not altered, and therefore optimistic readers
 * can still proceed.
 */
template<typename OptimisticLatch>
class ScopedPhantomLock {
    OptimisticLatch& m_latch; // underlying latch
    bool m_is_released = false; // whether the latch has already been released
public:

    // Acquire the optimistic latch in t-mode
    ScopedPhantomLock(OptimisticLatch& latch) : m_latch(latch) {
        m_latch.phantom_lock();
    };

    // Destructor
    ~ScopedPhantomLock(){
        unlock();
    }

    // Release the optimistic latch in t-mode
    // @return the version associated to the latch
    uint64_t unlock(){
        if(m_is_released){ return 0; }
        uint64_t version = m_latch.phantom_unlock();
        m_is_released = true;
        return version;
    }
};

/**
 * A standard read/write latch, that can be invalidated when required.
 */
class Latch {
    Latch(const Latch&) = delete;
    Latch& operator=(const Latch&) = delete;

    // Convention:
    // -2: the latch is invalid, it fires an Abort exception. Once invalid, it cannot be
    //     reversed. This is used to detect deleted nodes in the tree.
    // -1: the latch has been acquired in write mode, only one thread is allowed
    // 0: the latch is free
    // +1 - +inf: the latch has been acquired in read mode, multiple readers can access it
    std::atomic<int64_t> m_latch {0};

public:
    /**
     * Default ctor
     */
    Latch(){ }

    /**
     * Acquire the latch in read mode, fire an Abort exception if the latch is invalid (the associated node has been deleted)
     */
    void lock_read(){
        int64_t expected { 0 };
        while(!m_latch.compare_exchange_weak(/* by ref, out */ expected, /* by value */ expected+1,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed)){
            if(expected == -2)
                throw Abort {}; // this latch has been invalidated and the node deleted
            else if(expected == -1) // a writer is operating on this node, wait
                expected = 0;
            // else nop, try again with expected > 0, multiple readers
        }
    }

    /**
     * Releases the latch previously acquired in read mode. This method should never Abort if the protocol to
     * acquire/release the latch has been properly respected
     */
    void unlock_read(){
        assert(m_latch > 0 && "The latch should have been previously acquired in read mode");
        m_latch--;
    }

    /**
     * Acquire the latch in write mode, fire an Abort exception if the latch is invalid (the associated node has been deleted)
     */
    void lock_write(){
        int64_t current_value { 0 };
        while(!m_latch.compare_exchange_weak(/* by ref, out */ current_value, /* xclusive mode */ -1,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed)){
            if(current_value == -2) throw Abort {}; // this latch has been invalidated and the node deleted
            _mm_pause();
            current_value = 0; // try again
        }
    }

    /**
     * Attempt to acquire the latch in write mode and return immediately. Return true if the latch has been acquired, false otherwise.
     * It fires an Abort exception if the latch is invalid.
     */
    bool try_lock_write(){
        int64_t current_value { 0 };
        bool result = m_latch.compare_exchange_weak(/* by ref, out */ current_value, /* xclusive mode */ -1,
                /* memory order in case of success */ std::memory_order_release,
                /* memory order in case of failure */ std::memory_order_relaxed
        );
        if(current_value == -2) throw Abort {}; // this latch has been invalidated and the node deleted

        return result;
    }

    /**
     * Releases a latch previously acquired in write mode
     */
    void unlock_write(){
        assert(m_latch == -1 && "The latch should have been acquired previously in write mode");
        m_latch = 0;
    }

    /**
     * Invalidates the given latch
     */
    void invalidate(){
        m_latch = -2;
    }

    /**
     * Get the current value of the latch (for debugging purposes)
     */
    int64_t value() const {
        return m_latch;
    }
};

/**
 * Interface to acquire a latch in Read-Only manner.
 */
class ReadLatch {
    ReadLatch(const ReadLatch& latch) = delete;
    ReadLatch& operator=(const ReadLatch& latch) = delete;

    Latch* m_latch; // the latch owned. When nullptr => already released
public:
    /**
     * Init the instance and acquire the given latch in read mode
     */
    ReadLatch(Latch& latch) : m_latch(nullptr) {
        latch.lock_read();
        m_latch = &latch;
    }

    /**
     * Transfer the ownership of the associated latch
     */
    ReadLatch& operator=(ReadLatch&& old){
        release();
        m_latch = old.m_latch;
        old.m_latch = nullptr;
        return *this;
    }

    /**
     * Release the last acquired latch
     */
    ~ReadLatch(){ release(); }

    /**
     * Lock coupling: acquire the new latch in read mode, release the old latch
     */
    void traverse(Latch& latch){
        // acquire the new latch in read mode
        latch.lock_read();
        // release the old latch
        m_latch->unlock_read();

        // save the new latch
        m_latch = &latch;
    }

    /**
     * Release the current latch
     */
    void release(){
        if(m_latch != nullptr){ // the ctor may have fired an exception when acquired the latch
            m_latch->unlock_read();
            m_latch = nullptr;
        }
    }

};

/**
 * Interface to acquire a latch in Write mode
 */
class WriteLatch {
    Latch* m_latch; // the latch held, when nullptr => already released
    WriteLatch(const WriteLatch& latch) = delete;
    WriteLatch& operator=(const WriteLatch& latch) = delete;

public:
    /**
     * Init the instance and acquire the given latch in write mode
     */
    WriteLatch(Latch& latch) : m_latch(nullptr) {
        latch.lock_write();
        m_latch = &latch;
    }

    /**
     * Transfer the ownership of the latch
     */
    WriteLatch& operator=(WriteLatch&& old){
        release();
        m_latch = old.m_latch;
        old.m_latch = nullptr;
        return *this;
    }

    /**
     * Release the acquired latch
     */
    ~WriteLatch(){ release(); }

    /**
     * Release the acquired latch
     */
    void release(){
        if(m_latch == nullptr) return;
        m_latch->unlock_write();
        m_latch = nullptr;
    }

    /**
     * Invalidate the acquired latch
     */
    void invalidate(){
        if(m_latch == nullptr) throw std::runtime_error("Latch already released");
        m_latch->invalidate();
        m_latch = nullptr;
    }
};

/**
 * A traditional spin lock, that can be acquired by a single thread at the time, whether it's a reader or a writer
 */
class SpinLock {
    Latch m_latch; // internal implementation of the lock

public:
    // Acquire the lock in mutual exclusion
    void lock(){ m_latch.lock_write(); }

    // Release the lock previously acquired
    void unlock(){ m_latch.unlock_write(); }
};

} // namespace
