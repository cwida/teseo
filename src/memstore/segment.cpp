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

#include "teseo/memstore/segment.hpp"

#include <cassert>
#include <chrono>
#include <iomanip>
#include <ostream>

#include "teseo/aux/partial_result.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/latch_state.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/remove_vertex.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/compiler.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

Segment::Segment() : m_flags(0),  m_fence_key( KEY_MAX ), m_latch(0) {
    m_time_last_rebal = chrono::steady_clock::now();
    m_crawler = nullptr;
    m_used_space = 0;
}

Segment::~Segment() {

}

/*****************************************************************************
 *                                                                           *
 *   Latch                                                                   *
 *                                                                           *
 *****************************************************************************/
void Segment::reader_enter(bool fair_lock){
    uint64_t mask_queue = MASK_WRITER | MASK_REBALANCER | (fair_lock ? MASK_WAIT : 0);

    bool done = false;
    uint64_t expected = m_latch;
    do {
        if(expected & MASK_XLOCK){
            util::pause(); // spin lock
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if(expected & mask_queue){
            uint64_t desired = expected | MASK_XLOCK;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert(((expected & MASK_WAIT) == 0 || !m_queue.empty()) && "If MASK_WAIT is set, then the queue must be non empty");

                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();
                m_queue.append({ State::READ, &producer } );

                assert((expected & MASK_XLOCK) == 0 && "Already locked?");
                m_latch = expected | MASK_WAIT; // unlock

                consumer.wait();

                mask_queue &= ~MASK_WAIT; // don't respect the bit MASK_WAIT as now it should be our turn in the queue
                __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST); // reload expected
            } // else we failed, repeat the loop
        } else if ( UNLIKELY( ((expected & MASK_READERS) ^ MASK_READERS) == 0 ) ){ // freaky bit masks
            throw memstore::Error{ 0 , Error::Type::TooManyReaders };
        } else { // the segment is free
            uint64_t desired = (expected & ~MASK_READERS) | ( ((expected | MASK_VERSION) +1) & MASK_READERS ); // increment the number of readers by 1
            assert((desired & MASK_READERS) > (expected & MASK_READERS) && "It should have increased the number of readers by 1");
            assert((desired & MASK_VERSION) == (expected & MASK_VERSION) && "Version not preserved");
            assert((desired & (MASK_XLOCK | mask_queue)) == 0 && "Flags altered");

            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                done = true;
            } // else, we failed
        }
    } while (!done);
}

void Segment::reader_exit() noexcept {
    assert((m_latch & MASK_READERS) > 0 && "There are no readers registered in this segment");
    assert((m_latch & MASK_WRITER) == 0 && "As this reader locked this segment, no writer can be concurrently operating on it");

    bool done = false;
    uint64_t expected = m_latch;
    do {
        if(expected & MASK_XLOCK){
            //util::pause(); // spin lock
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if( (expected & MASK_READERS) == (MASK_VERSION + 1) && (expected & MASK_WAIT) != 0 ){ // this is the last reader
            uint64_t desired = expected | MASK_XLOCK;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert(!m_queue.empty() && "As MASK_WAIT is set => The queue cannot be empty");
                wake_next();

                if(m_queue.empty()){ // clear the bit MASK_WAIT
                    expected &= ~MASK_WAIT;
                    assert((expected & MASK_WAIT) == 0 && "We didn't clear the bit MASK_WAIT");
                }

                // decrease the number of readers by 1
                uint64_t desired = (expected & ~MASK_READERS) | (((expected >> __builtin_ctzl(Segment::MASK_READERS)) -1) << __builtin_ctzl(Segment::MASK_READERS));
                assert(((desired & MASK_READERS) < (expected & MASK_READERS)) && "It should have decreased the number of readers by 1... ");
                assert((desired & MASK_VERSION) == (expected & MASK_VERSION) && "Version not preserved");
                assert((desired & (MASK_REBALANCER)) == (expected & MASK_REBALANCER) && "Flag REBAL not preserved");
                assert((desired & (MASK_XLOCK | MASK_WRITER)) == 0 && "Flags incorrectly set");

                __atomic_store(&m_latch, &desired, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                done = true;
            } // else we failed, repeat the loop
        } else { // simply decrease the number of readers
            uint64_t desired = (expected & ~MASK_READERS) | (((expected >> __builtin_ctzl(Segment::MASK_READERS)) -1) << __builtin_ctzl(Segment::MASK_READERS));
            assert(((desired & MASK_READERS) < (expected & MASK_READERS)) && "It should have decreased the number of readers by 1... ");
            assert((desired & MASK_VERSION) == (expected & MASK_VERSION) && "Version not preserved");
            assert((desired & (MASK_REBALANCER | MASK_WAIT)) == (expected & (MASK_REBALANCER | MASK_WAIT)) && "Flags not preserved");
            assert((desired & (MASK_XLOCK | MASK_WRITER)) == 0 && "Flags incorrectly set");

            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                done = true;
            } // else, we failed
        }
    } while (!done);
}

uint64_t Segment::optimistic_enter() {
    uint64_t expected = m_latch;
    while( true ){
        if(expected & MASK_XLOCK){
            //util::pause(); // spin lock
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if ( expected & (MASK_WRITER | MASK_REBALANCER) ){ // similar to reader_enter
            uint64_t desired = expected | MASK_XLOCK;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert(((expected & MASK_WAIT) == 0 || !m_queue.empty()) && "If MASK_WAIT is set, then the queue must be non empty");

                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();
                m_queue.append({ State::FREE /* optimistic reader */, &producer } );

                desired = expected | MASK_WAIT;
                assert((desired & MASK_XLOCK) == 0 && "Already locked?");
                __atomic_store(&m_latch, &desired, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                consumer.wait();

                __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST); // reload
            } // else we failed, repeat the loop
        } else { // go on..
            uint64_t version = expected & MASK_VERSION;
            return version;
        }
    }
}

void Segment::writer_enter() noexcept {
    uint64_t maybe_mask_wait = MASK_WAIT;

    bool done = false;
    uint64_t expected = m_latch;
    do {
        if(expected & MASK_XLOCK){
            //util::pause(); // spin lock
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if (expected & (MASK_WRITER | MASK_REBALANCER | maybe_mask_wait | MASK_READERS)){
            uint64_t desired = expected | MASK_XLOCK;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert(((expected & MASK_WAIT) == 0 || !m_queue.empty()) && "If MASK_WAIT is set, then the queue must be non empty");

                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();
                m_queue.append({ State::WRITE, &producer } );

                desired = expected | MASK_WAIT;
                assert((desired & MASK_XLOCK) == 0 && "Already locked?");
                __atomic_store(&m_latch, &desired, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                consumer.wait(); // wait your turn in the queue
                // ZzZ...

                maybe_mask_wait = 0; // do not respect MASK_WAIT after the first time
                __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST); // reload the current value of the spin lock
            } // else we failed, repeat the loop
        } else { // go on
            uint64_t desired = expected | MASK_WRITER;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert((desired & MASK_XLOCK) == 0 && "Do not lock the segment");

#if !defined(NDEBUG)
                assert(m_writer_id == -1 && "Writer ID already set");
                m_writer_id = util::Thread::get_thread_id();
#endif
                done = true;
            } // else we failed, repeat the loop
        }
    } while (!done);
}

void Segment::writer_exit() noexcept {
    assert((m_latch & MASK_READERS) == 0 && "Readers present in the segment");
    assert((m_latch & MASK_WRITER) != 0 && "The segment has not been acquired by a writer");

#if !defined(NDEBUG)
    assert(m_writer_id == util::Thread::get_thread_id() && "Incorrect writer thread id");
    m_writer_id = -1;
#endif

    bool done = false;
    uint64_t expected = m_latch;
    do {
        if(expected & MASK_XLOCK){
            //util::pause(); // spin lock
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if (expected & MASK_WAIT){ // release the next thread from the queue
            uint64_t desired = expected | MASK_XLOCK;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert(!m_queue.empty() && "Because the flag MASK_WAIT is set");
                wake_next();

                desired = expected;
                if(m_queue.empty()){ desired &= ~MASK_WAIT; } // clear the bit MASK_WAIT
                desired = (desired & ~MASK_VERSION) | ((expected +1) & MASK_VERSION);
                desired &= ~MASK_WRITER;

                assert((desired & MASK_XLOCK) == 0 && "Already locked?");
                assert((get_version() +1 == (desired & MASK_VERSION)) || /* overflow */ (get_version() == MASK_VERSION && (desired & MASK_VERSION) == 0)); // next version +1
                __atomic_store(&m_latch, &desired, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                done = true;
            } // else, we failed
        } else {
            uint64_t desired = expected & ~MASK_WRITER; // clear the bit MASK_WRITER (unlock)
            desired = (desired & ~MASK_VERSION) | ((expected +1) & MASK_VERSION);
            assert((get_version() +1 == (desired & MASK_VERSION)) || (get_version() == MASK_VERSION && (desired & MASK_VERSION) == 0)); // next version +1

            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                done = true;
            }
        }
    } while(!done);
}

void Segment::writer_init_xlock() noexcept {
    assert(m_latch == 0 && "This method can be invoked only when the segment has been just initialised");
    m_latch = MASK_WRITER;

#if !defined(NDEBUG)
    assert(m_writer_id == -1 && "Writer ID already set");
    m_writer_id = util::Thread::get_thread_id();
#endif
}

/* static */
void Segment::async_rebalancer_enter(Context& context, Key lfkey, rebalance::Crawler* crawler) {
    Leaf* leaf = context.m_leaf;
    Segment* segment = context.m_segment;
    assert(leaf != nullptr && segment != nullptr);

    bool done = false;
    bool is_first_time = true; // discriminate from the case we have been awaken from the waiting list
    uint64_t expected = segment->m_latch;
    do {
        uint64_t desired = expected | MASK_XLOCK;
        if(expected & MASK_XLOCK){
            //util::pause(); // spin lock
            __atomic_load(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if( __atomic_compare_exchange(&(segment->m_latch), &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){ // acquire the xlock
            if (leaf->check_fence_keys(context.segment_id(), lfkey) != FenceKeysDirection::OK){ // wrong segment
                if(!is_first_time){ handle_mask_wait(segment, &expected); } // if we have been awaken from the waiting list, extract the next worker from the queue
                segment->m_latch = expected; // unlock
                throw Abort{};
            } else if(!segment->need_async_rebalance(lfkey)){ // it also checks for MASK_REBAL
                if(!is_first_time){ handle_mask_wait(segment, &expected); } // if we have been awaken from the waiting list, extract the next worker from the queue
                segment->m_latch = expected; // unlock
                throw rebalance::RebalanceNotNecessary{};
            } else if (expected & (MASK_WRITER | MASK_READERS)){
                assert((expected & MASK_REBALANCER) == 0 && "Already caught by #segment->need_async_rebalance(lfkey)");
                assert(segment->m_rebalancer_id == -1 && "Because MASK_REBALANCER is not set");
                assert(!segment->has_crawler() && "Because MASK_REBALANCER is not set");

                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();
                segment->m_queue.prepend({ State::REBAL, &producer } );

                assert((expected & MASK_XLOCK) == 0 && "Already locked?");
                expected |= MASK_WAIT;
                __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                consumer.wait();

                is_first_time = false;
                __atomic_load(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // reload the value of the spin lock
            } else { // free to access
                assert((expected & MASK_REBALANCER) == 0 && "Already caught by #segment->need_async_rebalance(lfkey)");
                assert(segment->m_rebalancer_id == -1 && "Because MASK_REBALANCER is not set");
                assert(!segment->has_crawler() && "Because MASK_REBALANCER is not set");

#if !defined(NDEBUG)
                segment->m_rebalancer_id = util::Thread::get_thread_id();
#endif
                segment->set_crawler(crawler);

                if(crawler != nullptr){ // nullptr only in testing
                    // we'll set the window here to avoid a data race: if we did it after the lock has been released,
                    // then another rebalancer could steal the window, still unitialised, when browsing through the leaf
                    crawler->set_initial_window(context.segment_id(), segment->used_space());
                }

                assert((expected & MASK_XLOCK) == 0 && "Already locked?");
                expected |= MASK_REBALANCER;
                __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                done = true;
            }
        }
    } while (!done);
}

void Segment::async_rebalancer_exit() noexcept {
    assert((m_latch & MASK_REBALANCER) != 0 && "Rebalancer not set");

    bool done = false;
    uint64_t expected = m_latch;
    do {
        if(expected & MASK_XLOCK){
            //util::pause(); // spin lock
            __atomic_load(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else {
            uint64_t desired = expected | MASK_XLOCK;
            if( __atomic_compare_exchange(&m_latch, &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){
                assert((m_latch & MASK_READERS) == 0 && "Readers are present in the segment");
                assert((m_latch & MASK_WRITER) == 0 && "A writer is present in the segment");

                wake_all();

#if !defined(NDEBUG)
                assert(m_rebalancer_id == util::Thread::get_thread_id());
                m_rebalancer_id = -1;
#endif
                set_crawler(nullptr);
                mark_rebalanced();

                expected = expected & (~MASK_WAIT) & (~MASK_REBALANCER); // clear the bits MASK_WAIT and MASK_REBALANCER
                expected = (expected & ~MASK_VERSION) | ((expected +1) & MASK_VERSION);  // bump the version of this segment

                assert((get_version() +1 == (expected & MASK_VERSION)) || (get_version() == MASK_VERSION && (expected & MASK_VERSION) == 0)); // next version +1

                __atomic_store(&m_latch, &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock
                done = true;
            } // else we failed, repeat
        }
    } while(!done);
}

/*****************************************************************************
 *                                                                           *
 *   Queue                                                                   *
 *                                                                           *
 *****************************************************************************/
void Segment::wake_next(){
    if(m_queue.empty()) return;

    // FREE are "optimistic readers". Wake them up immediately only if there are standard readers after them OR
    // there no writers or rebalancers at all in the queue. We cannot resume the optimistic readers if there are
    // other writers in the queue, because there is no guarantee that, on abortion or on exit, optimistic reader
    // may not resume other threads in the queue.
    if(m_queue[0].m_purpose == State::FREE){
        uint64_t i = 0;
        uint64_t sz = m_queue.size();
        bool is_safe = true;
        bool stop = false;
        while(i < sz && !stop && is_safe){
            auto role = m_queue[i].m_purpose;
            is_safe = role == State::FREE || role == State::READ;
            stop = role != State::FREE;
            i++;
        }

        if(is_safe){
            do {
                m_queue[0].m_promise->set_value();
                m_queue.pop();
            } while(!m_queue.empty() && (m_queue[0].m_purpose == State::READ || m_queue[0].m_purpose == State::FREE));

            return; // done
        } else {
            i = 0;
            do {
                auto item = m_queue[0]; // copy the item. Otherwise if the queue resizes, the ref won't be valid anymore
                m_queue.pop();
                m_queue.append(item);
            } while(i < sz && m_queue[0].m_purpose == State::FREE );

            assert(!m_queue.empty() && m_queue[0].m_purpose != State::FREE); // otherwise we are causing a deadlock
        }
    }

    switch(m_queue[0].m_purpose){
    case State::READ:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && (m_queue[0].m_purpose == State::READ || m_queue[0].m_purpose == State::FREE));
        break;
    case State::WRITE:
        m_queue[0].m_promise->set_value();
        m_queue.pop();
        break;
    case State::REBAL:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::REBAL);
        break;
    default:
        assert(0 && "Invalid state");
    }
}

void Segment::wake_all(){
    assert(get_state() == State::REBAL && "To invoke this method the internal lock must be acquired first");

    while(!m_queue.empty()){
        m_queue[0].m_promise->set_value(); // notify
        m_queue.pop();
    }
}

void Segment::handle_mask_wait(Segment* segment, uint64_t* expected){
    assert(segment != nullptr && "Segment is a null pointer");
    assert(expected != nullptr && "Expected is a null pointer");
    assert((segment->m_latch & MASK_XLOCK) && "We should hold the latch in mutual exclusion before invoking this method");

    segment->wake_next();

    if(segment->m_queue.empty()){
        *expected = *expected & ~MASK_WAIT; // clear the bit MASK_WAIT
    }
}

/*****************************************************************************
 *                                                                           *
 *   State                                                                   *
 *                                                                           *
 *****************************************************************************/

Segment::State Segment::get_state() const noexcept {
    uint64_t v = m_latch;
    if(v & MASK_WRITER){
        return State::WRITE;
    } else if (v & MASK_READERS){
        return State::READ;
    } else if (v & MASK_REBALANCER){
        return State::REBAL;
    } else {
        return State::FREE;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Fence keys                                                              *
 *                                                                           *
 *****************************************************************************/
Key Segment::get_lfkey(const Context& context) {
    assert(context.m_segment != nullptr && "Context not set");
    return context.m_segment->m_fence_key;
}

Key Segment::get_hfkey(const Context& context) {
    uint64_t next_segment_id = context.segment_id() +1;
    if(next_segment_id == context.m_leaf->num_segments()){
        return context.m_leaf->get_hfkey();
    } else {
        return context.m_leaf->get_segment(next_segment_id)->m_fence_key;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/
void Segment::update(Context& context, const Update& update, bool has_source_vertex) {
    profiler::ScopedTimer profiler { profiler::SEGMENT_UPDATE };

    COUT_DEBUG("update: " << update << ", has_source_vertex: " << has_source_vertex);

    // first of all, ensure we hold a writer lock on this segment
    Segment* segment = context.m_segment;
    assert(segment->get_state() == State::WRITE || segment->get_state() == State::REBAL);
    assert(segment->m_writer_id == util::Thread::get_thread_id());
    assert(update.key() >= get_lfkey(context) && "This update does not respect the low fence key of this segment");
    assert(update.key() < get_hfkey(context) && "This update does not respect the high fence key of this segment");

    // perform the update
    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        int64_t space_before = sf->used_space();
        sf->validate(context); // debug only, nop in opt build
        bool success = sf->update(context, update, has_source_vertex);
        sf->validate(context); // debug only, nop in opt build
        int64_t space_after = sf->used_space();
        segment->m_used_space += (space_after - space_before);

        if(!success){
            to_dense_file(context);
            segment->m_used_space += dense_file(context)->update(context, update, has_source_vertex);
        }
    } else {
        assert(segment->is_dense());
        segment->m_used_space += dense_file(context)->update(context, update, has_source_vertex);
    }

    request_async_rebalance(context);
}

void Segment::remove_vertex(RemoveVertex& instance){
    Context& context = instance.context();
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        int64_t space_before = sf->used_space();
        sf->validate(context); // debug only, nop in opt build
        bool success = sf->remove_vertex(instance);
        sf->validate(context); // debug only, nop in opt build
        int64_t space_after = sf->used_space();
        segment->m_used_space += (space_after - space_before);

        if(!success){
            to_dense_file(context);
            segment->m_used_space += dense_file(context)->remove_vertex(instance);
        }

    } else {
        assert(segment->is_dense());
        segment->m_used_space += dense_file(context)->remove_vertex(instance);
    }

    if(!instance.done()){
        instance.m_key = Segment::get_hfkey(context);
        if(instance.m_key.source() > instance.vertex_id()){
            instance.set_done();
        }
    }

    request_async_rebalance(context);
}

void Segment::unlock_vertex(RemoveVertex& instance){
    Context& context = instance.context();
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        sf->validate(context); // debug only, nop in opt build
        sf->unlock_removed_vertex(instance);
        sf->validate(context); // debug only, nop in opt build
    } else {
        assert(segment->is_dense());
        dense_file(context)->unlock_vertex(instance);
    }

    if(!instance.done()){
        instance.m_key = segment->m_fence_key.predecessor();
        if(instance.m_key.source() != instance.vertex_id()){
            instance.set_done();
        }
    }
}

void Segment::rollback(Context& context, const Update& update, transaction::Undo* next){
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        int64_t space_before = sf->used_space();
        sf->rollback(context, update, next);
        int64_t space_after = sf->used_space();
        segment->m_used_space += (space_after - space_before);
    } else {
        assert(segment->is_dense());
        segment->m_used_space += dense_file(context)->rollback(context, update, next);
    }
}


void Segment::request_async_rebalance(Context& context){
    Segment* segment = context.m_segment;
    if(segment->has_requested_rebalance()) return; // we already sent one

    if(segment->is_sparse()){
        SparseFile* sf = context.sparse_file();
        // very scientific threshold ...
        constexpr int64_t THRESHOLD = static_cast<int64_t>(SparseFile::max_num_qwords()) - static_cast<int64_t>(3*OFFSET_VERTEX + 2*OFFSET_VERSION);
        if( static_cast<int64_t>(sf->used_space()) < THRESHOLD ){
            return; // there is still space in the file
        }
    }

    COUT_DEBUG("Request rebalance, leaf: " << context.m_leaf << ", segment: " << context.segment_id());
    segment->set_flag(FLAG_REBAL_REQUESTED, 1);
    //context.m_tree->global_context()->async()->request(context);
    context.m_tree->global_context()->runtime()->schedule_rebalance(context, segment->m_fence_key);
}

/*****************************************************************************
 *                                                                           *
 *   Point look ups                                                          *
 *                                                                           *
 *****************************************************************************/

bool Segment::has_item_optimistic(Context& context, const Key& key, bool is_unlocked) {
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        return sparse_file(context)->has_item_optimistic(context, key, is_unlocked);
    } else {
        return dense_file(context)->has_item_optimistic(context, key, is_unlocked);
    }
}

double Segment::get_weight_optimistic(Context& context, const Key& key) {
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        return sparse_file(context)->get_weight_optimistic(context, key);
    } else {
        return dense_file(context)->get_weight_optimistic(context, key);
    }
}

uint64_t Segment::get_degree(Context& context, Key& next){
    Segment* segment = context.m_segment;
    bool vertex_found = !(next.destination() == 0);
    auto lfkey = Segment::get_lfkey(context);
    auto hfkey = Segment::get_hfkey(context);
    uint64_t degree { 0 };

    if(segment->is_sparse()){
        degree = sparse_file(context)->get_degree(context, next, vertex_found);

        if(context.has_version()) { context.validate_version(); } // before setting the next key check our result is correct
        next = hfkey;
    } else {
        if(context.has_version()) context.validate_version(); // ensure lfkey and hfkey are correct
        Key dfnext = next;
        DenseFile* df = dense_file(context);
        bool done = false;
        do {
            try {
                df->get_degree(context, dfnext, vertex_found, degree);
                done = true;
            } catch( Abort ){
                // see if we can recover from this update
                context.optimistic_bump(lfkey);
                if(context.m_segment != segment ||  segment->m_fence_key != lfkey || !segment->is_dense() ) throw; // we failed
            }
        } while(!done);


        if(context.has_version()){ // for optimistic readers
            auto new_hfkey = Segment::get_hfkey(context);
            if(new_hfkey == hfkey){
                next = hfkey;
            } else {
                next = dfnext;
            }
        } else { // for locked readers
            next = hfkey;
        }
    }

    return degree;
}

/*****************************************************************************
 *                                                                           *
 *   Auxiliary view                                                          *
 *                                                                           *
 *****************************************************************************/
bool Segment::aux_partial_result(Context& context, Key& next, aux::PartialResult* partial_result){
    assert(!context.has_version() && "A read lock for this operation is required");

    Segment* segment = context.m_segment;
    auto hfkey = Segment::get_hfkey(context);
    bool read_next = true; // move to the next segment ?

    if(segment->is_sparse()){
        bool check_end_interval = partial_result->key_to() < hfkey;
        read_next = sparse_file(context)->aux_partial_result(context, next, check_end_interval, partial_result);
    } else {
        read_next = dense_file(context)->aux_partial_result(context, next, partial_result);
    }

    next = hfkey;

    if(hfkey == KEY_MAX){ // we're done
        read_next = false;
    }
    return read_next;
}

/*****************************************************************************
 *                                                                           *
 *   Maintenance                                                             *
 *                                                                           *
 *****************************************************************************/
void Segment::load(Context& context, rebalance::ScratchPad& scratchpad){
    if(context.m_segment->is_sparse()){
        sparse_file(context)->load(scratchpad);
    } else {
        assert(context.m_segment->is_dense());
        dense_file(context)->load(scratchpad);
    }
}

void Segment::save(Context& context, rebalance::ScratchPad& scratchpad, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget_achieved) {
    to_sparse_file(context); // ensure the file is sparse
    SparseFile* sf = sparse_file(context);
    sf->save(context, scratchpad, pos_next_vertex, pos_next_element, target_budget, out_budget_achieved);
    sf->validate_vertex_table(context, /* prune ? */ false); // only if NDEBUG is not defined
    context.m_segment->m_used_space = sf->used_space();
}

void Segment::clear_versions(Context& context){
    Segment* segment = context.m_segment;
    if(segment->is_sparse()){
        context.sparse_file()->clear_versions();
    } else {
        assert(segment->is_dense());
        context.dense_file()->clear_versions();
    }
}

/*****************************************************************************
 *                                                                           *
 *   Prune                                                                   *
 *                                                                           *
 *****************************************************************************/
uint64_t Segment::prune(Context& context, bool rebuild_vertex_table){
    profiler::ScopedTimer profiler { profiler::SEGMENT_PRUNE };

    assert(context.m_leaf != nullptr && "Leaf not set");
    assert(context.m_segment != nullptr && "Segment not set");

    Segment* segment = context.m_segment;
    uint64_t result = 0; // amount of filled space in the segment
    uint64_t maybe_mask_wait = MASK_WAIT;

    bool done = false;
    uint64_t expected = segment->m_latch;
    do {
        const bool is_first_time = maybe_mask_wait; // discriminate from the case we have been awaken from the waiting list
        uint64_t desired = expected | MASK_XLOCK;
        if(expected & MASK_XLOCK){
            util::pause(); // spin lock
            __atomic_load(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST);
        } else if( __atomic_compare_exchange(&(segment->m_latch), &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){ // acquire the xlock
            // an optimisation here: if the segment's state is free or read, check immediately if it can be pruned and retrieve its used space
            if((expected & (MASK_WRITER | MASK_REBALANCER)) == 0 &&
                    (segment->is_dense() /* dense segments do not support pruning */ ||
                    (!context.sparse_file()->is_dirty()  /* nothing to prune here */ &&
                    !(rebuild_vertex_table && segment->need_rebuild_vertex_table()) /* no need to rebuild the vertex table */ ))){
                result = segment->used_space();
                assert((expected & MASK_XLOCK) == 0 && "flag locked set");
                if(!is_first_time) { handle_mask_wait(segment, &expected); } // wake the next worker from the waiting list
                __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                done = true;
            } else if( expected & (MASK_WRITER | MASK_REBALANCER | maybe_mask_wait | MASK_READERS) ){ // the segment is busy atm, try later
                assert(((expected & MASK_WAIT) == 0 || !segment->m_queue.empty()) && "If MASK_WAIT is set, then the queue must be non empty");

                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();
                segment->m_queue.append({ State::WRITE, &producer } );

                assert((expected & MASK_XLOCK) == 0 && "flag locked set?");
                expected |= MASK_WAIT; // unlock
                __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                consumer.wait();

                maybe_mask_wait = 0; // if we have been awaken, then it's our turn to access the segment
                __atomic_load(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // reload the value of expected
            } else { // proceed with the pruning
                assert(segment->is_sparse() && "Dense segments cannot be pruned");

#if !defined(NDEBUG)
                assert(segment->m_writer_id == -1 && "Writer ID already set");
                segment->m_writer_id = util::Thread::get_thread_id();
#endif
                assert((expected & MASK_XLOCK) == 0 && "flag locked set?");
                assert((expected & MASK_WRITER) == 0 && "This thread is entering the segment as writer in mutual exclusion");
                expected |= MASK_WRITER;
                __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                // prune the sparse file
                SparseFile* sf = sparse_file(context);
                sf->validate(context);
                if(sf->is_dirty()){ // otherwise we just need to rebuild the vertex table
                    sf->prune();
                    sf->validate(context);
                }
                if(rebuild_vertex_table) {
                    sf->rebuild_vertex_table(context);
                    segment->set_flag(FLAG_VERTEX_TABLE, 0);
                    sf->validate_vertex_table(context, /* prune ? */ true); // DEBUG only
                }

                result = segment->m_used_space = sf->used_space();
                segment->cancel_rebalance_request();

                // unlock the segment
                segment->writer_exit();

                done = true;
            }
        }
    } while (!done);

    return result;
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/
uint64_t Segment::cardinality(Context& context) {
    if(context.m_segment->is_sparse()){
        return sparse_file(context)->cardinality();
    } else {
        assert(context.m_segment->is_dense());
        return dense_file(context)->cardinality();
    }
}

uint64_t Segment::used_space(Context& context) {
    return context.m_segment->used_space();
}

bool Segment::is_unindexed(Context& context){
    return get_lfkey(context) == get_hfkey(context);
}

LatchState Segment::latch_state() const {
    return LatchState( m_latch );
}

bool Segment::has_requested_rebalance() const {
    return get_flag(FLAG_REBAL_REQUESTED);
}

bool Segment::need_async_rebalance() const {
    return ((m_latch & MASK_REBALANCER) == 0) && /* there is not another rebalancer operating */
           has_requested_rebalance() && /* a rebalance has been requested */
           (std::chrono::steady_clock::now() >= m_time_last_rebal); /* enough time has passed */
}

bool Segment::need_async_rebalance(Key lfkey) const {
    return m_fence_key == lfkey && need_async_rebalance();
}

bool Segment::need_rebuild_vertex_table() const {
    return get_flag(FLAG_VERTEX_TABLE) != 0;
}

void Segment::mark_rebalanced(){
    m_time_last_rebal = std::chrono::steady_clock::now();
    set_flag(FLAG_REBAL_REQUESTED, 0);
}

void Segment::cancel_rebalance_request() {
    set_flag(FLAG_REBAL_REQUESTED, 0);
}

rebalance::Crawler* Segment::get_crawler() const noexcept {
    return m_crawler;
}

bool Segment::has_crawler() const noexcept {
    return m_crawler != nullptr;
}

void Segment::set_crawler(rebalance::Crawler* crawler) noexcept {
    m_crawler = crawler;
}

void Segment::set_flag_rebal_requested() {
    set_flag(FLAG_REBAL_REQUESTED, 1);
}

uint64_t Segment::max_num_readers() const {
    return MASK_READERS >> __builtin_ctzl(Segment::MASK_READERS);
}

/*****************************************************************************
 *                                                                           *
 *   Sparse file                                                             *
 *                                                                           *
 *****************************************************************************/

void Segment::to_sparse_file(Context& context){
    if(context.m_segment->is_sparse()){ return; } // it's already a sparse segment
    profiler::ScopedTimer profiler { profiler::SEGMENT_TO_SPARSE };

    DenseFile* df = dense_file(context);
    df->clear();
    df->~DenseFile();
    new (sparse_file(context)) SparseFile();

    context.m_segment->set_flag(FLAG_FILE_TYPE, 0); /* 0 = sparse file, 1 = dense file */
}

SparseFile* Segment::sparse_file(Context& context) {
    return context.sparse_file();
}

/*****************************************************************************
 *                                                                           *
 *   Dense file                                                              *
 *                                                                           *
 *****************************************************************************/
void Segment::load_to_file(SparseFile* sparse_file, bool is_lhs, void* output_file, void* output_txlocks){
    DenseFile::File* file = reinterpret_cast<DenseFile::File*>(output_file);
    DenseFile::TransactionLocks* transaction_locks = reinterpret_cast<DenseFile::TransactionLocks*>(output_txlocks);

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = sparse_file->get_content_start(is_lhs);
    uint64_t* __restrict c_end = sparse_file->get_content_end(is_lhs);
    uint64_t* __restrict v_start = sparse_file->get_versions_start(is_lhs);
    uint64_t* __restrict v_end = sparse_file->get_versions_end(is_lhs);

     // iterate over the content section
     int64_t c_index = 0;
     int64_t c_length = c_end - c_start;
     int64_t v_index = 0;
     int64_t v_length = v_end - v_start;
     uint64_t v_backptr = 0;
     Vertex* vertex = nullptr;
     Edge* edge = nullptr;
     Version* version = nullptr;

     while(c_index < c_length){
         // Fetch a vertex
         vertex = sparse_file->get_vertex(c_start + c_index);
         edge = nullptr;
         version = nullptr;

         if(v_index < v_length &&  sparse_file->get_version(v_start + v_index)->get_backptr() == v_backptr){
             version = sparse_file->get_version(v_start + v_index);
             v_index += OFFSET_VERSION;
         }

         if(vertex->m_first == 1){ // do not save dummy vertices
             DataItem* data_item = file->append();

             bool is_insert = true;
             if(version != nullptr){
                 data_item->m_version = *version;
                 is_insert = version->is_insert();
             }

             data_item->m_update = Update(/* is vertex ? */ true, is_insert, Key { vertex->m_vertex_id } );
         }

         if(vertex->m_lock == 1){
             transaction_locks->lock(vertex->m_vertex_id);
         }

         c_index += OFFSET_VERTEX;
         v_backptr++;

         // Fetch its edges
         int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
         while(c_index < e_length){
             edge = sparse_file->get_edge(c_start + c_index);
             version = nullptr;

             if(v_index < v_length && sparse_file->get_version(v_start + v_index)->get_backptr() == v_backptr){
                 version = sparse_file->get_version(v_start + v_index);
                 v_index += OFFSET_VERSION;
             }

             DataItem* data_item = file->append();
             bool is_insert = true;
             if(version != nullptr){
                 data_item->m_version = *version;
                 is_insert = version->is_insert();
             }

             data_item->m_update = Update(/* is vertex ? */ false, is_insert, Key { vertex->m_vertex_id, edge->m_destination}, edge->get_weight());

             // next iteration
             c_index += OFFSET_EDGE;
             v_backptr++;
         } // end while, fetch edges
     } // end while, fetch vertices
}

void Segment::to_dense_file(Context& context){
    profiler::ScopedTimer profiler { profiler::SEGMENT_TO_DENSE };
    assert(!context.m_segment->is_dense() && "It's already a dense file");

    SparseFile* sf = sparse_file(context);
    DenseFile::File file;
    DenseFile::TransactionLocks transaction_locks;

    load_to_file(sf, /* true -> lhs */ true, &file, &transaction_locks);
    load_to_file(sf, /* false -> rhs */ false, &file, &transaction_locks);

    sf->~SparseFile();

    DenseFile* df = dense_file(context);
    new (df) DenseFile(move(file), move(transaction_locks));

    context.m_segment->set_flag(FLAG_FILE_TYPE, 1); /* 0 = sparse file, 1 = dense file */
}

DenseFile* Segment::dense_file(Context& context) {
    return context.dense_file();
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

static void print_tabs(std::ostream& out, int tabs){
    auto flags = out.flags();
    out << setw(tabs * 2) << setfill(' ') << ' ';
    out.setf(flags);
}

void Segment::dump() {
    cout << "[Segment] " << (void*) this << ", ";
    if(is_sparse()){
        cout << "sparse, ";
    } else {
        cout << "dense, ";
    }
    cout << "state: " << get_state() << ", ";
    cout << "used space: " << m_used_space << " qwords, ";
    cout << "low fence key: " << m_fence_key << ", ";
    cout << "latch: {" << latch_state() << "}, ";
#if !defined(NDEBUG)
    cout << "writer_id: " << m_writer_id << ", ";
    cout << "rebalancer_id: " << m_rebalancer_id << ", ";
#endif
    cout << "rebalance requested: " << boolalpha << has_requested_rebalance() << ", ";
    cout << "crawler: " << m_crawler << endl;
}

void Segment::dump_and_validate(std::ostream& out, Context& context, bool* integrity_check) {
    Segment* segment = context.m_segment;
    assert(segment != nullptr);

    print_tabs(out, 1);
    out << "+-- [SEGMENT #"  << context.segment_id() << "] " << ((void*) segment);
        out << ", state: " << segment->get_state();
#if !defined(NDEBUG)
        out << ", latch: {" << segment->latch_state() << "}";
        if(segment->m_writer_id != -1){
            out << ", writer_id: " << segment->m_writer_id;
        }
        if(segment->m_rebalancer_id != -1){
            out << ", rebalancer_id: " << segment->m_rebalancer_id;
        }
#endif
    if(segment->has_requested_rebalance()){
        out << ", rebalance requested";
    }
    out << ", used space: " << segment->m_used_space << " qwords";
    out << ", fence keys = [" << Segment::get_lfkey(context) << ", " << Segment::get_hfkey(context) << ") \n";

    dump_file(out, context, integrity_check);
}

void Segment::dump_file(std::ostream& out, Context& context, bool* integrity_check) {
    Segment* segment = context.m_segment;
    if(segment->is_sparse()){
        segment->sparse_file(context)->dump_and_validate(out, context, integrity_check);
    } else {
        assert(segment->is_dense());
        segment->dense_file(context)->dump_and_validate(out, context, integrity_check);
    }
}

void Segment::dump_unfold_undo(std::ostream& out, const transaction::Undo* undo){
    uint64_t tx_max = numeric_limits<uint64_t>::max();
    uint64_t i = 0;

    while(undo != nullptr) {
        const transaction::TransactionImpl* tx = undo->transaction();
        uint64_t read_id = tx->ts_read();
        uint64_t write_id = tx->ts_write();

        print_tabs(out, 5);
        out << i << ". " << undo << ", ";

        if(read_id != write_id){
            out << "version locked by txn read_id: " << read_id << ", write_id: " << write_id;
        } else {
            out << "version (";
            if(tx_max == numeric_limits<uint64_t>::max()){
                out << "+inf";
            } else {
                out << tx_max;
            }
            out << ", " << read_id << "]";
        }

        Update* update = reinterpret_cast<Update*>(undo->payload());
        transaction::Undo* next = undo->next();
        out << ", update: {" << *update << "}, next: " << next << "\n";

        tx_max = read_id;
        undo = next;
    }
}

ostream& operator<<(ostream& out, const Segment::State& state){
    switch(state){
    case Segment::State::FREE:
        out << "FREE";
        break;
    case Segment::State::READ:
        out << "READ";
        break;
    case Segment::State::WRITE:
        out << "WRITE";
        break;
    case Segment::State::REBAL:
        out << "REBAL";
        break;
    default:
        out << "Unknown (" << ((int) state) << ")";
    }
    return out;
}

} // namespace
