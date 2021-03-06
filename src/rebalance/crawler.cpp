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

#include "teseo/rebalance/crawler.hpp"

#include <cassert>
#include <cmath>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;
using namespace teseo::memstore;

namespace teseo::rebalance {

/*****************************************************************************
 *                                                                           *
 *  Initialisation                                                           *
 *                                                                           *
 *****************************************************************************/

Crawler::Crawler(const memstore::Context& context) : m_context(context), m_can_continue(true), m_can_be_stopped(false), m_own_locks(true), m_window_start(0), m_window_end(0){
    assert(context.m_tree != nullptr);
    assert(context.m_leaf != nullptr);
    assert(context.m_segment == nullptr && "Expected to be used by a merger for the whole leaf, so no segment should be set");
}

Crawler::Crawler(const memstore::Context& context, memstore::Key key) : m_context(context), m_can_continue(true), m_can_be_stopped(true), m_own_locks(true){
    assert(m_context.m_tree != nullptr);
    assert(m_context.m_leaf == nullptr && "A leaf should not be already set. We are going to find it through the usage of the search key");
    assert(m_context.m_segment == nullptr && "A segment should not be already set. We are going to find it through the usage of the search key");
    m_window_start = numeric_limits<int32_t>::max(); // invalid values
    m_window_end = numeric_limits<int32_t>::max();

    m_context.async_rebalancer_enter(key, this);
    assert(m_context.m_segment->get_state() == Segment::State::REBAL && "The segment should be in the rebalance state");
}

Crawler::~Crawler(){
    if(m_can_continue && m_own_locks){
        // Release the acquired segments
        for(int64_t segment_id = m_window_start, end = m_window_end; segment_id < end; segment_id++){
            release_segment(segment_id);
        }
    }
}

void Crawler::set_initial_window(uint64_t segment_id, uint64_t used_space){
    assert(m_window_start == numeric_limits<int32_t>::max() && "Window already initialised");
    assert(m_window_end == numeric_limits<int32_t>::max() && "Window already initialised");
    m_window_start = segment_id;
    m_window_end = m_window_start +1;
    m_used_space = used_space;
}

/*****************************************************************************
 *                                                                           *
 *  Properties                                                               *
 *                                                                           *
 *****************************************************************************/

uint64_t Crawler::cardinality() const {
    Context context = m_context;
    Leaf* leaf = context.m_leaf;

    uint64_t total = 0;
    for(uint64_t segment_id = m_window_start, end = m_window_end; segment_id < end; segment_id++) {
        context.m_segment = leaf->get_segment(segment_id);
        total += Segment::cardinality(context);
    }

    return total;
}

uint64_t Crawler::used_space() const {
    return m_used_space;
}

void Crawler::set_lock_ownership(bool value){
    m_own_locks = value;
}

/*****************************************************************************
 *                                                                           *
 *  Latching                                                                 *
 *                                                                           *
 *****************************************************************************/
void Crawler::leaf_xlock(){
    Leaf* leaf = m_context.m_leaf;

    leaf->lock();
    while(leaf->is_active()){
        std::promise<void> producer;
        std::future<void> consumer = producer.get_future();
        leaf->wait(&producer);
        leaf->unlock();
        consumer.wait();
        leaf->lock();
    }

    assert(!leaf->is_active() && "Someone else is operating at the chunk level");

    // can we still process this segment?
    if(!m_can_continue){
        leaf->wake_next();
        leaf->unlock();
        throw RebalanceNotNecessary{}; // someone else will rebalance our segment
    }

    leaf->set_active(true);
    leaf->unlock();

}
void Crawler::leaf_xunlock(){
    Leaf* leaf = m_context.m_leaf;

    leaf->lock();
    assert(leaf->is_active() && "Expected to be already locked");
    leaf->set_active(false);
    leaf->wake_next();
    leaf->unlock();
}


/*****************************************************************************
 *                                                                           *
 *  Find the window to rebalance                                             *
 *                                                                           *
 *****************************************************************************/

Plan Crawler::make_plan() {
    profiler::ScopedTimer profiler { profiler::CRAWLER_MAKE_PLAN };
    bool do_rebalance = false;
    double height { 0. }; // current height at the calibrator tree
    const uint64_t segment_id = m_window_start; // the leaf in the calibrator tree
    int64_t window_start = segment_id; // the start of the window, in terms of segments
    int64_t window_length = 1; // the length of the window, in terms of number of segments
    int64_t index_left = window_start -1; // next segment to read from the left
    int64_t index_right = window_start + window_length; // next segment to read from the right
    const int64_t num_segments_leaf = m_context.m_leaf->num_segments(); // cast to int64_t to silence a compiler warning

    leaf_xlock();

    window_length *= 2;
    while(!do_rebalance && window_length <= num_segments_leaf){
        height = log2(window_length) +1.;

        // readjust the window
        int64_t window_start_new = (segment_id / static_cast<int64_t>(pow(2, (height -1)))) * window_length;
        if(window_start_new + window_length >= num_segments_leaf){
            window_start_new = num_segments_leaf - window_length;
        }

        COUT_DEBUG("(begin iteration) height: " << height << ", previous start position: " << window_start << ", new start position: " << window_start_new << ", window: [" << window_start_new << ", " << window_start_new + window_length << ")");
        assert(window_start_new <= window_start);
        window_start = window_start_new;
        int64_t window_end = window_start + window_length;

        // read the amount of space filled
        while(index_right < window_end){
            acquire_segment(/*inout*/ index_right, /* right direction ? */ true);
            m_window_end = index_right +1; // excl, we may have acquired more than one segment
            index_right++;
        }
        window_end = index_right; // in #acquire_segment, we may have eaten another crawler, expanding our window

        while(index_left >= window_start){
            acquire_segment(/*inout*/ index_left, /* right direction ? */ false);
            m_window_start = index_left; // incl, we may have acquired more than one segment
            index_left--;
        }

        window_start = index_left +1; // in #acquire_segment, we may have eaten another crawler, expanding our window
        window_length = window_end - window_start;

        // compute the density
        int height_window_in_calibrator_tree = floor(log2(window_length)) +1.;
        int64_t min_space_filled { 0 }, max_space_filled { 0 };
        std::tie(min_space_filled, max_space_filled) = get_thresholds( height_window_in_calibrator_tree, num_segments_leaf );
        if(m_used_space <= max_space_filled){
            do_rebalance = true;
        } else {
            // next window
            if(window_length == num_segments_leaf) break;
            window_length = std::min<int64_t>( window_length * 2, num_segments_leaf );
        }
    }

    // release the latch in the chunk
    assert(m_can_continue == true && "As this is the current active rebalancer");
    m_can_be_stopped = false;
    leaf_xunlock();

    // wait for the threads in the wait list to leave their segment
    for(auto ptr_consumer : m_threads2wait){
        auto f = ptr_consumer->get_future(); // allocate the future object
        f.get(); // wait to be released by the other reader/writer
        delete ptr_consumer;
    }

    set_lock_ownership(false); // the spread operator will release the held locks

    if( do_rebalance ){
        return Plan::create_spread(cardinality(), m_context.m_leaf, m_window_start, m_window_end);
    } else { // split
        m_window_start = 0;
        m_window_end = num_segments_leaf;
        return Plan::create_split(cardinality(), m_used_space, m_context.m_leaf);
    }
}

void Crawler::lock2merge(){
    profiler::ScopedTimer profiler { profiler::CRAWLER_LOCK2MERGE };
    assert(m_window_start == 0 && m_window_end == 0 && "#init values at the ctor");

    leaf_xlock();

    for(int64_t segment_id = 0, end = m_context.m_leaf->num_segments(); segment_id < end; segment_id ++ ){
        acquire_segment(/* in/out */ segment_id, /* right direction ? */ true);
        m_window_end = segment_id +1; // excl.
    }

    leaf_xunlock();

    // wait for the threads in the wait list to leave their gate
    for(auto ptr_consumer : m_threads2wait){
        auto f = ptr_consumer->get_future(); // allocate the future object
        f.get(); // wait to be released by the other reader/writer
        delete ptr_consumer;
    }
}

/*****************************************************************************
 *                                                                           *
 *  Calibrator tree                                                          *
 *                                                                           *
 *****************************************************************************/

/**
 * Density thresholds, to compute the fill factor of the nodes in the calibrator tree associated to the sparse array/PMA
 * The following constraint must be satisfied: 0 < rho_0 < rho_h <= tau_h < tau_0 <= 1.
 * The magic constants below are based on the the work & experiments described in the paper Packed Memory Arrays - Rewired.
 */
constexpr static double DENSITY_RHO_0 = 0.5; // lower bound, leaf
constexpr static double DENSITY_RHO_H = 0.75; // lower bound, root of the calibrator tree
constexpr static double DENSITY_TAU_H = 0.75; // upper bound, root of the calibrator tree
constexpr static double DENSITY_TAU_0 = 1; // upper bound, leaf

int Crawler::get_cb_height_per_chunk(uint64_t num_segments_leaf) const {
    if(context::StaticConfiguration::crawler_calibrator_tree_height > 0){
        return context::StaticConfiguration::crawler_calibrator_tree_height;
    } else {
        return floor(log2(num_segments_leaf)) +1;
    }
}

std::pair<int64_t, int64_t> Crawler::get_thresholds(int window_height, uint64_t num_segments_leaf) const {
    // first compute the density for the given height
    double rho {DENSITY_RHO_0}, tau {DENSITY_TAU_0};
    const int tree_height = get_cb_height_per_chunk(num_segments_leaf);

    // avoid dividing by zero
    if(tree_height > 1){
        const double scale = static_cast<double>(tree_height - window_height) / static_cast<double>(tree_height -1);
        rho = /* max density */ DENSITY_RHO_H - /* delta */ (DENSITY_RHO_H - DENSITY_RHO_0) * scale;
        tau = /* min density */ DENSITY_TAU_H + /* delta */ (DENSITY_TAU_0 - DENSITY_TAU_H) * scale;
    }

    int64_t num_segs = std::min<int64_t>(num_segments_leaf, pow(2.0, window_height -1));
    int64_t space_per_segment = memstore::SparseFile::max_num_qwords();
    int64_t min_space = num_segs * space_per_segment * rho;
    int64_t max_space = num_segs * (space_per_segment - /* always leave 5 qwords of space in each segment */ 5) * tau;
    if(min_space >= max_space) min_space = max_space - 1;

    return std::make_pair(min_space, max_space);
}

/*****************************************************************************
 *                                                                           *
 *  Acquire & release the segments                                           *
 *                                                                           *
 *****************************************************************************/

void Crawler::acquire_segment(int64_t& segment_id, bool is_right_direction){
    Context context = m_context; // temporary context
    Leaf* leaf = m_context.m_leaf;

    assert(leaf != nullptr && "Null pointer");
    assert(segment_id < (int64_t) leaf->num_segments() && "Overflow");

    Segment* segment = leaf->get_segment(segment_id);
    context.m_segment = segment;

    bool done = false;
    uint64_t expected = segment->m_latch;
    do {
        if(expected & Segment::MASK_INVALID){
            throw RebalanceNotNecessary{}; // this leaf has been deleted
        } else if(expected & Segment::MASK_XLOCK){
            util::pause(); // spin lock
            __atomic_load(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST);
            continue; // try again
        }

        uint64_t desired = expected | Segment::MASK_XLOCK;
        if( __atomic_compare_exchange(&(segment->m_latch), &expected, &desired, /* ignore the rest for x86-64 */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST) ){ // locked
            if(expected & Segment::MASK_REBALANCER){
                assert(segment->has_crawler());
                Crawler* ctxt2 = segment->get_crawler();
                assert(ctxt2 != this && "Re-processing the same segment");

                if(!ctxt2->m_can_be_stopped){ // we cannot progress, there is another rebalancer busy
                    std::promise<void> producer;
                    std::future<void> consumer = producer.get_future();
                    segment->m_queue.prepend({ Segment::State::REBAL, &producer } );

                    uint64_t desired = expected | Segment::MASK_WAIT;
                    __atomic_store(&(segment->m_latch), &desired, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                    leaf_xunlock();

                    consumer.wait();

                    leaf_xlock(); // can raise a RebalanceNotNecessary{}

                    // try again...
                    __atomic_load(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST);
                } else { // stop the other rebalancer
                    ctxt2->m_can_continue = false;
                    m_used_space += ctxt2->m_used_space;
                    if(is_right_direction){
                        assert(segment_id < ctxt2->m_window_end && "We are supposed to expand the window forwards");
                        segment_id = ctxt2->m_window_end -1;
                    } else {
                        assert(segment_id >= ctxt2->m_window_start && "We are supposed to expand the window backwards");
                        segment_id = ctxt2->m_window_start;
                    }

                    for(auto p : ctxt2->m_threads2wait){ m_threads2wait.push_back(p); }
                    ctxt2->m_threads2wait.clear();

                    for(int64_t i = ctxt2->m_window_start, end = ctxt2->m_window_end; i < end; i++){
                        Segment* segment2 = leaf->get_segment(i);
                        segment2->set_crawler( this );
#if !defined(NDEBUG)
                        assert(segment2->m_rebalancer_id != -1);
                        //assert(segment2->m_rebalancer_id != util::Thread::get_thread_id()); // invalid only during testing (main thread)
                        segment2->m_rebalancer_id = util::Thread::get_thread_id();
#endif
                    }

                    assert((expected & Segment::MASK_REBALANCER) != 0 && "We're stealing the segment to another rebalancer");
                    __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                    done = true;
                }
            } else {
                int64_t space_filled = Segment::used_space(context);
                switch(segment->get_state()){
                case Segment::State::WRITE:
                    // if a writer is currently processing a segment, then the (pessimistic) assumption is that
                    // it's going to add a new edge with a dummy vertex and a new version
                    space_filled += /* new edge */ OFFSET_EDGE + OFFSET_VERSION + /* dummy vertex, with m_first = 0 */ OFFSET_VERTEX;
                case Segment::State::READ: // fall through
                    m_threads2wait.push_back( new promise<void>() ); // yes, this has to be a pointer as its address needs to remain stable even when the vector resizes
                    segment->m_queue.prepend({ Segment::State::REBAL, m_threads2wait.back() });
                    expected |= Segment::MASK_WAIT;
                case Segment::State::FREE: // fall through
                    segment->set_crawler( this );
    #if !defined(NDEBUG)
                    assert(segment->m_rebalancer_id == -1);
                    segment->m_rebalancer_id = util::Thread::get_thread_id();
    #endif
                    m_used_space += space_filled;

                    assert((expected & Segment::MASK_XLOCK) == 0 && "It should not be flagged as locked");
                    assert((expected & Segment::MASK_REBALANCER) == 0 && "There should not be other rebalancers active");
                    expected |= Segment::MASK_REBALANCER;
                    __atomic_store(&(segment->m_latch), &expected, /* whatever */ __ATOMIC_SEQ_CST); // unlock

                    done = true;
                    break;
                default:
                    assert(0 && "unexpected case");
                } // end switch
            }
        }
    } while(!done);
}

void Crawler::release_segment(int64_t segment_id){
    Leaf* leaf = m_context.m_leaf;

    assert(segment_id < (int64_t) leaf->num_segments() && "Invalid segment/lock ID");
    Segment* segment = leaf->get_segment(segment_id);
    segment->async_rebalancer_exit();
}

/*****************************************************************************
 *                                                                           *
 *  Dump                                                                     *
 *                                                                           *
 *****************************************************************************/

string Crawler::to_string() const {
    stringstream ss;
    ss << "CRAWLER, context: " << m_context << ", ";
    ss << "can_continue: " << boolalpha << m_can_continue << ", ";
    ss << "can_be_stopped: " << m_can_be_stopped << ", ";
    ss << "own locks: " << m_own_locks << ", ";
    ss << "used space: " << m_used_space << " qwords, ";
    ss << "window: [" << m_window_start << ", " << m_window_start << "), ";
    ss << "threads2wait: [";
    for(uint64_t i = 0; i < m_threads2wait.size(); i++){
        if(i > 0) ss << ", ";
        ss << m_threads2wait[i];
    }
    ss << "] (" << m_threads2wait.size() << ")";

    return ss.str();
}

void Crawler::dump() const {
    cout << to_string() << endl;
}

} // namespace

