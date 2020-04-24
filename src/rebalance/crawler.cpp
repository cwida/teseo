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
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/util/thread.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;
using namespace teseo::memstore;

namespace teseo::rebalance {

/*****************************************************************************
 *                                                                           *
 *  Initialisation                                                           *
 *                                                                           *
 *****************************************************************************/

Crawler::Crawler(memstore::Context& context) : m_context(context), m_can_continue(true), m_can_be_stopped(true), m_invalidate_upon_release(false){
    assert(context.m_tree != nullptr);
    assert(context.m_leaf != nullptr);

    if(context.m_segment == nullptr) { // this is for use by a merger
        m_can_be_stopped = false;
        m_window_start = 0;
        m_window_end = 0;

    } else { // this is meant to be used by a *synchronous* rebalancer
        m_window_start = context.segment_id();
        m_window_end = m_window_start +1;

        Segment* segment = context.m_segment;

        // install the context in our own segment
        lock_guard<Segment> lock(*segment);

        // someone else is going to rebalance this segment
        if(segment->get_state() == Segment::State::REBAL){ throw RebalanceNotNecessary{}; }

        assert(segment->get_state() == Segment::State::WRITE);
        segment->set_state( Segment::State::REBAL );
        assert(segment->get_num_active_threads() == 1); // that's my self, who should have already acquired this segment in write mode
        segment->decr_num_active_threads();
#if !defined(NDEBUG)
        assert(segment->m_writer_id == util::Thread::get_thread_id());
        segment->m_writer_id = -1;
        assert(segment->m_rebalancer_id == -1);
        segment->m_rebalancer_id = util::Thread::get_thread_id();
#endif
        assert(! segment->has_crawler() && "Already occupied");
        segment->set_crawler( this );

        m_used_space = Segment::used_space(context);
    }
}

Crawler::~Crawler(){
    if(m_can_continue){
        // Release the acquired segments
        for(int64_t segment_id = m_window_start, end = m_window_end; segment_id < end; segment_id++){
            release_segment(segment_id, m_invalidate_upon_release);
        }
    }
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

void Crawler::invalidate(){
    m_invalidate_upon_release = true;
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
    const uint64_t segmnet_id = m_window_start; // the leaf in the calibrator tree
    int64_t window_start = segmnet_id; // the start of the window, in terms of segments
    int64_t window_length = 1; // the length of the window, in terms of number of segments
    int64_t index_left = window_start -1; // next segment to read from the left
    int64_t index_right = window_start + window_length; // next segment to read from the right
    const int64_t num_segments_per_leaf = m_context.m_leaf->num_segments(); // cast to int64_t to silence a compiler warning

    leaf_xlock();

    window_length *= 2;
    while(!do_rebalance && window_length <= num_segments_per_leaf){
        height = log2(window_length) +1.;

        // readjust the window
        int64_t window_start_new = (segmnet_id / static_cast<int64_t>(pow(2, (height -1)))) * window_length;
        if(window_start_new + window_length >= num_segments_per_leaf){
            window_start_new = num_segments_per_leaf - window_length;
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
        int height_in_calibrator_tree = floor(log2(window_length)) +1.;
        int64_t min_space_filled { 0 }, max_space_filled { 0 };
        std::tie(min_space_filled, max_space_filled) = get_thresholds( height_in_calibrator_tree );
        if(m_used_space <= max_space_filled){
            do_rebalance = true;
        } else {
            // next window
            const int64_t num_segments_per_chunk = num_segments_per_leaf; // cast to int64_t to silence a compiler warning
            if(window_length == num_segments_per_leaf) break;
            window_length = std::min<int64_t>( window_length * 2, num_segments_per_chunk );
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

    if( do_rebalance ){
        return Plan::create_spread(cardinality(), m_context.m_leaf, m_window_start, m_window_end);
    } else { // split
        m_window_start = 0;
        m_window_end = num_segments_per_leaf;

        int64_t ideal_number_segments = static_cast<double>(m_used_space) / (0.75 * context::StaticConfiguration::memstore_segment_size);
        int64_t num_segments = max<int64_t>(context::StaticConfiguration::memstore_num_segments_per_leaf, ideal_number_segments);
        if(num_segments == num_segments_per_leaf){
            return Plan::create_spread(cardinality(), m_context.m_leaf, m_window_start, m_window_end);
        } else {
            return Plan::create_split(cardinality(), m_context.m_leaf, num_segments);
        }
    }
}

void Crawler::lock2merge(){
    profiler::ScopedTimer profiler { profiler::CRAWLER_LOCK2MERGE };

    m_window_start = 0;

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

int64_t Crawler::get_cb_height_per_chunk() const {
    if(context::StaticConfiguration::crawler_calibrator_tree_height > 0){
        return context::StaticConfiguration::crawler_calibrator_tree_height;
    } else {
        return floor(log2(context::StaticConfiguration::memstore_num_segments_per_leaf)) +1.0;
    }
}

std::pair<int64_t, int64_t> Crawler::get_thresholds(int height) const {
    // first compute the density for the given height
    double rho {DENSITY_RHO_0}, tau {DENSITY_TAU_0};
    const int tree_height = get_cb_height_per_chunk();

    // avoid diving by zero
    if(tree_height > 1){
        const double scale = static_cast<double>(tree_height - height) / static_cast<double>(tree_height -1);
        rho = /* max density */ DENSITY_RHO_H - /* delta */ (DENSITY_RHO_H - DENSITY_RHO_0) * scale;
        tau = /* min density */ DENSITY_TAU_H + /* delta */ (DENSITY_TAU_0 - DENSITY_TAU_H) * scale;
    }

    int64_t num_segs = std::min<int64_t>(context::StaticConfiguration::memstore_num_segments_per_leaf, pow(2.0, height -1));
    int64_t space_per_segment = context::StaticConfiguration::memstore_segment_size;
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
    segment->lock();

    bool done = false;
    do {
        int64_t space_filled = Segment::used_space(context);
        switch(segment->get_state()){
        case Segment::State::WRITE:
            // if a writer is currently processing a segment, then the (pessimistic) assumption is that it's going to add a new single entry
            assert(segment->m_writer_id != -1);
            space_filled += OFFSET_ELEMENT *2 /* with m_first = 0 */ + OFFSET_VERSION;
        case Segment::State::READ: // fall through
            m_threads2wait.push_back( new promise<void>() ); // yes, this has to be a pointer as its address needs to remain stable even when the vector resizes
            segment->m_queue.prepend({ Segment::State::REBAL, m_threads2wait.back() });
        case Segment::State::FREE: // fall through
            segment->set_state( Segment::State::REBAL ); // the releasing worker shall not set this segment to FREE
            segment->set_crawler( this );
#if !defined(NDEBUG)
            assert(segment->m_rebalancer_id == -1);
            segment->m_rebalancer_id = util::Thread::get_thread_id();
#endif
            m_used_space += space_filled;

            done = true;
            break;
        case Segment::State::REBAL: {
            assert(segment->has_crawler());
            Crawler* ctxt2 = segment->get_crawler();
            if(!ctxt2->m_can_be_stopped){ // we cannot progress, there is another rebalancer busy
                std::promise<void> producer;
                std::future<void> consumer = producer.get_future();
                segment->m_queue.prepend({ Segment::State::REBAL, &producer } );
                segment->unlock();

                leaf_xunlock();

                consumer.wait();

                leaf_xlock(); // can raise a RebalanceNotNecessary{}
                segment->lock();

                // try again...
            } else { // stop the other rebalancer
                ctxt2->m_can_continue = false;
                m_used_space += ctxt2->m_used_space;
                if(is_right_direction){
                    segment_id = ctxt2->m_window_end -1;
                } else {
                    segment_id = ctxt2->m_window_start;
                }

                for(auto p : ctxt2->m_threads2wait){ m_threads2wait.push_back(p); }
                ctxt2->m_threads2wait.clear();

                for(int64_t i = ctxt2->m_window_start, end = ctxt2->m_window_end; i < end; i++){
                    Segment* segment2 = leaf->get_segment(i);
                    segment2->set_crawler( this );
#if !defined(NDEBUG)
                    segment2->m_rebalancer_id = util::Thread::get_thread_id();
#endif
                }

                done = true;
            }
            } break;
        default:
            assert(0 && "unexpected case");
        }
    } while(!done);

    segment->unlock();
}

void Crawler::release_segment(int64_t segment_id, bool invalidate){
    Leaf* leaf = m_context.m_leaf;

    assert(segment_id < (int64_t) leaf->num_segments() && "Invalid segment/lock ID");
    Segment* segment = leaf->get_segment(segment_id);

    // acquire the spin lock associated to this segment
    segment->lock();

    assert(segment->get_state() == Segment::State::REBAL && "This segment was supposed to be acquired previously");
    assert(segment->get_num_active_threads() == 0 && "This segment should be closed for rebalancing");
#if !defined(NDEBUG)
    assert(segment->m_rebalancer_id == util::Thread::get_thread_id());
    segment->m_rebalancer_id = -1;
#endif

    segment->set_state( Segment::State::FREE );
    segment->set_crawler( nullptr );
    segment->mark_rebalanced();

    // Use #wake_all rather than #wake_next! Potentially the fence keys have been changed, threads
    // upon wake up might move to other segments. If there are other threads in the wait list, they
    // might potentially end up blocked forever.
    segment->wake_all();

    // done
    if(invalidate){
        segment->invalidate();
    } else {
        segment->unlock();
    }
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
    ss << "invalidate_upon_release: " << m_invalidate_upon_release << ", ";
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
    cout << to_string();
}

} // namespace

