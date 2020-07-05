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
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::context;
using namespace teseo::memstore;
using namespace teseo::rebalance;

static_assert(StaticConfiguration::test_mode, "Reconfigure in test mode (configure --enable-test), otherwise the sparse arrays are too large for these tests");

/**
 * Check the crawler can properly acquire two segments
 */
TEST_CASE("rb_crawler1", "[rebalance]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    ScopedEpoch epoch;
    Memstore* memstore = global_context()->memstore();
    Leaf* leaf = memstore->index()->find(0).leaf();
    leaf->get_segment(0)->set_flag_rebal_requested();

    Context context { memstore };
    Crawler crawler { context, KEY_MIN };
    REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::REBAL);

    auto plan = crawler.make_plan();

    REQUIRE(plan.window_length() == 2);
    REQUIRE(plan.is_spread());
    REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::REBAL);
    REQUIRE(leaf->get_segment(2)->get_state() == Segment::State::FREE);
}

/**
 * Spread the elements in two segments
 */
TEST_CASE("rb_spread2", "[rebalance]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(20);
    tx.insert_vertex(10);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.commit();
    //memstore->dump();

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.window_start() == 0);
        REQUIRE(plan.window_length() == 2);
        REQUIRE(plan.cardinality() == 4);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    tx = teseo.start_transaction();
    REQUIRE(tx.num_vertices() == 4);
    REQUIRE(tx.has_vertex(10));
    REQUIRE(tx.has_vertex(20));
    REQUIRE(tx.has_vertex(30));
    REQUIRE(tx.has_vertex(40));
}

/**
 * Check the crawler can properly merge with other crawlers, left to right
 */
TEST_CASE("rb_crawler2", "[rebalance]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.commit();
    global_context()->runtime()->rebalance_first_leaf();
    // segment 0: [11, 21]
    // segment 1: [21, 31]

    ScopedEpoch epoch;
    Memstore* memstore = global_context()->memstore();
    Leaf* leaf = memstore->index()->find(0).leaf();
    Context context { memstore };

    // Lock segment 0
    Segment* segment0 = leaf->get_segment(0);
    segment0->set_flag_rebal_requested();
    Crawler crawler0 { context, segment0->m_fence_key };
    REQUIRE(segment0->get_state() == Segment::State::REBAL);

    // Lock segment1
    Segment* segment1 = leaf->get_segment(1);
    segment1->set_flag_rebal_requested();
    Crawler crawler1 { context, segment1->m_fence_key };
    REQUIRE(segment1->get_state() == Segment::State::REBAL);
    REQUIRE(segment0->get_crawler() != segment1->get_crawler());

    // Let crawler0 proceed
    auto plan = crawler0.make_plan();
    REQUIRE(plan.window_length() == 2);
    REQUIRE(plan.is_spread());
    REQUIRE(segment0->get_state() == Segment::State::REBAL);
    REQUIRE(segment1->get_state() == Segment::State::REBAL);
    REQUIRE(segment0->get_crawler() == segment1->get_crawler());
    REQUIRE_THROWS_AS(crawler1.make_plan(), RebalanceNotNecessary);
}

/**
 * Check the crawler can properly merge with other crawlers, right to left
 */
TEST_CASE("rb_crawler3", "[rebalance]"){
    Teseo teseo;
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.commit();
    global_context()->runtime()->rebalance_first_leaf();
    // segment 0: [11, 21]
    // segment 1: [21, 31]

    ScopedEpoch epoch;
    Memstore* memstore = global_context()->memstore();
    Context context { memstore };
    Leaf* leaf = memstore->index()->find(0).leaf();

    // Lock segment 0
    Segment* segment0 = leaf->get_segment(0);
    segment0->set_flag_rebal_requested();
    Crawler crawler0 { context, segment0->m_fence_key };
    REQUIRE(segment0->get_state() == Segment::State::REBAL);

    // Lock segment1
    Segment* segment1 = leaf->get_segment(1);
    segment1->set_flag_rebal_requested();
    Crawler crawler1 { context, segment1->m_fence_key };
    REQUIRE(segment1->get_state() == Segment::State::REBAL);
    REQUIRE(segment0->get_crawler() != segment1->get_crawler());

    // Let crawler1 proceed
    auto plan = crawler1.make_plan();
    REQUIRE(plan.window_length() == 2);
    REQUIRE(plan.is_spread());
    REQUIRE(segment0->get_state() == Segment::State::REBAL);
    REQUIRE(segment1->get_state() == Segment::State::REBAL);
    REQUIRE(segment0->get_crawler() == segment1->get_crawler());
    REQUIRE_THROWS_AS(crawler0.make_plan(), RebalanceNotNecessary);
}

/**
 * Spread the elements in four segments
 */
TEST_CASE("rb_spread4", "[rebalance]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t MAX_VERTEX_ID = 200;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.commit();

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context , segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.window_start() == 0);
        REQUIRE(plan.window_length() == 4);
        REQUIRE(plan.cardinality() == 20);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    /**
     * Check all segments have been correctly released
     */
    {
        ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        for(uint64_t segment_id = 0; segment_id < 4; segment_id ++){
            Segment* segment = leaf->get_segment(segment_id);
            REQUIRE(segment->get_state() == Segment::State::FREE);
            REQUIRE(segment->is_sparse());
            REQUIRE(!segment->has_requested_rebalance());
        }
    }


    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }
}

/**
 * Split over two leaves, only vertices
 */
TEST_CASE("rb_split1", "[rebalance]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t MAX_VERTEX_ID = 1000; // fill completely 2 leaves

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    uint64_t cardinality = tx.num_vertices();
    tx.commit();

    //memstore->dump();

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.cardinality() == cardinality);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    //memstore->dump();

    /**
     * Check all segments have been correctly released
     */
    {
        ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        for(uint64_t segment_id = 0; segment_id < 4; segment_id ++){
            Segment* segment = leaf->get_segment(segment_id);
            REQUIRE(segment->get_state() == Segment::State::FREE);
            REQUIRE(segment->is_sparse());
            REQUIRE(!segment->has_requested_rebalance());
        }
    }


    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }
}

/**
 * Split over many leaves, only vertices
 */
TEST_CASE("rb_split2", "[rebalance]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t MAX_VERTEX_ID = 2220; // fill partially multiple leaves

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    uint64_t cardinality = tx.num_vertices();
    tx.commit();

    //memstore->dump();

    /**
     * Rebalance
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.cardinality() == cardinality);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    //memstore->dump();

    /**
     * Check all segments have been correctly released
     */
    {
        ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        for(uint64_t segment_id = 0; segment_id < 4; segment_id ++){
            Segment* segment = leaf->get_segment(segment_id);
            REQUIRE(segment->get_state() == Segment::State::FREE);
            REQUIRE(segment->is_sparse());
            REQUIRE(!segment->has_requested_rebalance());
        }
    }


    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }
}

/**
 * Perform two rebalances, first a spread & then a split over multiple vertices
 */
TEST_CASE("rb_split3", "[rebalance]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t MAX_VERTEX_ID = 400; // spread

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    uint64_t cardinality = tx.num_vertices();
    tx.commit();

    //memstore->dump();

    /**
     * First rebalance, spread
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.cardinality() == cardinality);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }
    { // Check all segments have been correctly released
        ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        for(uint64_t segment_id = 0; segment_id < 4; segment_id ++){
            Segment* segment = leaf->get_segment(segment_id);
            REQUIRE(segment->get_state() == Segment::State::FREE);
            REQUIRE(segment->is_sparse());
            REQUIRE(!segment->has_requested_rebalance());
        }
    }

    tx = teseo.start_transaction();
    for(uint64_t vertex_id = 85; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    cardinality = tx.num_vertices();
    tx.commit();

    //memstore->dump();

    /**
     * Second rebalance, split
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.cardinality() == cardinality);
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }
    { // Check all segments have been correctly released
        ScopedEpoch epoch;
        Leaf* leaf = memstore->index()->find(0).leaf();
        for(uint64_t segment_id = 0; segment_id < 4; segment_id ++){
            Segment* segment = leaf->get_segment(segment_id);
            REQUIRE(segment->get_state() == Segment::State::FREE);
            REQUIRE(segment->is_sparse());
            REQUIRE(!segment->has_requested_rebalance());
        }
    }

    tx = teseo.start_transaction();
    REQUIRE(tx.num_vertices() == cardinality);
    REQUIRE(tx.num_edges() == 0);
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }
    for(uint64_t vertex_id = 85; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }

    //memstore->dump();
}

/**
 * Perform 2 splits:
 * - The first split creates three leaves A, B, C
 * - The second split is from the middle leaf B, we want to validate that the newly created
 *   leaves are properly linked to their existing neighbours A and C
 */
TEST_CASE("rb_split4", "[rebalance]"){
    Teseo teseo;
    Memstore* memstore = global_context()->memstore();
    global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    constexpr uint64_t MAX_VERTEX_ID = 1200;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.commit();

    /**
     * First split
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(0).leaf();
        Segment* segment = leaf->get_segment(0);
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    /**
     * Insert new vertices into the second leaf
     */
    uint64_t vertex_second_split_from, vertex_second_split_to;
    { // find the fence keys of the second leaf
        ScopedEpoch epoch;
        Leaf* leaf1 = memstore->index()->find(0).leaf();
        REQUIRE(leaf1->get_hfkey() != KEY_MAX); // otherwise there isn't a second leaf
        Leaf* leaf2 = memstore->index()->find(leaf1->get_hfkey().source(), leaf1->get_hfkey().destination()).leaf();
        REQUIRE(leaf1 != leaf2);
        vertex_second_split_from = leaf2->get_lfkey().source() -1; // we already knew that, as it's the same as leaf1->get_hfkey()
        vertex_second_split_to = leaf2->get_hfkey().source() -1; // 291 -> 290
    }
    tx = teseo.start_transaction();
    for(uint64_t vertex_id = vertex_second_split_from +5; vertex_id <= vertex_second_split_to; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.commit();

    //memstore->dump();

    /**
     * Second split
     */
    {
        ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = memstore->index()->find(vertex_second_split_from +1).leaf();
        Segment* segment = leaf->get_segment(3); // 3 for a change, it shouldn't make any difference
        segment->set_flag_rebal_requested();
        Crawler crawler { context, segment->m_fence_key };
        Plan plan = crawler.make_plan();
        REQUIRE(plan.is_split());
        ScratchPad scratchpad { plan.cardinality() };
        SpreadOperator rebalance { context, scratchpad, plan };
        rebalance();
    }

    //memstore->dump();

    /**
     * Validate all vertices are present
     */
    tx = teseo.start_transaction();
    REQUIRE(tx.num_edges() == 0);
    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }
    for(uint64_t vertex_id = vertex_second_split_from +5; vertex_id <= vertex_second_split_to; vertex_id += 10){
        REQUIRE(tx.has_vertex(vertex_id));
    }
}

/**
 * Insert 2000 vertices, in ascending order 10, 20, ...
 * Let the async rebalancers do the dirty work
 */
TEST_CASE("rb_async1", "[rebalance]"){
    Teseo teseo;
    constexpr uint64_t MAX_VERTEX_ID = 20000;

    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_vertex(vertex_id) == false);
        tx.insert_vertex(vertex_id);
        REQUIRE(tx.has_vertex(vertex_id) == true);
        tx.commit();
        this_thread::sleep_for(1ms); // give time to the async rebalancer to pick up
    }

    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_vertex(vertex_id) == true);
        tx.commit();
    }
}

/**
 * Insert 2000 vertices, in descending order 20.000, 19.990, 19.980, ...
 * Let the async rebalancers do the dirty work
 */
TEST_CASE("rb_async2", "[rebalance]"){
    Teseo teseo;
    constexpr uint64_t MAX_VERTEX_ID = 20000;

    for(uint64_t vertex_id = MAX_VERTEX_ID; vertex_id >= 10; vertex_id -= 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_vertex(vertex_id) == false);
        tx.insert_vertex(vertex_id);
        REQUIRE(tx.has_vertex(vertex_id) == true);
        tx.commit();
        this_thread::sleep_for(1ms); // give time to the async rebalancer to pick up
    }

    for(uint64_t vertex_id = 10; vertex_id <= MAX_VERTEX_ID; vertex_id += 10){
        auto tx = teseo.start_transaction();
        REQUIRE(tx.has_vertex(vertex_id) == true);
        tx.commit();
    }
}
