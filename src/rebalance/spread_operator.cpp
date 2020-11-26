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

#include "teseo/rebalance/spread_operator.hpp"

#include <cassert>
#include <cmath>
#include <vector>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/index_entry.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/vertex_table.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/scoped_leaf_lock.hpp"
#include "teseo/rebalance/scratchpad.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::rebalance {

/*****************************************************************************
 *                                                                           *
 *  Interface                                                                *
 *                                                                           *
 *****************************************************************************/

SpreadOperator::SpreadOperator(const memstore::Context& context, ScratchPad& scratchpad, Plan& plan, RebalancedLeaves* out_rebalanced_leaves) :
    m_context(context), m_scratchpad(scratchpad), m_plan(plan), m_rebalanced_leaves(out_rebalanced_leaves), m_profiler(plan) {
    assert(m_context.m_tree != nullptr && "The memstore should be at least set");
    COUT_DEBUG("plan: " << plan);
}

void SpreadOperator::operator() (){
    load();
    prune();
    tune_plan();
    save();
}


/*****************************************************************************
 *                                                                           *
 *  Load                                                                     *
 *                                                                           *
 *****************************************************************************/
void SpreadOperator::load(){
    [[maybe_unused]] auto prof0 = m_profiler.profile_load_time();
    assert(m_cleanup_list.empty() && "There should be no leaves to unlink at the start of a spread operation");

    m_scratchpad.clear();
    m_scratchpad.ensure_capacity(m_plan.cardinality_ub());

    memstore::Leaf* leaf = m_plan.first_leaf();
    load(leaf); // load the elements in the first leaf

    while(leaf != m_plan.last_leaf()){
        assert(m_plan.is_merge() && "Only merge plans (currently) contain more than one leaf");

        // jump to the next leaf
        memstore::Key hfkey = leaf->get_hfkey();
        memstore::IndexEntry entry = m_context.m_tree->index()->find(hfkey.source(), hfkey.destination());
        assert(entry.segment_id() == 0 && "As we're fetching the leaf low fence key, the first segment must be 0");
        assert(entry.leaf() != leaf && "Infinite loop");

        leaf = entry.leaf();
        load(leaf);

        // only unlink the leaf in the #save phase to decrease the chance of other threads constantly obtaining invalid entries in the index
        m_cleanup_list.push_back(leaf);
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
}

void SpreadOperator::load(memstore::Leaf* leaf){
    assert(leaf != nullptr && "The current leaf is null");

    uint64_t window_start = m_plan.window_start();
    uint64_t window_end = m_plan.window_end();

    // if it's a resize, that is we are going to create new leaves, then load the elements from all segments
    if(m_plan.is_resize() || m_plan.is_merge()){
        window_start = 0;
        window_end = leaf->num_segments();
    }

    load(leaf, window_start, window_end);
}

void SpreadOperator::load(memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end) {
    memstore::Index* index = m_context.m_tree->index();
    m_context.m_leaf = leaf;

    for(uint64_t segment_id = window_start; segment_id < window_end; segment_id ++ ){
        memstore::Segment* segment = leaf->get_segment(segment_id);
        m_context.m_segment = segment;
        if(segment->is_unindexed(m_context)) continue; // empty & unindexed, ignore it

        if(segment_id > window_start){
            memstore::Key fence_key = segment->get_lfkey(m_context);
            COUT_DEBUG("[remove fence key] leaf: " << m_context.m_leaf << ", segment_id: " << segment_id << ", used space: " << segment->used_space() << ", fence keys: [" << fence_key << ", " << segment->get_hfkey(m_context) << ")");
            index->remove(fence_key.source(), fence_key.destination());
        }

        segment->load(m_context, m_scratchpad);
    }
}

/*****************************************************************************
 *                                                                           *
 *  Prune                                                                    *
 *                                                                           *
 *****************************************************************************/
void SpreadOperator::prune(){
    using namespace memstore;
    [[maybe_unused]] auto prof0 = m_profiler.profile_prune_time(/* start immediately the timer ? */ true);

    uint64_t pos = 0;
    uint64_t cardinality = m_scratchpad.size();
    uint64_t num_elts_removed = 0;

    while(pos < cardinality){
        m_scratchpad.shift_back(pos, num_elts_removed);
        uint64_t vertex_pos = pos - num_elts_removed;
        Vertex* vertex = m_scratchpad.get_vertex(vertex_pos);

        m_profiler.incr_count_in_num_vertices();
        m_profiler.incr_count_out_num_vertices();
        m_profiler.incr_count_in_num_edges(vertex->m_count);
        m_profiler.incr_count_out_num_edges(vertex->m_count);
        m_profiler.incr_count_in_num_qwords(/* vertex */ 1ull * OFFSET_VERTEX + /* its edges */ vertex->m_count * OFFSET_EDGE);

        // prune its outgoing edges first
        pos++;
        for(uint64_t i = 0, end = vertex->m_count; i < end; i++){
            m_scratchpad.shift_back(pos, num_elts_removed);
            uint64_t edge_pos = pos - num_elts_removed;

            if(m_scratchpad.has_version(edge_pos)){
                m_profiler.incr_count_in_num_qwords(OFFSET_VERSION);
                Version* version = m_scratchpad.get_version(edge_pos);
                version->prune();
                if(version->get_undo() != nullptr){
                    m_space_required += OFFSET_EDGE + OFFSET_VERSION;
                } else if (version->is_insert()){
                    m_scratchpad.unset_version(edge_pos);
                    m_space_required += OFFSET_EDGE;
                } else {
                    assert(version->is_remove());
                    m_profiler.incr_count_out_num_elts( -1 );
                    m_profiler.incr_count_out_num_edges( - 1 );
                    num_elts_removed ++;
                    assert(vertex->m_count > 0 && "Underflow");
                    vertex->m_count --;

                }
            } else { // no version
                m_space_required += OFFSET_EDGE;
            }

            pos++;
        }

        // prune the vertex now
        if(m_scratchpad.has_version(vertex_pos)){
            m_profiler.incr_count_in_num_qwords(OFFSET_VERSION);
            Version* version = m_scratchpad.get_version(vertex_pos);
            version->prune();
            if(version->get_undo() != nullptr){
                m_space_required += OFFSET_VERTEX + OFFSET_VERSION;
            } else if (version->is_insert()){
                m_scratchpad.unset_version(vertex_pos);
                m_space_required += OFFSET_VERTEX;
            } else { // remove the vertex from the file/scratchpad
                assert(version->is_remove());
                m_profiler.incr_count_out_num_elts( -1 );
                m_profiler.incr_count_out_num_vertices( - 1 );
                assert(vertex->m_count == 0 && "Removing a vertex with dangling edges");
                //m_scratchpad.unset_element(vertex_pos); // rather overwrite the slot with the next elts
                num_elts_removed++;

                // update the vertex table
                m_context.m_tree->vertex_table()->remove(vertex->m_vertex_id);
            }
        } else if(vertex->m_first == false && vertex->m_count == 0){
            // remove empty dummy vertices
            m_profiler.incr_count_out_num_elts( -1 );
            m_profiler.incr_count_out_num_vertices( - 1 );
            num_elts_removed++;
        } else {
            m_space_required += OFFSET_VERTEX;
        }

        // pos++ // <- not needed, already incremented when iterating over the edges
    }

    m_scratchpad.set_size(m_scratchpad.size() - num_elts_removed);

    m_profiler.incr_count_in_num_elts(m_scratchpad.size());
    m_profiler.incr_count_out_num_elts(m_scratchpad.size());
    m_profiler.incr_count_out_num_qwords(m_space_required);
}

void SpreadOperator::tune_plan(){
    if(m_plan.is_resize()){
        // recompute the optimal number of segments, after pruning
        uint64_t num_segments = ceil( static_cast<double>(m_space_required) / (0.75 * memstore::SparseFile::max_num_qwords()) );

        if(num_segments <= m_plan.num_output_segments()){
            // can we store all segments in the first leaf of the sequence -> transform it to a rebalance
            if(num_segments <= m_plan.first_leaf()->num_segments()){ // m_plan.is_resize() would be redundant here
                m_plan.set_resize(false);
            }
            assert((!m_plan.is_resize() || num_segments >= context::StaticConfiguration::memstore_max_num_segments_per_leaf /2) &&
                    "Because every leaf has at least max_num_segments/2 segments");

#if defined(DEBUG)
            if(num_segments < m_plan.num_output_segments()){
                COUT_DEBUG("change number of output segments: " << m_plan.num_output_segments() << " -> " << num_segments <<
                        " (" << ceil(static_cast<double>(num_segments)/context::StaticConfiguration::memstore_max_num_segments_per_leaf) << " leaves)");
            }
#endif

            // reset the number of output segments
            m_plan.set_num_output_segments(num_segments);
            m_profiler.set_window_length(num_segments);
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *  Save                                                                     *
 *                                                                           *
 *****************************************************************************/
void SpreadOperator::save(){
    [[maybe_unused]] auto prof0 = m_profiler.profile_write_time();

    // state for the method #save()
    uint64_t num_segments_saved = 0;
    uint64_t budget_achieved = 0;
    int64_t pos_vertex = 0;
    int64_t pos_element = 0;

    // deallocate & unindex the leaves we found in the load phase
    for(auto leaf : m_cleanup_list){ unlink(leaf); }
    m_cleanup_list.clear();

    if(!m_plan.is_resize()){
        save(m_plan.leaf(), m_plan.window_start(), m_plan.window_end(), m_plan.num_output_segments(), num_segments_saved, budget_achieved, pos_vertex, pos_element);
        if(m_plan.is_merge()){ m_plan.first_leaf()->set_hfkey( m_plan.last_leaf()->get_hfkey() ); }
        update_fence_keys(m_plan.leaf(), m_plan.window_start() +1, m_plan.window_end());
    } else { // Resize it into one or multiple leaves
        assert(m_plan.is_resize());

        // We need to ensure that the created leaves are not accessed by other threads (readers or writers) while being constructed
        vector<ScopedLeafLock> xlocks;

        constexpr int64_t MC = context::StaticConfiguration::memstore_max_num_segments_per_leaf;
        int64_t num_segments_to_fill = m_plan.num_output_segments();

        // get the interval for the fence keys
        auto lfkey = m_plan.first_leaf()->get_lfkey();
        auto hfkey = m_plan.last_leaf()->get_hfkey();

        memstore::Leaf* previous = nullptr; // the previous sibling of the current leaf
        memstore::Leaf* current = nullptr; // current leaf being examined
        bool relink_first_leaf = true; // whether we need to re-index the fence key for the first leaf
        uint64_t num_leaves = 0; // iterator count

        while(num_segments_to_fill > 0){
            const bool is_first_leaf = (num_leaves == 0); // whether this the first leaf created in the interval

            // try to create the first N-1 leaves as big as possible (MC), and the last one as small as possible
            int64_t num_segments = num_segments_to_fill; // if num_segments_to_fill <= MC, this is the last iteration
            if(num_segments > MC){
                int64_t next = max(MC/2, num_segments_to_fill - (is_first_leaf ? static_cast<int64_t>(m_plan.first_leaf()->num_segments()) : MC));
                num_segments = num_segments_to_fill - next;
                num_segments_to_fill = next;
            } else {
                num_segments_to_fill = 0; // we're done
            }
            assert(num_segments >= MC/2 && "Every leaf must contain at least MC/2 segments");
            assert(num_segments <= MC && "No leaf can have more than MC segment");

            previous = current;
            if(is_first_leaf && ((uint64_t) num_segments <= m_plan.first_leaf()->num_segments())){ // special case, don't recreate the first leaf
                current = m_plan.first_leaf();
                relink_first_leaf = false;
            } else {
                if(is_first_leaf){ unlink(m_plan.first_leaf()); } // explicitly remove the first leaf of the fat tree

                current = memstore::create_leaf(num_segments);
                COUT_DEBUG("[" << num_leaves << "] create_leaf: " << current << ", num_segments: " << num_segments);
                xlocks.emplace_back(current); // lock the leaf until we're done
            }

            save(current, 0, current->num_segments(), num_segments, num_segments_saved, budget_achieved, pos_vertex, pos_element);

            // update the fence keys of the leaf before the current
            link(num_leaves, lfkey, hfkey, previous, current, relink_first_leaf);

            num_leaves++; // next iteration
        }

        // update the fence keys of the last leaf
        link(num_leaves, lfkey, hfkey, current, nullptr, relink_first_leaf);

        validate_leaf_traversals(lfkey, hfkey);
    }

    assert(m_space_required == budget_achieved && "We didn't copy all data from the buffer");
}


void SpreadOperator::save(memstore::Leaf* leaf, int64_t window_start, int64_t window_end, uint64_t num_filled_segments, uint64_t& num_segments_saved, uint64_t& budget_achieved, int64_t& pos_vertex, int64_t& pos_element){
    assert(window_start >= 0 && "Invalid initial segment");
    assert(window_start < window_end && "be sure the caller provides window_end and not window_length as argument");
    assert(num_filled_segments <= (uint64_t) (window_end - window_start) && "number of segments to fill larger than the available window");

    m_context.m_leaf = leaf;

    // how many empty segments should be placed for each filled one?
    const double empty_per_filled = static_cast<double>(num_filled_segments)/(window_end - window_start) - 1.0;
    double empty_balance = 0;

    // report to the invoker the leaves rebalanced/created
    if(m_rebalanced_leaves != nullptr){ m_rebalanced_leaves->emplace_back(leaf, 0); }

    for(int64_t segment_id = window_start; segment_id < window_end; segment_id ++) {
        memstore::Segment* segment = leaf->get_segment(segment_id);
        m_context.m_segment = segment;

        int64_t target_budget = 0;
        if(empty_balance >= 1.0 || /* due to rounding issues */ num_filled_segments == 0){ // make the segment empty
            // target_budget = 0
            empty_balance -= 1.0;
        } else { // fill the segment
            target_budget = (m_space_required - budget_achieved) / (m_plan.num_output_segments() - num_segments_saved);

            empty_balance += empty_per_filled;
            num_filled_segments--;
            num_segments_saved++;
        }

        int64_t in_budget_achieved = 0;

        segment->save(m_context, m_scratchpad, pos_vertex, pos_element, target_budget, &in_budget_achieved);


        budget_achieved += in_budget_achieved;
        if(m_rebalanced_leaves != nullptr){ m_rebalanced_leaves->back().second += segment->used_space(); } // record the amount of space filled in the window
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
}

void SpreadOperator::update_fence_keys(memstore::Leaf* leaf, int64_t window_start, int64_t window_end){
    COUT_DEBUG("leaf: " << leaf << ", window: [" << window_start << ", " << window_end << "), leaf segments: " << leaf->num_segments());

    memstore::Index* index = m_context.m_tree->index();
    m_context.m_leaf = leaf;

    for(int64_t segment_id = window_end - 1; segment_id >= window_start; segment_id --){
        memstore::Segment* segment = leaf->get_segment(segment_id);
        m_context.m_segment = segment;

        assert(segment->is_sparse() && "As it has just been rebalanced");
        assert(segment->is_xlocked() && "We must hold a xlock on the segment");

        memstore::SparseFile* sf = m_context.sparse_file();

        if(sf->is_empty()){ // this is a mess
            uint64_t next_segment_id = segment_id + 1;
            if(next_segment_id == leaf->num_segments()){
                segment->m_fence_key = leaf->get_hfkey();
            } else {
                segment->m_fence_key = leaf->get_segment(next_segment_id)->m_fence_key;
            }
        } else {
            memstore::Key key = sf->get_minimum();
            segment->m_fence_key = key;

            memstore::IndexEntry entry { leaf, (uint64_t) segment_id };
            index->insert(key.source(), key.destination(), entry);
        }

        COUT_DEBUG("segment_id: " << segment_id << ", min fence key: " << segment->m_fence_key << (sf->is_empty() ? " (empty segment)" : ""));
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
}

void SpreadOperator::link(uint64_t position /* in [1, num_leaves] */, const memstore::Key& interval_lfkey, const memstore::Key& interval_hfkey, memstore::Leaf* previous, memstore::Leaf* current, bool relink_first_leaf) {
    if(position < 1) return; // nop
    const bool is_first_leaf = (position == 1);
    const bool is_last_leaf = (current == nullptr);

    memstore::Key hfkey = is_last_leaf ? interval_hfkey : memstore::Context::sparse_file(current, 0)->get_minimum();

#if defined(DEBUG)
    if(is_last_leaf){
        COUT_DEBUG("[Leaf #" << (position -1) << "] Link " << previous << " to <existing next> via " << hfkey);
    } else {
        COUT_DEBUG("[Leaf #" << (position -1) << "] Link " << previous << " to " << current << " via " << hfkey);
    }
#endif

    previous->set_hfkey( hfkey );
    uint64_t window_start = 0 + is_first_leaf; // for the first leaf, start from segment 1, for the others from segment 0
    update_fence_keys(previous, window_start, previous->num_segments());

    if(is_first_leaf && relink_first_leaf){
        previous->set_lfkey(interval_lfkey);
        auto index = m_context.m_tree->index();
        memstore::IndexEntry entry { previous, /* first segment */ 0 };
        index->insert(interval_lfkey.source(), interval_lfkey.destination(), entry);
    }
}

void SpreadOperator::unlink(memstore::Leaf* leaf){
    COUT_DEBUG("Leaf: " << leaf);

    // do not try to remove the first leaf of the fat tree
    assert(! leaf->is_first() && "Attempting to deallocate the first leaf of the fat tree");

    memstore::Key lfkey = leaf->get_lfkey();
    m_context.m_tree->index()->remove(lfkey.source(), lfkey.destination());
    leaf->decr_ref_count();
}

/*****************************************************************************
 *                                                                           *
 *  Validate                                                                 *
 *                                                                           *
 *****************************************************************************/

void SpreadOperator::validate_leaf_traversals(memstore::Key lfkey, memstore::Key hfkey) {
#if !defined(NDEBUG)
    if(!m_plan.is_resize()){ // just check that the leaf is reachable
        auto entry = m_context.m_tree->index()->find(lfkey.source(), lfkey.destination());
        assert(entry.leaf() == m_plan.first_leaf() && "The leaf rebalanced is not reachable from the index");
        assert(entry.leaf()->ref_count() > 0 && "The leaf rebalanced has been deallocated");
        assert(entry.segment_id() == 0 && "The index entry does not link to the first segment of the leaf rebalanced");
    } else if (m_rebalanced_leaves != nullptr){ // we must know the leaves created
        assert(m_plan.is_resize() && "Due to the top guard in the if stmt");

        for(uint64_t i = 0; i < m_rebalanced_leaves->size(); i++){
            auto expected = m_rebalanced_leaves->at(i);
            auto entry = m_context.m_tree->index()->find(lfkey.source(), lfkey.destination());
            assert(entry.leaf() == expected.first && "Cannot retrieve the expected leaf");
            assert(entry.segment_id() == 0 && "The index should always point to the first segment in a leaf traversal");
            assert(entry.leaf()->ref_count() > 0 && "The ref count for the leaf is already zero?");

            auto next = entry.leaf()->get_hfkey();
            bool is_last = (i +1 == m_rebalanced_leaves->size());
            if(is_last){
                assert(next == hfkey && "The last leaf of the resized interval is not linked to the next leaf");
            } else { // next iteration
                lfkey = next;
            }
        }
    }
#endif
}

} // namespace


