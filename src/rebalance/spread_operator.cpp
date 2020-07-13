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

SpreadOperator::SpreadOperator(const memstore::Context& context, ScratchPad& scratchpad, const Plan& plan) :
    m_context(context), m_scratchpad(scratchpad), m_plan(plan), m_profiler(plan) {
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

    m_scratchpad.clear();
    m_scratchpad.ensure_capacity(m_plan.cardinality_ub());

    if(m_plan.is_spread() || m_plan.is_split()){
        load(m_plan.leaf(), m_plan.window_start(), m_plan.window_end());
    } else {
        assert(m_plan.is_merge());
        memstore::Leaf* start = m_plan.first_leaf();
        memstore::Leaf* end = m_plan.last_leaf();
        assert(start != end);
        memstore::Leaf* next = start;
        do {
            memstore::Leaf* leaf = next;
            load(leaf, 0, leaf->num_segments());

            // next iteration
            memstore::Key hfkey = leaf->get_hfkey();
            if(leaf != end){
                memstore::IndexEntry entry = m_context.m_tree->index()->find(hfkey.source(), hfkey.destination());
                assert(entry.segment_id() == 0 && "As we're fetching the leaf fence key, the first segment must be 0");
                assert(entry.leaf() != leaf && "Infinite loop");
                next = entry.leaf();
            } else {
                start->set_hfkey(hfkey); // copy the high fence key to the first leaf
                next = nullptr;
            }

            // deallocate the leaf
            if(leaf != start){
                memstore::Key lfkey = leaf->get_lfkey();
                m_context.m_tree->index()->remove(lfkey.source(), lfkey.destination());
                //m_context.m_tree->global_context()->gc()->mark(leaf, memstore::destroy_leaf );
                //context::thread_context()->gc_mark(leaf, (void (*)(void*)) memstore::destroy_leaf);
                leaf->decr_ref_count();
            }

        } while(next != nullptr);

    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
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
            //COUT_DEBUG("[remove fence key] leaf: " << m_context.m_leaf << ", segment_id: " << segment_id << ", used space: " << segment->used_space() << ", fence keys: [" << fence_key << ", " << segment->get_hfkey(m_context) << ")");
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
        m_profiler.incr_count_in_num_qwords((/* vertex */ 1ull + /* its edges */ vertex->m_count) * OFFSET_ELEMENT);

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
                    m_space_required += OFFSET_ELEMENT + OFFSET_VERSION;
                } else if (version->is_insert()){
                    m_scratchpad.unset_version(edge_pos);
                    m_space_required += OFFSET_ELEMENT;
                } else {
                    assert(version->is_remove());
                    m_profiler.incr_count_out_num_elts( -1 );
                    m_profiler.incr_count_out_num_edges( - 1 );
                    num_elts_removed ++;
                    assert(vertex->m_count > 0 && "Underflow");
                    vertex->m_count --;

                }
            } else { // no version
                m_space_required += OFFSET_ELEMENT;
            }

            pos++;
        }

        // prune the vertex now
        if(m_scratchpad.has_version(vertex_pos)){
            m_profiler.incr_count_in_num_qwords(OFFSET_VERSION);
            Version* version = m_scratchpad.get_version(vertex_pos);
            version->prune();
            if(version->get_undo() != nullptr){
                m_space_required += OFFSET_ELEMENT + OFFSET_VERSION;
            } else if (version->is_insert()){
                m_scratchpad.unset_version(vertex_pos);
                m_space_required += OFFSET_ELEMENT;
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
            m_space_required += OFFSET_ELEMENT;
        }

        // pos++ // <- not needed, already incremented when iterating over the edges
    }

    m_scratchpad.set_size(m_scratchpad.size() - num_elts_removed);

    m_profiler.incr_count_in_num_elts(m_scratchpad.size());
    m_profiler.incr_count_out_num_elts(m_scratchpad.size());
    m_profiler.incr_count_out_num_qwords(m_space_required);
}

void SpreadOperator::tune_plan(){
    if(m_plan.is_split()){
        double ideal_number_segments_dbl = static_cast<double>(m_space_required) / (0.75 * memstore::SparseFile::max_num_qwords());

        // In test mode, segments & leaves are very small, round up just to be sure we always have enough room to restore all the elements
        uint64_t ideal_number_segments = !context::StaticConfiguration::test_mode ? floor(ideal_number_segments_dbl) : ceil(ideal_number_segments_dbl);

        if(ideal_number_segments != m_plan.num_output_segments()){
            uint64_t num_segments = max<uint64_t>(context::StaticConfiguration::memstore_num_segments_per_leaf, ideal_number_segments);
            COUT_DEBUG("change number of segments: " << m_plan.num_output_segments() << " -> " << num_segments <<
                    " (" << ceil(static_cast<double>(num_segments)/context::StaticConfiguration::memstore_num_segments_per_leaf) << " leaves)");
            m_plan.set_num_output_segments(num_segments);
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
    const uint64_t num_output_segments = m_plan.num_output_segments();
    uint64_t num_segments_saved = 0;
    uint64_t budget_achieved = 0;
    int64_t pos_vertex = 0;
    int64_t pos_element = 0;

    if(m_plan.is_spread() || m_plan.is_merge()){
        // don't use m_plan.window_end(), in a merge it's larger than one leaf
        save(m_plan.leaf(), m_plan.window_start(), m_plan.window_start() + num_output_segments, num_output_segments, num_segments_saved, budget_achieved, pos_vertex, pos_element);
        update_fence_keys(m_plan.leaf(), m_plan.window_start() +1, m_plan.window_start() + num_output_segments);

    } else { // Split into multiple leaves
        assert(m_plan.is_split());
        assert(m_plan.num_output_segments() > context::StaticConfiguration::memstore_num_segments_per_leaf && "As this is a split");
        const int64_t num_leaves = ceil(static_cast<double>(m_plan.num_output_segments()) / context::StaticConfiguration::memstore_num_segments_per_leaf);
        const int64_t num_segments_per_leaf = m_plan.num_output_segments() / num_leaves;
        const int64_t num_bigger_leaves = m_plan.num_output_segments() % num_leaves;

        // Let's start with the first leaf
        memstore::Leaf* parent = nullptr;
        memstore::Leaf* leaf = m_plan.leaf();
        auto hfkey = leaf->get_hfkey();

        for(int64_t i = 0; i < num_leaves; i++){
            if(i > 0){ // the first leaf is already set
                parent = leaf;
                leaf = memstore::create_leaf();
            }

            int64_t num_filled_segments = num_segments_per_leaf + (i < num_bigger_leaves);
            save(leaf, 0, leaf->num_segments(), num_filled_segments, num_segments_saved, budget_achieved, pos_vertex, pos_element);

            if(parent != nullptr){ // update the fence keys of the parent
                auto hfkey = memstore::Context::sparse_file(leaf, 0)->get_minimum();
                COUT_DEBUG("Link " << parent << " to " << leaf << " via " << hfkey);
                parent->set_hfkey( hfkey );
                int64_t parent_window_start = 0 + (parent == m_plan.leaf());
                update_fence_keys(parent, parent_window_start, parent->num_segments());
            }
        }

        // update the fence keys of the last leaf
        COUT_DEBUG("Link " << leaf << " to <existing next> via " << hfkey);
        leaf->set_hfkey( hfkey );
        update_fence_keys(leaf, 0, leaf->num_segments());
    }

    assert(m_space_required == budget_achieved && "We didn't copy all data from the buffer");
}


void SpreadOperator::save(memstore::Leaf* leaf, int64_t window_start, int64_t window_end, uint64_t num_filled_segments, uint64_t& num_segments_saved, uint64_t& budget_achieved, int64_t& pos_vertex, int64_t& pos_element){
    m_context.m_leaf = leaf;

    // how many empty segments should be placed for each filled one?
    const double empty_per_filled = static_cast<double>(num_filled_segments)/(window_end - window_start) - 1.0;
    double empty_balance = 0;

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
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
}

void SpreadOperator::update_fence_keys(memstore::Leaf* leaf, int64_t window_start, int64_t window_end){
    memstore::Index* index = m_context.m_tree->index();
    m_context.m_leaf = leaf;

    for(int64_t segment_id = window_end - 1; segment_id >= window_start; segment_id --){
        memstore::Segment* segment = leaf->get_segment(segment_id);
        m_context.m_segment = segment;

        assert(segment->is_sparse() && "As it has just been rebalanced");
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
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
}

} // namespace


