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
#include "teseo/rebalance/scratchpad.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

// [DEBUG] Set this macro to validate the content of the scratchpad after its content has been loaded from the segments
#define VALIDATE_LOAD

// [DEBUG] Set this macro to validate the content of the scratchpad after old versions have been pruned
#define VALIDATE_PRUNE

// [DEBUG] Set this macro to check the leaves are linked together after the involved leaves have been rebalanced
#define VALIDATE_LEAF_TRAVERSALS

// [DEBUG] Set this macro to validate the content of the filled segments after the content from the scratchpad has been copied back
#define VALIDATE_SAVE

using namespace std;

namespace teseo::rebalance {

/*****************************************************************************
 *                                                                           *
 *   Debug macros                                                            *
 *                                                                           *
 *****************************************************************************/
// def the macro DEBUG_LOAD( x ) as ( x )
#if !defined(VALIDATE_LOAD)
#define DEBUG_LOAD( stuff )
#elif defined(VALIDATE_LOAD) && defined(NDEBUG)
#warning "[Teseo] Setting VALIDATE_LOAD ignored because the macro NDEBUG is defined"
#define DEBUG_LOAD( stuff )
#else // setting defined, ndebug is not defined
#define DEBUG_LOAD( stuff ) stuff
#endif

// def the macro DEBUG_PRUNE( x ) as ( x )
#if !defined(VALIDATE_PRUNE)
#define DEBUG_PRUNE( stuff )
#elif defined(VALIDATE_PRUNE) && defined(NDEBUG)
#warning "[Teseo] Setting VALIDATE_PRUNE ignored because the macro NDEBUG is defined"
#define DEBUG_PRUNE( stuff )
#else // validate_prune is defined, ndebug is not defined
#define DEBUG_PRUNE( stuff ) stuff
#endif

// def the macro DEBUG_SAVE( x ) as ( x )
#if !defined(VALIDATE_LEAF_TRAVERSALS)
#define DEBUG_LEAF_TRAVERSALS( stuff )
#elif defined(VALIDATE_LEAF_TRAVERSALS) && defined(NDEBUG)
#warning "[Teseo] Setting VALIDATE_LEAF_TRAVERSALS ignored because the macro NDEBUG is defined"
#define DEBUG_LEAF_TRAVERSALS( stuff )
#else // setting defined, ndebug is not defined
#define DEBUG_LEAF_TRAVERSALS( stuff ) stuff
#endif

// def the macro DEBUG_SAVE( x ) as ( x )
#if !defined(VALIDATE_SAVE)
#define DEBUG_SAVE( stuff )
#elif defined(VALIDATE_SAVE) && defined(NDEBUG)
#warning "[Teseo] Setting VALIDATE_SAVE ignored because the macro NDEBUG is defined"
#define DEBUG_SAVE( stuff )
#else // setting defined, ndebug is not defined
#define DEBUG_SAVE( stuff ) stuff
#endif

/*****************************************************************************
 *                                                                           *
 *  Interface                                                                *
 *                                                                           *
 *****************************************************************************/

SpreadOperator::SpreadOperator(const memstore::Context& context, ScratchPad& scratchpad, Plan& plan) :
    m_context(context), m_scratchpad(scratchpad), m_plan(plan), m_profiler(plan) {
    assert(m_context.m_tree != nullptr && "The memstore should be at least set");
    COUT_DEBUG("plan: " << plan);
}

SpreadOperator::~SpreadOperator(){
    // Release all acquired locks
    for(auto& item : m_rebalanced_leaves){
        memstore::Leaf* leaf = item.leaf();
        const bool delete_leaf = item.is_removed();

        for(uint64_t segment_id = item.window_start(); segment_id < item.window_end(); segment_id++){
            memstore::Segment* segment = leaf->get_segment(segment_id);
            segment->async_rebalancer_exit(/* invalidate ? */ delete_leaf);
        }

        if(delete_leaf){ leaf->decr_ref_count(); } // mark for GC
    }
}

void SpreadOperator::operator() (){
    load();
    prune();
    tune_plan();
    save();
}

memstore::Leaf* SpreadOperator::last_leaf() const {
    for(int64_t i = static_cast<int64_t>(m_rebalanced_leaves.size()) -1; i >= 0; i--){
        auto& item = m_rebalanced_leaves[i];
        if(item.is_removed()) continue;
        return item.leaf();
    }

    assert(false && "There are no leaves in this interval ?");
    return nullptr;
}

/*****************************************************************************
 *                                                                           *
 *  Load                                                                     *
 *                                                                           *
 *****************************************************************************/
void SpreadOperator::load(){
    [[maybe_unused]] auto prof0 = m_profiler.profile_load_time();
    assert(m_rebalanced_leaves.empty() && "There should be no leaves to unlink at the start of a spread operation");

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

        // we're going to remove this leaf at the end of the rebalance
        m_rebalanced_leaves.back().set_removed();
    }

    DEBUG_LOAD( validate_load() );
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
    m_rebalanced_leaves.emplace_back(leaf, window_start, window_end);
    m_rebalanced_leaves.back().set_existent();
}

void SpreadOperator::load(memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end) {
    m_context.m_leaf = leaf;

    for(uint64_t segment_id = window_start; segment_id < window_end; segment_id ++ ){
        memstore::Segment* segment = leaf->get_segment(segment_id);
        m_context.m_segment = segment;
        if(segment->is_unindexed(m_context)) continue; // empty & unindexed, ignore it
        segment->load(m_context, m_scratchpad);
    }

    m_context.m_segment = nullptr;
    m_context.m_leaf = nullptr;
}

/*****************************************************************************
 *                                                                           *
 *  Prune                                                                    *
 *                                                                           *
 *****************************************************************************/
void SpreadOperator::prune(){
    using namespace memstore;
    [[maybe_unused]] auto prof0 = m_profiler.profile_prune_time(/* start immediately the timer ? */ true);
    DEBUG_PRUNE( ScratchPad copy ( m_scratchpad ) );

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

    DEBUG_PRUNE( validate_prune(copy) );

    m_profiler.incr_count_in_num_elts(m_scratchpad.size());
    m_profiler.incr_count_out_num_elts(m_scratchpad.size());
    m_profiler.incr_count_out_num_qwords(m_space_required);
}

void SpreadOperator::tune_plan(){
    using namespace memstore;

    if(m_plan.is_resize()){
        // Recompute the optimal number of segments, after pruning.
        // In case of a test build, segments are very small (31 qwords as of today, 18/12/2020). Add a bit of more gaps
        // to account for dummy vertices or vertices carried over to the next segment
        double extra_space_required = m_space_required * (1.0 + static_cast<double>(OFFSET_VERTEX + OFFSET_VERSION + OFFSET_EDGE) / SparseFile::max_num_qwords());
        uint64_t num_segments = ceil( extra_space_required / (0.75 * SparseFile::max_num_qwords()) );

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

    if(!m_plan.is_resize()){
        save(m_plan.leaf(), m_plan.window_start(), m_plan.window_end(), m_plan.num_output_segments(), num_segments_saved, budget_achieved, pos_vertex, pos_element);
    } else { // Resize it into one or multiple leaves
        assert(m_plan.is_resize());

        constexpr int64_t MC = context::StaticConfiguration::memstore_max_num_segments_per_leaf;
        int64_t num_segments_to_fill = m_plan.num_output_segments();
        bool is_first_leaf = true;

        while(num_segments_to_fill > 0){
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

            memstore::Leaf* leaf = nullptr;
            if(is_first_leaf && ((uint64_t) num_segments <= m_plan.first_leaf()->num_segments())){ // special case, don't recreate the first leaf
                leaf = m_plan.first_leaf();
            } else {
                if(is_first_leaf){ m_rebalanced_leaves[0].set_removed(); } // explicitly delete the first leaf of the plan
                leaf = create_leaf(num_segments);
            }

            save(leaf, 0, leaf->num_segments(), num_segments, num_segments_saved, budget_achieved, pos_vertex, pos_element);

            is_first_leaf = false; // next iteration
        }
    }
    assert(m_space_required == budget_achieved && "We didn't copy all data from the buffer");

    update_fence_keys();
    DEBUG_LEAF_TRAVERSALS( validate_leaf_traversals() );
    DEBUG_SAVE( validate_save() );
}

void SpreadOperator::save(memstore::Leaf* leaf, int64_t window_start, int64_t window_end, uint64_t num_filled_segments, uint64_t& num_segments_saved, uint64_t& budget_achieved, int64_t& pos_vertex, int64_t& pos_element){
    assert(window_start >= 0 && "Invalid initial segment");
    assert(window_start < window_end && "be sure the caller provides window_end and not window_length as argument");
    assert(num_filled_segments <= (uint64_t) (window_end - window_start) && "number of segments to fill larger than the available window");

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

void SpreadOperator::update_fence_keys(){
    memstore::Index* index = m_context.m_tree->index();

    // The lowest fence key of the interval must be preserved, so that the segment/leaf can be still linked with the segment
    // that precedes the interval. Same for the highest fence key.
    memstore::Key lfkey = m_rebalanced_leaves[0].leaf()->get_segment(m_rebalanced_leaves[0].window_start())->m_fence_key;
    memstore::Key hfkey = memstore::KEY_MAX;
    memstore::Leaf* first_leaf = nullptr;
    int64_t first_segment_id = m_plan.window_start();
    assert((first_segment_id == 0 || !m_plan.is_resize()) && "Only rebalances start can start with a segment ID != 0");

    // We're going to perform two passes: first we remove from the index all fence keys from the existing leaves, and, second, we add
    // again in the index the fence keys of the remaining leaves

    // First pass, remove the existing search keys from the index
    for(uint64_t i = 0; i < m_rebalanced_leaves.size(); i++){
        auto& item = m_rebalanced_leaves[i];
        if(first_leaf == nullptr && !item.is_removed()) { first_leaf = item.leaf(); }
        if(item.is_created()) continue; // this is a new leaf => it doesn't have search keys in the index

        memstore::Leaf* leaf = m_context.m_leaf = item.leaf();
        for(uint64_t segment_id = item.window_start(); segment_id < item.window_end(); segment_id++){
            memstore::Segment* segment = m_context.m_segment = leaf->get_segment(segment_id);
            hfkey = memstore::Segment::get_hfkey(m_context); // keep track of the max fence key

            if(segment->is_unindexed(m_context)) continue; // empty segments do not have a search key in the index

            memstore::Key search_key = segment->get_lfkey(m_context);
            if(search_key == memstore::KEY_MIN) { // never remove the mimimum search key of the fat tree
                assert(lfkey == memstore::KEY_MIN && "This is the minimum fence key");
                assert(!item.is_removed() && "Never remove the very first leaf of the fence key");
                assert(segment_id == 0 && "The minimum of the fat tree can only refer to segment 0");
                continue;
            }

            index->remove(search_key.source(), search_key.destination());
            //COUT_DEBUG("[remove fence key] leaf: " << m_context.m_leaf << (item.is_removed() ?" (deleted)" : "") << ", segment_id: " << segment_id << ", fence keys: [" << segment->get_lfkey(m_context) << ", " << segment->get_hfkey(m_context) << ")");
        }
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;

    // Second pass, update the fence keys for the surviving leaves
    for(int64_t i = m_rebalanced_leaves.size() -1; i >= 0; i--){
        auto& item = m_rebalanced_leaves[i];
        if(item.is_removed()) continue; // this leaf is going to deleted

        memstore::Leaf* leaf = m_context.m_leaf = item.leaf();
        int64_t window_start = item.window_start();
        int64_t window_end = item.window_end();

        // reset the max fence key for this leaf
        if(leaf->num_segments() == (uint64_t) window_end){ leaf->set_hfkey(hfkey); }

        for(int64_t segment_id = window_end -1; segment_id >= window_start; segment_id --){
            memstore::Segment* segment = m_context.m_segment = leaf->get_segment(segment_id);

            assert(segment->is_sparse() && "As it has just been rebalanced");
            assert(segment->is_xlocked() && "We must hold an xlock on the segment");
            assert(hfkey != memstore::KEY_MIN && "The local max fence key can never be the global min fence key of the fat tree"); // corner case

            memstore::SparseFile* sf = m_context.sparse_file();

            if(leaf == first_leaf && segment_id == first_segment_id && /* corner case, this key has already been indexed */ lfkey != hfkey){ // reset the min fence key of the interval
                if(lfkey != memstore::KEY_MIN){ // we never remove the min fence key of the fat tree
                    segment->m_fence_key = lfkey;
                    memstore::IndexEntry entry { leaf, (uint64_t) segment_id };
                    index->insert(lfkey.source(), lfkey.destination(), entry);

                    //COUT_DEBUG("[set fence key] leaf: " << leaf << (item.is_created() ?" (new)" : "") << ", segment_id: " << segment_id << ", fence keys: [" << segment->get_lfkey(m_context) << ", " << segment->get_hfkey(m_context) << "), first segment");
                }
            } else if(sf->is_empty()){ // do not index this segment
                segment->m_fence_key = hfkey;

                //COUT_DEBUG("[set fence key] leaf: " << leaf << (item.is_created() ?" (new)" : "") << ", segment_id: " << segment_id << ", fence keys: [" << segment->get_lfkey(m_context) << ", " << segment->get_hfkey(m_context) << "), empty segment");
            } else {
                memstore::Key lfkey = sf->get_minimum();
                segment->m_fence_key = lfkey;

                memstore::IndexEntry entry { leaf, (uint64_t) segment_id };
                index->insert(lfkey.source(), lfkey.destination(), entry);

                // next iteration
                hfkey = lfkey;

                //COUT_DEBUG("[set fence key] leaf: " << leaf << (item.is_created() ?" (new)" : "") << ", segment_id: " << segment_id << ", fence keys: [" << segment->get_lfkey(m_context) << ", " << segment->get_hfkey(m_context) << "), regular segment");
            }
        }
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
}

memstore::Leaf* SpreadOperator::create_leaf(uint64_t num_segments){
    memstore::Leaf* leaf = memstore::create_leaf(num_segments);
    COUT_DEBUG("leaf: " << leaf << ", num_segments: " << num_segments);

    for(uint64_t i = 0, end = leaf->num_segments(); i < end; i++){
        memstore::Segment* segment = leaf->get_segment(i);
        segment->async_rebalancer_init_xlock();
    }

    m_rebalanced_leaves.emplace_back(leaf);
    m_rebalanced_leaves.back().set_created();

    return leaf;
}

/*****************************************************************************
 *                                                                           *
 *  Validate                                                                 *
 *                                                                           *
 *****************************************************************************/
void SpreadOperator::validate_load() const {
#if !defined(DEBUG)
    memstore::Context context { m_context.m_tree };
    int64_t pos_next_vertex = 0;
    int64_t pos_next_element = 0;

    // first leaf
    memstore::Leaf* leaf = context.m_leaf = m_plan.first_leaf();
    uint64_t window_start = (m_plan.is_merge() || m_plan.is_resize()) ? 0 : m_plan.window_start();
    uint64_t window_end = (m_plan.is_merge() || m_plan.is_resize()) ? leaf->num_segments() : m_plan.window_end();
    for(uint64_t segment_id = window_start; segment_id < window_end; segment_id++){
        context.m_segment = leaf->get_segment(segment_id);
        memstore::Segment::validate_scratchpad(context, m_scratchpad, pos_next_vertex, pos_next_element);
    }

    // validate the remaining loaded leaves (merge only)
    while(leaf != m_plan.last_leaf()){
        assert(m_plan.is_merge() && "Only merge plans (currently) contain more than one leaf");

        // jump to the next leaf
        memstore::Key hfkey = leaf->get_hfkey();
        memstore::IndexEntry entry = m_context.m_tree->index()->find(hfkey.source(), hfkey.destination());
        assert(entry.segment_id() == 0 && "As we're fetching the leaf low fence key, the first segment must be 0");
        assert(entry.leaf() != leaf && "Infinite loop");

        leaf = context.m_leaf = entry.leaf();
        for(uint64_t segment_id = 0; segment_id < leaf->num_segments(); segment_id++){ // all segments should have been loaded
            context.m_segment = leaf->get_segment(segment_id);
            memstore::Segment::validate_scratchpad(context, m_scratchpad, pos_next_vertex, pos_next_element);
        }
    }
#endif
}

void SpreadOperator::validate_prune(const ScratchPad& copy) const {
#if !defined(NDEBUG) // otherwise all the assertions are nops
    using namespace memstore;

    /**
     * The idea here is that all insertions in the copy should also be present in the current scratchpad.
     * For deletions, we are allowed to skip
     */
    uint64_t main_index = 0; // index in the main scratchpad, the one in the instance member
    uint64_t cp_index = 0; // index in the copied scratchpad

    while(main_index < m_scratchpad.size() && cp_index < copy.size()){
        // Fetch a vertex
        Vertex* main_vertex = m_scratchpad.get_vertex(main_index);
        Vertex* cp_vertex = copy.get_vertex(cp_index);

        // Match?
        if(cp_vertex->m_vertex_id != main_vertex->m_vertex_id){
            // If the vertex is not present, then:
            // 1. the deleted vertex must precede the current vertex
            assert(cp_vertex->m_vertex_id < main_vertex->m_vertex_id && "Sorted order not respected");

            // 2. either this is a dummy vertex or it was flagged with a deletion
            Version* cp_version = copy.get_version(cp_index);
            assert((cp_vertex->m_first == 0 || (cp_version != nullptr && cp_version->is_remove())) && "This vertex should have not been removed from the scratchpad");

            // 3. and then all of its attached edges have been flagged as deletions
            cp_index++;
            const uint64_t cp_index_next_vertex = cp_index + cp_vertex->m_count;
            assert(cp_index_next_vertex <= copy.size() && "Overflow");
            while(cp_index < cp_index_next_vertex){
                Version* cp_version2 = copy.get_version(cp_index);
                assert(cp_version2 != nullptr && cp_version2->is_remove() && "This vertex cannot be removed because it still contains attached edges");

                cp_index++;
            }
        } else { // if the vertices match, fetch the attached edges
            assert((cp_vertex->m_count >= main_vertex->m_count) && "As we have only pruned some edges, the copy should contain more edges, or they are equal (no pruning)");
            main_index++; cp_index++;
            uint64_t main_index_next_vertex = main_index + main_vertex->m_count;
            uint64_t cp_index_next_vertex = cp_index + cp_vertex->m_count;

            assert(main_index_next_vertex <= m_scratchpad.size() && "Overflow, scratchpad");
            assert(cp_index_next_vertex <= copy.size() && "Overflow, copy");

            while(main_index < main_index_next_vertex){
                WeightedEdge* main_edge = m_scratchpad.get_edge(main_index);

                // Skip pruned edges
                while(cp_index < cp_index_next_vertex && copy.get_edge(cp_index)->m_destination != main_edge->m_destination){
                    WeightedEdge* cp_edge = copy.get_edge(cp_index);
                    Version* cp_version = copy.get_version(cp_index);
                    assert(cp_edge->m_destination < main_edge->m_destination && "Sorted order not respected, the edge should precede the main_edge");
                    assert((cp_version != nullptr && cp_version->is_remove()) && "This edge could not be pruned by the scratchpad, because it is an insertion");

                    cp_index++; // next edge from the copy
                }

                assert(cp_index < cp_index_next_vertex && "Edges in the copy depleted");
                WeightedEdge* cp_edge = copy.get_edge(cp_index);
                assert(main_edge->m_destination == cp_edge->m_destination && "The two edges must match");


                main_index++;
                cp_index++;
            }

            // Skip the remaining (pruned) edges in the copy
            while(cp_index < cp_index_next_vertex){
                Version* cp_version = copy.get_version(cp_index);
                assert((cp_version != nullptr && cp_version->is_remove()) && "This edge could not be pruned by the scratchpad, because it is an insertion");

                cp_index++;
            }
        }
    }

    assert(main_index == m_scratchpad.size() && "All elements from the scratchpad should have been processed");

    // Check the remaining elements in the copy are all deletions
    while(cp_index < copy.size()){
        Vertex* cp_vertex = copy.get_vertex(cp_index);
        Version* cp_version = copy.get_version(cp_index);
        assert((cp_vertex->m_first == 0 || (cp_version != nullptr && cp_version->is_remove())) && "(Final pass) This vertex could have not been removed from the scratchpad");

        // check that all attached edges are marked as deletions
        cp_index++;
        uint64_t cp_index_next_vertex = cp_index + cp_vertex->m_count;
        assert(cp_index_next_vertex <= copy.size() && "(Final pass) Overflow");
        while(cp_index < cp_index_next_vertex){
            Version* cp_version2 = copy.get_version(cp_index);
            assert(cp_version2 != nullptr && cp_version2->is_remove() && "(Final pass) This vertex cannot be removed because it still contains attached edges");

            cp_index++;
        }
    }
#endif
}

void SpreadOperator::validate_leaf_traversals() {
#if !defined(NDEBUG)
    memstore::Key lfkey = m_plan.is_resize() ? m_plan.first_leaf()->get_lfkey() : m_plan.first_leaf()->get_segment(m_plan.window_start())->m_fence_key;

    for(auto& item : m_rebalanced_leaves){
        if(item.is_removed()) continue; // ignore leaves marked for deletion, they are not part of the rebalance
        memstore::Leaf* leaf = m_context.m_leaf = item.leaf();

        // count the number of unindexed segments at the start of this leaf
        uint64_t segment_id = item.window_start();
        uint64_t num_unindexed_segments = 0;
        bool is_unindexed = true;
        while(is_unindexed && segment_id < item.window_end()){
            memstore::Segment* segment = m_context.m_segment = leaf->get_segment(segment_id);
            assert(segment->is_sparse() && "Because the segment has just been rebalanced");
            is_unindexed = memstore::Segment::is_unindexed(m_context);
            num_unindexed_segments += is_unindexed;
            segment_id ++;
        }

        // check the entry in the index
        auto entry = m_context.m_tree->index()->find(lfkey.source(), lfkey.destination());
        assert(entry.leaf()->ref_count() > 0 && "This leaf is not marked for deletion");

        // if the whole interval consists of empty segments, then this leaf is not indexed
        bool all_empty = (item.window_length() == num_unindexed_segments);
        if(entry.leaf() != item.leaf() && all_empty){ continue; } // okay, all segments are empty

        assert(entry.leaf() == item.leaf() && "The leaf rebalanced is not reachable from the index");
        assert((entry.segment_id() == item.window_start() + num_unindexed_segments ||
                // I still don't get how this case could raise. The whole interval is composed of empty AND unindexed segments?
               (all_empty && entry.segment_id() >= item.window_end())
        ) && "The segment ID does not match the first segment of the leaf/interval");

        // next iteration
        lfkey = item.leaf()->get_hfkey();
    }

    m_context.m_leaf = nullptr;
    m_context.m_segment = nullptr;
#endif
}

void SpreadOperator::validate_save() const {
#if !defined(NDEBUG)
    memstore::Context context { m_context.m_tree };
    int64_t pos_next_vertex = 0;
    int64_t pos_next_element = 0;
    if(!m_plan.is_resize()){
        memstore::Leaf* leaf = context.m_leaf = m_plan.leaf();
        for(uint64_t segment_id = m_plan.window_start(); segment_id < m_plan.window_end(); segment_id++){
            context.m_segment = leaf->get_segment(segment_id);
            memstore::Segment::validate_scratchpad(context, m_scratchpad, pos_next_vertex, pos_next_element);
        }
    } else {
        assert(m_plan.is_resize() && "Due to the top guard in the if stmt");
        for(auto& item : m_rebalanced_leaves){
            if(item.is_removed()) continue; // skip removed leaves

            memstore::Leaf* leaf = context.m_leaf = item.leaf();
            for(uint64_t segment_id = 0, num_segments = leaf->num_segments(); segment_id < num_segments; segment_id++){
                context.m_segment = leaf->get_segment(segment_id);
                memstore::Segment::validate_scratchpad(context, m_scratchpad, pos_next_vertex, pos_next_element);
            }
        }

    }
    assert(pos_next_element == (int64_t) m_scratchpad.size() && "Not all elements have been copied into the fat tree");
#endif
}

} // namespace


