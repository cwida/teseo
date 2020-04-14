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

#include "rebalancer.hpp"

#include "util/miscellaneous.hpp"
#include "context.hpp"
#include "gate.hpp"
#include "key.hpp"

#include <cstring>
#include <iostream>

using namespace teseo::internal;
using namespace teseo::internal::context;
using namespace teseo::internal::util;
using namespace std;

namespace teseo::internal::memstore {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[Rebalancer::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

Rebalancer::Rebalancer(SparseArray* instance, int64_t num_segments_input, int64_t num_segments_output, RebalancerScratchPad& scratchpad) :
        m_instance(instance), m_scratchpad(scratchpad), /*m_num_segments_input(num_segments_input),*/ m_num_segments_output(num_segments_output),
        m_profiler(num_segments_input, num_segments_output) {
}

Rebalancer::~Rebalancer() {
    m_scratchpad.clear();
}

/*****************************************************************************
 *                                                                           *
 *   Load                                                                    *
 *                                                                           *
 *****************************************************************************/
void Rebalancer::load(SparseArray::Chunk* chunk){
    load(chunk, 0, m_instance->get_num_segments_per_chunk());
}

void Rebalancer::load(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length){
    for(uint64_t i = window_start, end = window_start + window_length; i < end; i++){
        load(chunk, i);
    }
}

void Rebalancer::load(SparseArray::Chunk* chunk, uint64_t segment_id){
    SparseArray::SegmentMetadata* segment = m_instance->get_segment(chunk, segment_id);

    {  // process the lhs of the segment
        uint64_t* c_start = m_instance->get_segment_lhs_content_start(chunk, segment);
        uint64_t* c_end = m_instance->get_segment_lhs_content_end(chunk, segment);
        uint64_t* v_start = m_instance->get_segment_lhs_versions_start(chunk, segment);
        uint64_t* v_end = m_instance->get_segment_lhs_versions_end(chunk, segment);
        do_load(c_start, c_end, v_start, v_end);
    }

    { // process the rhs of the segment
        uint64_t* c_start = m_instance->get_segment_rhs_content_start(chunk, segment);
        uint64_t* c_end = m_instance->get_segment_rhs_content_end(chunk, segment);
        uint64_t* v_start = m_instance->get_segment_rhs_versions_start(chunk, segment);
        uint64_t* v_end = m_instance->get_segment_rhs_versions_end(chunk, segment);
        do_load(c_start, c_end, v_start, v_end);
    }
}


void Rebalancer::do_load(uint64_t* __restrict c_start, uint64_t* __restrict c_end, uint64_t* __restrict v_start, uint64_t* __restrict v_end){
    [[maybe_unused]] auto prof0 = m_profiler.profile_load_time();
    auto PROFILER_PRUNE_TIME = m_profiler.profile_prune_time(false);

    // iterate over the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    uint64_t v_backptr = 0;
    SparseArray::SegmentVertex* vertex = nullptr;
    SparseArray::SegmentEdge* edge = nullptr;
    SparseArray::SegmentVersion* version = nullptr;

    while(c_index < c_length){
        // Fetch a vertex
        vertex = SparseArray::get_vertex(c_start + c_index);
        edge = nullptr;
        version = nullptr;
        m_profiler.incr_count_in_num_elts();
        m_profiler.incr_count_in_num_vertices();
        m_profiler.incr_count_in_num_qwords(SparseArray::OFFSET_VERTEX);

        if(v_index < v_length && SparseArray::get_backptr(SparseArray::get_version(v_start + v_index)) == v_backptr){
            m_profiler.incr_count_in_num_qwords(SparseArray::OFFSET_VERSION);
            version = SparseArray::get_version(v_start + v_index);
            m_instance->validate_version_vertex(vertex, version);
            v_index += SparseArray::OFFSET_VERSION;
            PROFILER_PRUNE_TIME.start();
            SparseArray::prune_on_write(version, /* force */ true);
            PROFILER_PRUNE_TIME.stop();
            if(SparseArray::get_undo(version) == nullptr && SparseArray::is_remove(version)){
                COUT_DEBUG("Skip vertex " << vertex->m_vertex_id);
                c_index += SparseArray::OFFSET_VERTEX + vertex->m_count * SparseArray::OFFSET_EDGE;
                v_backptr += 1 + vertex->m_count;
                continue;
            }
        }

        if(vertex->m_first == 1 || !m_scratchpad.has_last_vertex()){
            SparseArray::SegmentVersion* v_vertex = nullptr;
            m_space_required += SparseArray::OFFSET_VERTEX;
            if(version != nullptr && SparseArray::get_undo(version) != nullptr){
                v_vertex = version;
                m_space_required += SparseArray::OFFSET_VERSION;
            }

            COUT_DEBUG("[" << m_scratchpad.size() << "] " << SparseArray::vertex2string(vertex, version) << ", cumulative space required: " << m_space_required << " qwords");

            m_scratchpad.load_vertex(vertex, v_vertex);
        } else { // compact the duplicate vertex entries
            assert(vertex->m_count > 0 && "Dummy vertex with zero edges attached");
            m_scratchpad.get_last_vertex()->m_count += vertex->m_count;
        }

        c_index += SparseArray::OFFSET_VERTEX;
        v_backptr++;

        // Fetch its edges
        int64_t e_length = c_index + vertex->m_count * SparseArray::OFFSET_EDGE;
        while(c_index < e_length){
            edge = SparseArray::get_edge(c_start + c_index);
            version = nullptr;
            m_profiler.incr_count_in_num_elts();
            m_profiler.incr_count_in_num_edges();
            m_profiler.incr_count_in_num_qwords(SparseArray::OFFSET_EDGE);

            // Prune the undo records
            if(v_index < v_length && SparseArray::get_backptr(SparseArray::get_version(v_start + v_index)) == v_backptr){
                m_profiler.incr_count_in_num_qwords(SparseArray::OFFSET_VERSION);
                version = SparseArray::get_version(v_start + v_index);
                m_instance->validate_version_edge(vertex, edge, version);
                v_index += SparseArray::OFFSET_VERSION;
                PROFILER_PRUNE_TIME.start();
                SparseArray::prune_on_write(version, /* force */ true);
                PROFILER_PRUNE_TIME.stop();
                if(SparseArray::get_undo(version) == nullptr && SparseArray::is_remove(version)){
                    COUT_DEBUG("Skip edge " << vertex->m_vertex_id << " -> " << edge->m_destination);
                    c_index += SparseArray::OFFSET_EDGE;
                    v_backptr++;

                    // Update the count of the attached vertex
                    SparseArray::SegmentVertex* loaded_vertex = m_scratchpad.get_last_vertex();
                    assert(loaded_vertex->m_count > 0 && "Underflow");
                    loaded_vertex->m_count--;
                    if(loaded_vertex->m_first == 0 && loaded_vertex->m_count == 0){ // remove the vertex
                        COUT_DEBUG("[" << m_scratchpad.size() << "] Remove the last (dummy) vertex loaded, its count became zero");
                        // dummy vertices don't have a version
                        assert(m_space_required >= SparseArray::OFFSET_VERTEX && "Underflow");
                        m_space_required -= SparseArray::OFFSET_VERTEX;
                        m_scratchpad.unload_last_vertex();
                    }

                    continue;
                }
            } // end if, prune the edge undo records


            SparseArray::SegmentVersion* v_edge = nullptr;
            m_space_required += SparseArray::OFFSET_EDGE;
            if(version != nullptr && SparseArray::get_undo(version) != nullptr){
                v_edge = version;
                m_space_required += SparseArray::OFFSET_VERSION;
            }

            COUT_DEBUG("[" << m_scratchpad.size() << "] " << SparseArray::edge2string(vertex, edge, version) << ", cumulative space required: " << m_space_required << " qwords");
            m_scratchpad.load_edge(edge, v_edge);

            // next iteration
            c_index += SparseArray::OFFSET_EDGE;
            v_backptr++;
        } // end while, fetch edges
    } // end while, fetch vertices
}

/*****************************************************************************
 *                                                                           *
 *   Save                                                                    *
 *                                                                           *
 *****************************************************************************/
void Rebalancer::save(SparseArray::Chunk* chunk){
    save(chunk, 0, m_instance->get_num_segments_per_chunk());
}

void Rebalancer::save(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length){
    for(uint64_t i = window_start, end = window_start + window_length; i < end; i++){
        do_save(chunk, i);
    }
}

void Rebalancer::do_save(SparseArray::Chunk* chunk, uint64_t segment_id){
    assert(m_num_segments_saved < m_num_segments_output);

    SparseArray::SegmentMetadata* segment = m_instance->get_segment(chunk, segment_id);
    // how many qwords should we store in this segment?
    int64_t budget = (m_space_required - m_save_space_used) / (m_num_segments_output - m_num_segments_saved);

    COUT_DEBUG(">> chunk: " << chunk << ", segment: " << segment_id << ", space required: " << m_space_required << ", space used: " << m_save_space_used << ", segments total: " << m_num_segments_output << ", segments saved: " << m_num_segments_saved << ", budget: " << budget << " qwords");

    // fill the lhs
    COUT_DEBUG("segment: " << segment_id << " (lhs)");
    int64_t target_budget_lhs = budget / 2 + (budget % 2 == 1);
    int64_t achieved_budget_lhs = 0;;
    write</* is_lhs ? */ true>(target_budget_lhs, segment, &achieved_budget_lhs);
    Key validate_key = Key::min();
    m_instance->validate_content(chunk, segment, true, &validate_key);

    // fill the rhs
    COUT_DEBUG("segment: " << segment_id << " (rhs)");
    int64_t target_budget_rhs = max<int64_t>(0, budget - achieved_budget_lhs);
    int64_t achieved_budget_rhs = 0;
    write</* is_lhs ? */ false>(target_budget_rhs, segment, &achieved_budget_rhs);
    m_instance->validate_content(chunk, segment, false, &validate_key);

    m_save_space_used += (achieved_budget_lhs + achieved_budget_rhs);
    m_num_segments_saved++;

    COUT_DEBUG("total space loaded: " << m_space_required << " qwords, total space used: " << m_save_space_used << " qwords, num segments visited so far: " << m_num_segments_saved);
}

template<bool is_lhs>
void Rebalancer::write(int64_t target_len, SparseArray::SegmentMetadata* segment, int64_t* out_space_consumed){
    [[maybe_unused]] auto prof0 = m_profiler.profile_write_time();

    *out_space_consumed = 0;

    int64_t num_versions = 0; // number of versions to store
    int64_t space_consumed = 0;

    // the position of the cursor at the start;
    bool is_first = true; // first vertex in the sequence
    bool write_spurious_vertex_at_start = (m_write_next_vertex < m_write_cursor);
    uint64_t spurious_vertex_space_required = 0;

    uint64_t write_start = m_write_cursor;
    uint64_t index_first_vertex = m_write_next_vertex;
    while(space_consumed < target_len && m_write_cursor < m_scratchpad.size()){
        SparseArray::SegmentVertex* vertex = m_scratchpad.get_vertex(m_write_next_vertex);

        { // space consumed
            bool has_undo = m_scratchpad.has_version(m_write_next_vertex); //m_versions[m_write_next_vertex].m_version != 0;
            int64_t space_required = SparseArray::OFFSET_VERTEX + has_undo * SparseArray::OFFSET_VERSION;
            // stop here if we cannot at least write one of its edges
            if(vertex->m_count > 0 && !is_first && space_consumed + space_required >= target_len){ break; }
            num_versions += has_undo;

            if(!(is_first && write_spurious_vertex_at_start)){ // do not account the first vertex at the start
                space_consumed += space_required;
                m_write_cursor++;
            } else {
                spurious_vertex_space_required = space_required;
            }
        }


        is_first = true; // first edge in the sequence
        uint64_t i = 0;
        uint64_t num_edges = vertex->m_count; // number of edges to read
        while(((space_consumed < target_len) ||
                /* corner case: if we have just written a non first vertex, we need to write at least one edge */
                (is_first && vertex->m_first == 0)
            ) && i < num_edges){

            assert(m_write_cursor < m_scratchpad.size() && "Counted more edges than what loaded");

            bool has_undo = m_scratchpad.has_version(m_write_cursor); //m_versions[m_write_cursor].m_version != 0;
            num_versions += has_undo;
            space_consumed += SparseArray::OFFSET_EDGE + (has_undo) * SparseArray::OFFSET_VERSION;
            m_write_cursor++;
            i++;

            is_first = false;
        }

        // vertex->m_count is eventually altered in the method #write_content

        if(i == num_edges){
            m_write_next_vertex = m_write_cursor;
        }
        is_first = false;
    }
    uint64_t write_end = m_write_cursor;


    // copy the data back to the sparse array
    uint64_t space_consumed_total = space_consumed + spurious_vertex_space_required;
    uint64_t* raw_content_area = reinterpret_cast<uint64_t*>(segment + 1);
    uint64_t *content(nullptr), *versions(nullptr), v_start(0), v_end(0);
    if(is_lhs){
        v_start = space_consumed_total - num_versions * SparseArray::OFFSET_VERSION;
        v_end = space_consumed_total;
        segment->m_versions1_start = v_start;
        segment->m_empty1_start = v_end;

        content = raw_content_area;
        versions = raw_content_area + v_start;
    } else {
        const uint64_t upper_capacity = m_instance->get_num_qwords_per_segment();
        v_start = upper_capacity - space_consumed_total;
        v_end = v_start + num_versions * SparseArray::OFFSET_VERSION;
        segment->m_empty2_start = v_start;
        segment->m_versions2_start = v_end;

        content = raw_content_area + v_end;
        versions = raw_content_area + v_start;

        // check we didn't overflow the segment
        assert(segment->m_versions1_start <= segment->m_empty1_start);
        assert(segment->m_empty2_start <= segment->m_versions2_start);
        assert(segment->m_empty1_start <= segment->m_empty2_start);
    }

    assert(space_consumed > 0 || space_consumed_total == 0); // if space_consumed == 0 => then we didn't write anything

    if(space_consumed > 0){
        write_content(content, index_first_vertex, write_start + (!write_spurious_vertex_at_start), write_end);
    }
    if(v_start < v_end) { // otherwise the list of versions is empty
        write_versions(versions, write_start, write_end, write_spurious_vertex_at_start /* true => 1, false => 0 */);
    }

    // if we were required to write some content (target_len > 0), then we must have written something (space_consumed > 0)
    assert(target_len == 0 || space_consumed > 0);

    *out_space_consumed = space_consumed;
#if defined(DEBUG)
    COUT_DEBUG("write_cursor: " << m_write_cursor << ", target budget: " << target_len << " qwords, achieved: " << space_consumed << " qwords");
    write_dump<is_lhs>(segment);
#endif
}

void Rebalancer::write_content(uint64_t* dest_raw, uint64_t src_first_vertex, uint64_t src_start, uint64_t src_end){
    //COUT_DEBUG("src_first_vertex: " << src_first_vertex << ", src_start: " << src_start << ", src_end: " << src_end);

    bool is_first_vertex = true;
    while((src_start < src_end) || (is_first_vertex && src_first_vertex < src_start)){
        uint64_t vertex_src_index = is_first_vertex ? src_first_vertex : src_start;
        SparseArray::SegmentVertex* vertex_src = m_scratchpad.get_vertex(vertex_src_index); //&(m_elements[vertex_src_index].m_vertex);
        //COUT_DEBUG("vertex_src[" << vertex_src_index << "]: " << vertex_src->m_vertex_id);
        SparseArray::SegmentVertex* vertex_dst = reinterpret_cast<SparseArray::SegmentVertex*>(dest_raw);
        if(!is_first_vertex) src_start++;

        // copy the vertex
        *vertex_dst = *vertex_src;
        vertex_src->m_first = 0;
        vertex_src->m_lock = vertex_dst->m_lock;
        int64_t edges2copy = std::min(src_end - src_start, vertex_src->m_count );
        vertex_dst->m_count = edges2copy;
        vertex_src->m_count -= edges2copy;
        dest_raw += SparseArray::OFFSET_VERTEX;

        m_profiler.incr_count_out_num_elts();
        m_profiler.incr_count_out_num_vertices();
        m_profiler.incr_count_out_num_qwords(SparseArray::OFFSET_VERTEX);

        // copy the attached edges
        static_assert(sizeof(SparseArray::SegmentVertex) == sizeof(SparseArray::SegmentEdge), "Otherwise we cannot use memcpy below");
        memcpy(dest_raw, m_scratchpad.get_edge(src_start), edges2copy * sizeof(SparseArray::SegmentEdge));
        dest_raw += SparseArray::OFFSET_EDGE * edges2copy;
        src_start += edges2copy;

        m_profiler.incr_count_out_num_elts(edges2copy);
        m_profiler.incr_count_out_num_edges(edges2copy);
        m_profiler.incr_count_out_num_qwords(SparseArray::OFFSET_EDGE * edges2copy);

        is_first_vertex = false;
    }

}

void Rebalancer::write_versions(uint64_t* dest_raw, uint64_t src_start, uint64_t src_end, uint64_t backptr){
    SparseArray::SegmentVersion* __restrict destination = reinterpret_cast<SparseArray::SegmentVersion*>(dest_raw);

    uint64_t i_destination = 0;
    for( uint64_t i_input = src_start; i_input < src_end; i_input++ ) {
        //if(input[i_input].m_scalar != 0){
        if(m_scratchpad.has_version(i_input)){
            destination[i_destination] = m_scratchpad.move_version(i_input);
            destination[i_destination].m_backptr = backptr;

            i_destination++;

            m_profiler.incr_count_out_num_qwords(SparseArray::OFFSET_VERSION);
        }

        backptr++;
    }
}

template<bool is_lhs>
void Rebalancer::write_dump(SparseArray::SegmentMetadata* segment){
    lock_guard<mutex> lock(g_debugging_mutex);
    cout << "[Rebalancer::write_dump]" << endl;
    cout << "segment: " << (void*) segment << ", " <<
            "versions1: " << segment->m_versions1_start << ", empty1: " << segment->m_empty1_start << ", " <<
            "empty2: " << segment->m_empty2_start << ", version2: " << segment->m_versions1_start << ", ";
    if(!is_lhs) {
        // if this were the LHS, we still need to update empty2 and versions2. Invoking #get_segment_free_space
        // could have raised an assertion as these fields would have been inconsistent
        cout << "free space: " << m_instance->get_segment_free_space(nullptr, segment) << " qwords, " <<
                "used space: " << m_instance->get_segment_used_space(nullptr, segment) << " qwords, ";
    }
    cout << (is_lhs ? "lhs" : "rhs") << endl;


    uint64_t* content = m_instance->get_segment_content_start(nullptr, segment, is_lhs);
    uint64_t c_pos = 0;
    uint64_t c_len = m_instance->get_segment_content_end(nullptr, segment, is_lhs) - content;
    uint64_t* versions = m_instance->get_segment_versions_start(nullptr, segment, is_lhs);
    uint64_t v_pos = 0;
    uint64_t v_len = m_instance->get_segment_versions_end(nullptr, segment, is_lhs) - versions;
    uint64_t v_backptr = 0;

    auto dump_version = [&](SparseArray::SegmentVersion* version){
        if(version == nullptr) return;
        cout << " [version present] " << (SparseArray::is_insert(version) ? "insert" : "remove") << ", ";
        cout << "undo: " << SparseArray::get_undo(version) << ", undo chain length: ";
        if(version->m_undo_length == SparseArray::MAX_UNDO_LENGTH) {
            cout << "MAX >=" << version->m_undo_length;
        } else {
            cout << version->m_undo_length;
        }
        cout << ", backptr: " << version->m_backptr;
    };

    while(c_pos < c_len){
        SparseArray::SegmentVertex* vertex = SparseArray::get_vertex(content + c_pos);
        SparseArray::SegmentVersion* version = nullptr;

        if(v_pos < v_len){
            auto candidate = SparseArray::get_version(versions + v_pos);
            if(SparseArray::get_backptr(candidate) == v_backptr){
                version = candidate;
                v_pos += SparseArray::OFFSET_VERSION;
            }
        }

        cout << "[" << v_backptr << "] Vertex: " << vertex->m_vertex_id;
        if(vertex->m_first){ cout << " [first]"; };
        cout << ", edge count: " << vertex->m_count;
        dump_version(version);
        cout << endl;


        c_pos += SparseArray::OFFSET_VERTEX;
        v_backptr++;

        uint64_t e_pos = 0;
        uint64_t e_len = vertex->m_count;
        while(c_pos < c_len && e_pos < e_len){
            SparseArray::SegmentEdge* edge = SparseArray::get_edge(content + c_pos);
            version = nullptr;

            if(v_pos < v_len){
                auto candidate = SparseArray::get_version(versions + v_pos);
                if(SparseArray::get_backptr(candidate) == v_backptr){
                    version = candidate;
                    v_pos += SparseArray::OFFSET_VERSION;
                }
            }

            cout << "[" << v_backptr << "] Edge: " << vertex->m_vertex_id << " -> " << edge->m_destination << ", weight: " << edge->m_weight;
            dump_version(version);
            cout << endl;

            e_pos ++;
            c_pos += SparseArray::OFFSET_EDGE;
            v_backptr++;
        }
    }
}

void Rebalancer::validate(){
    assert(m_num_segments_saved == m_num_segments_output && "Not all segments have been serialised");
    assert(m_write_cursor == m_scratchpad.size() && "Not all elements have been saved");
    assert(m_save_space_used == (int64_t) m_space_required && "Again, counting error");
}

/*****************************************************************************
 *                                                                           *
 *   ScrachPad                                                               *
 *                                                                           *
 *****************************************************************************/

RebalancerScratchPad::RebalancerScratchPad(uint64_t capacity) : m_capacity(capacity){
    m_last_vertex_loaded = numeric_limits<uint64_t>::max();
    m_elements = (decltype(m_elements)) malloc(capacity * sizeof(m_elements[0]));
    m_versions = (decltype(m_versions)) malloc(capacity * sizeof(m_versions[0]));
    if(m_elements == nullptr || m_versions == nullptr){
        COUT_DEBUG_FORCE("bad alloc at " << __FILE__ << ":" << __LINE__ << ", capacity: " << capacity << ", m_elements: " << m_elements << ", m_versions: " << m_versions);
        throw std::bad_alloc{};
    }
}

RebalancerScratchPad::~RebalancerScratchPad(){
    free(m_elements); m_elements = nullptr;
    free(m_versions); m_versions = nullptr;
}

uint64_t RebalancerScratchPad::capacity() const {
    return m_capacity;
}

uint64_t RebalancerScratchPad::size() const {
    return m_size;
}

void RebalancerScratchPad::clear() {
    m_size = 0;
    m_last_vertex_loaded = -1;
}

void RebalancerScratchPad::load_vertex(SparseArray::SegmentVertex* vertex, SparseArray::SegmentVersion* version){
    assert(m_size < m_capacity && "Overflow");

    m_elements[m_size].m_vertex = *vertex;
    set_version(m_size, version);
    m_last_vertex_loaded = m_size;
    m_size ++;
}

void RebalancerScratchPad::unload_last_vertex(){
    assert(m_size > 0 && "Empty");
    assert(has_last_vertex() && "No last vertex registered");
    m_size = m_last_vertex_loaded;
    m_last_vertex_loaded = numeric_limits<uint64_t>::max();
}

void RebalancerScratchPad::load_edge(SparseArray::SegmentEdge* edge, SparseArray::SegmentVersion* version){
    assert(m_size < m_capacity && "Overflow");
    m_elements[m_size].m_edge = *edge;
    set_version(m_size, version);
    m_size++;
}

void RebalancerScratchPad::set_version(uint64_t position, SparseArray::SegmentVersion* version){
    if(version == nullptr || version->m_version == 0){
        unset_version(position);
    } else {
        m_versions[position] = *version;
    }
}

void RebalancerScratchPad::unset_version(uint64_t position) {
    reinterpret_cast<uint64_t*>(m_versions)[position] = 0;
}

SparseArray::SegmentVertex* RebalancerScratchPad::get_vertex(uint64_t position) const {
    assert(position < m_capacity && "Invalid index");
    return &(m_elements[position].m_vertex);
}

SparseArray::SegmentEdge* RebalancerScratchPad::get_edge(uint64_t position) const {
    assert(position < m_capacity && "Invalid index");
    return &(m_elements[position].m_edge);
}

SparseArray::SegmentVersion RebalancerScratchPad::move_version(uint64_t position) {
    assert(position < m_capacity && "Invalid index");
    assert(has_version(position) && "No version set");
    auto version = m_versions[position];
    unset_version(position);
    return version;
}

SparseArray::SegmentVertex* RebalancerScratchPad::get_last_vertex() const {
    return has_last_vertex() ? &(m_elements[m_last_vertex_loaded].m_vertex) : nullptr;
}

bool RebalancerScratchPad::has_last_vertex() const{
    return m_last_vertex_loaded != numeric_limits<uint64_t>::max();
}

bool RebalancerScratchPad::has_version(uint64_t position) const {
    assert(position < m_capacity && "Invalid index");
    return reinterpret_cast<uint64_t*>(m_versions)[position] != 0;
}

} // namespace
