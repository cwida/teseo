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
#define DEBUG
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

Rebalancer::Rebalancer(SparseArray* instance, uint64_t num_segments, uint64_t capacity) : m_instance(instance), m_capacity(capacity), m_num_segments_total(num_segments) {
    m_elements = (Element*) calloc(m_capacity, sizeof(Element));
    m_versions = (SparseArray::SegmentVersion*) calloc(m_capacity, sizeof(SparseArray::SegmentVersion));
    if(m_elements == nullptr || m_versions == nullptr) throw std::bad_alloc{};
}

Rebalancer::~Rebalancer() {
    free(m_elements); m_elements = nullptr;
    free(m_versions); m_versions = nullptr;
    assert(m_num_segments_saved == m_num_segments_total && "Not all segments have been serialised");
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

        if(v_index < v_length && SparseArray::get_backptr(SparseArray::get_version(v_start + v_index)) == v_backptr){
            version = SparseArray::get_version(v_start + v_index);
            v_index += SparseArray::OFFSET_VERSION;
            SparseArray::prune_on_write(version, /* force */ true);
        }

        if(vertex->m_first == 1 || m_load_previous_vertex < 0){
            resize_if_needed();
            m_load_previous_vertex = m_size;
            m_elements[m_size].m_vertex = *vertex;
            m_space_required += SparseArray::OFFSET_VERTEX;
            if(SparseArray::get_undo(version) != nullptr){
                m_versions[m_size] = *version;
                m_space_required += SparseArray::OFFSET_VERSION;
            }

            m_size++;
        } else { // compact the duplicate vertex entries
            m_elements[m_load_previous_vertex].m_vertex.m_count += vertex->m_count;
        }

        c_index += SparseArray::OFFSET_VERTEX;
        v_backptr++;

        // Fetch its edges
        int64_t e_length = c_index + vertex->m_count * SparseArray::OFFSET_EDGE;
        while(c_index < e_length){
            edge = SparseArray::get_edge(c_start + c_index);
            version = nullptr;

            if(v_index < v_length && SparseArray::get_backptr(SparseArray::get_version(v_start + v_index)) == v_backptr){
                version = SparseArray::get_version(v_start + v_index);
                v_index += SparseArray::OFFSET_VERSION;
                SparseArray::prune_on_write(version, /* force */ true);
            }

            resize_if_needed();
            m_elements[m_size].m_edge = *edge;
            m_space_required += SparseArray::OFFSET_EDGE;
            if(version != nullptr && SparseArray::get_undo(version) != nullptr){
                m_versions[m_size] = *version;
                m_space_required += SparseArray::OFFSET_VERSION;
            }
            m_size++;

            // next iteration
            c_index += SparseArray::OFFSET_EDGE;
            v_backptr++;
        }
    }
}

void Rebalancer::resize_if_needed(){
    if(m_size < m_capacity) return;

    uint64_t new_capacity = 1.5 * m_capacity;
    std::unique_ptr<Element, decltype(&free)> ptr_new_elements { (Element*) calloc(new_capacity, sizeof(Element)), &free };
    std::unique_ptr<SparseArray::SegmentVersion, decltype(&free)> ptr_new_versions { (SparseArray::SegmentVersion*) calloc(new_capacity, sizeof(SparseArray::SegmentVersion)), &free };
    Element* new_elements = ptr_new_elements.get();
    SparseArray::SegmentVersion* new_versions = ptr_new_versions.get();
    if(new_elements == nullptr || new_versions == nullptr) throw std::bad_alloc{};

    memcpy(new_elements, m_elements, m_size * sizeof(m_elements[0]));
    memcpy(new_versions, m_versions, m_size * sizeof(m_versions[0]));

    free(m_elements);
    free(m_versions);

    m_elements = ptr_new_elements.release();
    m_versions = ptr_new_versions.release();
    m_capacity = new_capacity;
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
    assert(m_num_segments_saved < m_num_segments_total);

    SparseArray::SegmentMetadata* segment = m_instance->get_segment(chunk, segment_id);
    // how many qwords should we store in this segment?
    int64_t budget = (m_space_required - m_save_space_used) / (m_num_segments_total - m_num_segments_saved);

    // fill the lhs
    int64_t target_budget_lhs = budget / 2 + (budget % 2 == 1);
    int64_t achieved_budget_lhs = 0;;
    write</* is_lhs ? */ true>(target_budget_lhs, segment, &achieved_budget_lhs);

    // fill the rhs
    int64_t target_budget_rhs = budget - achieved_budget_lhs;
    int64_t achieved_budget_rhs = 0;
    write</* is_lhs ? */ false>(target_budget_rhs, segment, &achieved_budget_rhs);

    m_save_space_used += (achieved_budget_lhs + achieved_budget_rhs);
    m_num_segments_saved++;
}

template<bool is_lhs>
void Rebalancer::write(int64_t target_len, SparseArray::SegmentMetadata* segment, int64_t* out_space_consumed){
    *out_space_consumed = 0;
    if(m_write_cursor >= m_size) return; // done

    int64_t num_versions = 0; // number of versions to store
    int64_t space_consumed = 0;

    // the position of the cursor at the start;
    bool is_first = true;
    bool write_spurious_vertex_at_start = (m_write_next_vertex < m_write_cursor);

    uint64_t write_start = m_write_cursor;
    uint64_t index_first_vertex = m_write_next_vertex;
    while(space_consumed < target_len && m_write_cursor < m_size){
        SparseArray::SegmentVertex* vertex = reinterpret_cast<SparseArray::SegmentVertex*>(m_elements + m_write_next_vertex);

        { // space consumed
            bool has_undo = m_versions[m_write_next_vertex].m_version != 0;
            int64_t space_required = SparseArray::OFFSET_VERTEX + has_undo * SparseArray::OFFSET_VERSION;
            // stop here if we cannot at least write one of its edges
            if(vertex->m_count > 0 && space_consumed + space_required >= target_len){ break; }
            num_versions += has_undo;
            space_consumed += space_required;
        }

        if(!(is_first && write_spurious_vertex_at_start)){
            m_write_cursor++;
        }

        uint64_t i = 0;
        uint64_t num_edges = vertex->m_count; // number of edges to read

        while(space_consumed < target_len && i < num_edges){
            assert(m_write_cursor < m_size && "Counted more edges than what loaded");

            bool has_undo = m_versions[m_write_cursor].m_version != 0;
            num_versions += has_undo * SparseArray::OFFSET_VERSION;
            space_consumed += SparseArray::OFFSET_EDGE + (has_undo) * SparseArray::OFFSET_VERSION;
            m_write_cursor++;
            i++;
        }

        if(i == num_edges){
            m_write_next_vertex = m_write_cursor;
        }
        is_first = false;
    }
    uint64_t write_end = m_write_cursor;

    // copy the data back to the sparse array
    uint64_t* raw_content_area = reinterpret_cast<uint64_t*>(segment + 1);
    uint64_t *content(nullptr), *versions(nullptr), v_start(0), v_end(0);
    if(is_lhs){
        v_start = space_consumed - num_versions * SparseArray::OFFSET_VERSION;
        v_end = space_consumed;
        segment->m_versions1_start = v_start;
        segment->m_empty1_start = v_end;

        content = raw_content_area;
        versions = raw_content_area + v_start;
    } else {
        const uint64_t upper_capacity = m_instance->get_num_qwords_per_segment();
        v_start = upper_capacity - space_consumed;
        v_end = upper_capacity - space_consumed + num_versions * SparseArray::OFFSET_VERSION;
        segment->m_empty2_start = v_start;
        segment->m_versions2_start = v_end;

        content = raw_content_area + v_end;
        versions = raw_content_area + v_start;
    }

    if(space_consumed > 0){
        write_content(content, index_first_vertex, write_start + (!write_spurious_vertex_at_start), write_end);
    }
    if(v_start < v_end) { // otherwise the list of versions is empty
        write_versions(versions, write_start, write_end, write_spurious_vertex_at_start /* true => 1, false => 0 */);
    }

    *out_space_consumed = space_consumed;
#if defined(DEBUG)
    COUT_DEBUG("target budget: " << target_len << " qwords, achieved: " << space_consumed << " qwords");
    write_dump<is_lhs>(segment);
#endif

}

void Rebalancer::write_content(uint64_t* dest_raw, uint64_t src_first_vertex, uint64_t src_start, uint64_t src_end){
    COUT_DEBUG("src_first_vertex: " << src_first_vertex << ", src_start: " << src_start << ", src_end: " << src_end);

    bool is_first_vertex = true;
    while((src_start < src_end) || (is_first_vertex && src_first_vertex < src_start)){
        uint64_t vertex_src_index = is_first_vertex ? src_first_vertex : src_start;
        SparseArray::SegmentVertex* vertex_src = &(m_elements[vertex_src_index].m_vertex);
        COUT_DEBUG("vertex_src[" << vertex_src_index << "]: " << vertex_src->m_vertex_id);
        SparseArray::SegmentVertex* vertex_dst = reinterpret_cast<SparseArray::SegmentVertex*>(dest_raw);
        if(!is_first_vertex) src_start++;

        // copy the vertex
        *vertex_dst = *vertex_src;
        vertex_src->m_first = 0;
        int64_t edges2copy = std::min(src_end - src_start, vertex_src->m_count );
        vertex_dst->m_count = edges2copy;
        vertex_src->m_count -= edges2copy;
        dest_raw += SparseArray::OFFSET_VERTEX;

        // copy the attached edges
        static_assert(sizeof(m_elements[0]) == sizeof(SparseArray::SegmentEdge), "Otherwise we cannot use memcpy below");
        memcpy(dest_raw, m_elements + src_start, edges2copy * sizeof(SparseArray::SegmentEdge));
        dest_raw += SparseArray::OFFSET_EDGE * edges2copy;
        src_start += edges2copy;

        is_first_vertex = false;
    }

}

void Rebalancer::write_versions(uint64_t* dest_raw, uint64_t src_start, uint64_t src_end, uint64_t backptr){
    union SourceVersion {
        SparseArray::SegmentVersion m_version;
        uint64_t m_scalar;
    };
    SourceVersion* __restrict input = reinterpret_cast<SourceVersion*>(m_versions);
    SparseArray::SegmentVersion* __restrict destination = reinterpret_cast<SparseArray::SegmentVersion*>(dest_raw);

    uint64_t i_destination = 0;
    for( uint64_t i_input = src_start; i_input < src_end; i_input++ ) {
        if(input[i_input].m_scalar != 0){
            destination[i_destination] = input[i_input].m_version;
            destination[i_destination].m_backptr = backptr;
            input[i_input].m_scalar = 0;

            i_destination++;
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
            "versions2: " << segment->m_versions2_start << ", empty2: " << segment->m_empty2_start << ", " <<
            "free space: " << m_instance->get_segment_free_space(nullptr, segment) << " qwords, " <<
            "used space: " << m_instance->get_segment_used_space(nullptr, segment) << " qwords, " <<
            (is_lhs ? "lhs" : "rhs") << endl;


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
        cout << "undo: " << SparseArray::get_undo(version) << ",  undo chain length: ";
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


} // namespace
