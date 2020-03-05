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

#include "context.hpp"
#include "gate.hpp"

#include <cstring>
#include <iostream>

using namespace teseo::internal;
using namespace teseo::internal::context;
using namespace std;

namespace teseo::internal::memstore {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(::teseo::internal::g_debugging_mutex); std::cout << "[Rebalancer::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
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

Rebalancer::Rebalancer(SparseArray* instance, uint64_t total_num_segments) : m_instance(instance), m_num_segments_total(total_num_segments) {

}

Rebalancer::~Rebalancer() {
    delete[] m_buffer_static; m_buffer_static = nullptr;
    delete[] m_buffer_delta; m_buffer_delta = nullptr;
}

Rebalancer::Vertex::Vertex(uint64_t vertex_id, Undo* undo, bool is_first, bool is_removed) : m_vertex_id(vertex_id), m_version(undo), m_is_first(is_first), m_is_removed(is_removed), m_space_estimated(0) { }
Rebalancer::Edge::Edge(uint64_t source, uint64_t destination, double weight, Undo* undo, bool is_removed) : m_source(source), m_destination(destination), m_weight(weight), m_version(undo), m_is_removed(is_removed), m_space_estimated(0) { }

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
    {  // process the lhs of the segment
        uint64_t* static_start = m_instance->get_segment_lhs_static_start(chunk, segment_id);
        uint64_t* static_end = m_instance->get_segment_lhs_static_end(chunk, segment_id);
        uint64_t* delta_start = m_instance->get_segment_lhs_delta_start(chunk, segment_id);
        uint64_t* delta_end = m_instance->get_segment_lhs_delta_end(chunk, segment_id);
        do_load(static_start, static_end, delta_start, delta_end);
    }

    { // process the rhs of the segment
        uint64_t* static_start = m_instance->get_segment_rhs_static_start(chunk, segment_id);
        uint64_t* static_end = m_instance->get_segment_rhs_static_end(chunk, segment_id);
        uint64_t* delta_start = m_instance->get_segment_rhs_delta_start(chunk, segment_id);
        uint64_t* delta_end = m_instance->get_segment_rhs_delta_end(chunk, segment_id);
        do_load(static_start, static_end, delta_start, delta_end);
    }
}


void Rebalancer::do_load(uint64_t* __restrict static_start, uint64_t* __restrict static_end, uint64_t* __restrict delta_start, uint64_t* __restrict delta_end){
    Key key_static; bool read_next_static = true; bool is_static_vertex = false;
    Key key_delta; bool read_next_delta = true;
    SparseArray::SegmentStaticVertex* vertex_static { nullptr };
    SparseArray::SegmentStaticEdge* edge_static { nullptr };
    SparseArray::SegmentDeltaVertex* vertex_delta { nullptr };
    SparseArray::SegmentDeltaEdge* edge_delta { nullptr };
    int vertex_static_count = 0;

    // merge the static & the delta storages
    uint64_t* __restrict static_current = static_start;
    uint64_t* __restrict delta_current = delta_start;
    while( ( !read_next_static || static_current < static_end ) && ( !read_next_delta || delta_current < delta_end ) ){
        if(read_next_static){ // fetch the next item from the static store
            if(vertex_static == nullptr || vertex_static_count <= 0){ // fetch the next vertex
                is_static_vertex = true;
                vertex_static = SparseArray::get_static_vertex(static_current);
                vertex_static_count = vertex_static->m_count;
                key_static.set(vertex_static->m_vertex_id);
                static_current += sizeof(SparseArray::SegmentStaticVertex) /8;
            } else { // fetch the next edge
                assert(vertex_static != nullptr);
                is_static_vertex = false;
                edge_static = SparseArray::get_static_edge(static_current);
                assert(key_static.get_source() == vertex_static->m_vertex_id);
                key_static.set(vertex_static->m_vertex_id, edge_static->m_destination);
                static_current += sizeof(SparseArray::SegmentStaticEdge) /8;
                vertex_static_count--;
            }

            read_next_static = false;
        }

        if(read_next_delta){ // fetch the next item from the delta store
            auto header = SparseArray::get_delta_header(delta_current);
            if(SparseArray::is_vertex(header)){
                vertex_delta = SparseArray::get_delta_vertex(delta_current);
                edge_delta = nullptr;
                key_delta.set(vertex_delta->m_vertex_id);
                delta_current += sizeof(SparseArray::SegmentDeltaVertex) /8;
            } else {
                vertex_delta = nullptr;
                edge_delta = SparseArray::get_delta_edge(delta_current);
                key_delta.set(edge_delta->m_source, edge_delta->m_destination);
                delta_current += sizeof(SparseArray::SegmentDeltaEdge) /8;
            }
            read_next_delta = false;
        }

        if(key_static <= key_delta){
            if(is_static_vertex){
                append_vertex(vertex_static->m_vertex_id, nullptr, vertex_static->m_first, false);
            } else {
                append_edge(vertex_static->m_vertex_id, edge_static->m_destination, edge_static->m_weight, nullptr, false);
            }
            read_next_static = true;
        } else {
            if(vertex_delta != nullptr){
                assert(edge_delta != nullptr);
                append_vertex(vertex_delta->m_vertex_id, SparseArray::get_delta_undo(vertex_delta), /* is first ? */ true, SparseArray::is_remove(vertex_delta));
            } else {
                assert(vertex_delta == nullptr);
                append_edge(edge_delta->m_source, edge_delta->m_destination, edge_delta->m_weight, SparseArray::get_delta_undo(edge_delta), SparseArray::is_remove(vertex_delta));
            }

            read_next_delta = true;
        }
    }

    // read all remaining records from the static storage, ignore key_static & key_delta
    while(static_current < static_end){
        if(vertex_static == nullptr || vertex_static_count <= 0){ // fetch the next vertex
            is_static_vertex = true;
            vertex_static = SparseArray::get_static_vertex(static_current);
            append_vertex(vertex_static->m_vertex_id, nullptr, vertex_static->m_first, false);
            vertex_static_count = vertex_static->m_count;
            static_current += sizeof(SparseArray::SegmentStaticVertex) /8;
        } else { // fetch the next edge
            assert(vertex_static != nullptr);
            edge_static = SparseArray::get_static_edge(static_current);
            append_edge(vertex_static->m_vertex_id, edge_static->m_destination, edge_static->m_weight, nullptr, false);
            static_current += sizeof(SparseArray::SegmentStaticEdge) /8;
            vertex_static_count--;
        }
    }
    assert(vertex_static_count == 0 && "We didn't read all the static storage");

    // read all remaining records from the delta storage, ignore key_static & key_delta
    while(delta_current < delta_end){
        auto header = SparseArray::get_delta_header(delta_current);
        if(SparseArray::is_vertex(header)){
            vertex_delta = SparseArray::get_delta_vertex(delta_current);
            edge_delta = nullptr; // just for consistency, but unnecessary
            append_vertex(vertex_delta->m_vertex_id, SparseArray::get_delta_undo(vertex_delta), /* is first ? */ true, SparseArray::is_remove(vertex_delta));
            delta_current += sizeof(SparseArray::SegmentDeltaVertex) /8;
        } else {
            vertex_delta = nullptr; // just for consistency, but unnecessary
            edge_delta = SparseArray::get_delta_edge(delta_current);
            append_edge(edge_delta->m_source, edge_delta->m_destination, edge_delta->m_weight, SparseArray::get_delta_undo(edge_delta), SparseArray::is_remove(vertex_delta));
            delta_current += sizeof(SparseArray::SegmentDeltaEdge) /8;
        }
    }
}

void Rebalancer::append_vertex(uint64_t vertex_id, Undo* version, bool is_first, bool is_remove) {
    Vertex* last_vertex = m_vertices.empty() ? nullptr : &(m_vertices.back());
    if(last_vertex == nullptr || last_vertex->m_vertex_id != vertex_id){
        assert(last_vertex == nullptr || last_vertex->m_vertex_id < vertex_id); // otherwise, the sorted order is not respected
        m_vertices.emplace_back(vertex_id, version, is_first, is_remove);
    } else { // overwrite the content of the previous entry
        assert(last_vertex != nullptr && last_vertex->m_vertex_id == vertex_id);
        last_vertex->m_version = version;
        last_vertex->m_is_first = is_first;
        last_vertex->m_is_removed = is_remove;
    }
}

void Rebalancer::append_edge(uint64_t source, uint64_t destination, double weight, Undo* version, bool is_remove){
    Edge* last_edge = m_edges.empty() ? nullptr : &(m_edges.back());
    if(last_edge != nullptr && last_edge->m_source == source && last_edge->m_destination == destination){ // overwrite the entry
        last_edge->m_weight = weight;
        last_edge->m_version = version;
        last_edge->m_is_removed = is_remove;
    } else {
        m_edges.emplace_back(source, destination, weight, version, is_remove);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Compact                                                                 *
 *                                                                           *
 *****************************************************************************/

void Rebalancer::compact(){
    auto min_epoch = global_context()->min_epoch();
    m_space_required = 0;

    // Start with the edges
    for(uint64_t i = 0; i < m_edges.size(); i++){
        auto& edge = m_edges[i];
        if(edge.m_version != nullptr && edge.m_version->transaction_id() < min_epoch){
            Undo::mark_chain_obsolete(transaction(), edge.m_version);
            edge.m_version = nullptr;
        }

        // Provide an estimate (lower bound) of the space required
        if(edge.m_version == nullptr && !edge.m_is_removed){
            m_space_required += edge.m_space_estimated = sizeof(SparseArray::SegmentStaticEdge) /8;
        } else if(edge.m_version != nullptr){
            m_space_required += edge.m_space_estimated = sizeof(SparseArray::SegmentDeltaEdge) /8;
        }

    }

    // Proceed with the vertices
    for(uint64_t i = 0; i < m_vertices.size(); i++){
        auto& vertex = m_vertices[i];
        if(vertex.m_version != nullptr && vertex.m_version->transaction_id() < min_epoch){
            Undo::mark_chain_obsolete(transaction(), vertex.m_version);
            vertex.m_version = nullptr;
        }

        // Provide an estimate (lower bound) of the space required
        if(vertex.m_version == nullptr && !vertex.m_is_removed){
            m_space_required += vertex.m_space_estimated = sizeof(SparseArray::SegmentStaticVertex) /8;
        } else if(vertex.m_version != nullptr){
            m_space_required += vertex.m_space_estimated = sizeof(SparseArray::SegmentDeltaVertex) /8;
        }
    }
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

    Gate* gate = m_instance->get_gate(chunk, segment_id / m_instance->get_num_segments_per_lock());
    uint64_t s2gid = (segment_id % m_instance->get_num_segments_per_lock()) *2;

    SparseArray::SegmentMetadata* segment = m_instance->get_segment_metadata(chunk, segment_id);
    int64_t budget = (m_space_required - m_save_space_used) / (m_num_segments_total - m_num_segments_saved);

    // fill the lhs
    int64_t target_budget_lhs = budget / 2 + (budget % 2 == 1);
    uint64_t* __restrict buffer_static_lhs = m_instance->get_segment_static_start(chunk, segment_id, true);
    uint64_t* __restrict buffer_delta_lhs = m_buffer_delta;
    int64_t buffer_static_lhs_len = 0;
    int64_t buffer_delta_lhs_len = 0;
    Key min_key;
    write_buffers(target_budget_lhs, buffer_static_lhs, buffer_delta_lhs, &buffer_static_lhs_len, &buffer_delta_lhs_len, &min_key);
    segment->m_delta1_start = buffer_static_lhs_len;
    segment->m_empty1_start = buffer_static_lhs_len + buffer_delta_lhs_len;
    memcpy(m_instance->get_segment_lhs_delta_start(chunk, segment_id), buffer_delta_lhs, buffer_delta_lhs_len * sizeof(uint64_t));
    gate->set_separator_key(s2gid, min_key);

    // fill the rhs
    int64_t target_budget_rhs = (budget / 2) + (buffer_static_lhs_len + buffer_delta_lhs_len - target_budget_lhs);
    uint64_t* __restrict buffer_static_rhs = m_buffer_static;
    uint64_t* __restrict buffer_delta_rhs = m_buffer_delta;
    int64_t buffer_static_rhs_len = 0;
    int64_t buffer_delta_rhs_len = 0;
    write_buffers(target_budget_rhs, buffer_static_rhs, buffer_delta_rhs, &buffer_static_rhs_len, &buffer_delta_rhs_len, &min_key);
    segment->m_delta2_start = m_instance->get_num_qwords_per_segment() - buffer_static_rhs_len;
    segment->m_empty2_start = static_cast<int64_t>(segment->m_delta2_start) - buffer_delta_rhs_len;
    gate->set_separator_key(s2gid +1, min_key);

    m_num_segments_saved++;
}

void Rebalancer::write_buffers(int64_t target_len, uint64_t* __restrict buffer_static, uint64_t* __restrict buffer_delta, int64_t* out_buffer_static_len, int64_t* out_buffer_delta_len, Key* out_min_key){
    int64_t buffer_static_len = 0;
    int64_t buffer_delta_len = 0;
    SparseArray::SegmentStaticVertex* last_static_vertex = nullptr;
    bool last_static_is_empty_vertex = false; // is the last record written in the static buffer an a vertex with m_first == false and no following edges?
    uint64_t last_static_estimated_space = 0; // estimated space for the last written static vertex

    while((buffer_static_len + buffer_delta_len < target_len) && (m_save_vertex_index < m_vertices.size() || m_save_edge_index < m_edges.size())){
        if((m_save_edge_index >= m_edges.size()) || m_vertices[m_save_vertex_index].m_vertex_id <= m_edges[m_save_edge_index].m_source){ // write a vertex
            auto& vertex = m_vertices[m_save_vertex_index];
            if(vertex.m_version == nullptr && !vertex.m_is_removed){
                last_static_vertex = reinterpret_cast<SparseArray::SegmentStaticVertex*>(buffer_static + buffer_static_len);
                last_static_vertex->m_vertex_id = vertex.m_vertex_id;
                last_static_vertex->m_count = 0;

                if(vertex.m_is_first){ // first time we write this static vertex?
                    last_static_vertex->m_first = 1;
                    vertex.m_is_first = false;
                    last_static_is_empty_vertex = false;
                } else {
                    last_static_vertex->m_first = 0;
                    last_static_is_empty_vertex = true;
                }

                buffer_static_len += sizeof(SparseArray::SegmentStaticVertex) /8;
                last_static_estimated_space = vertex.m_space_estimated;
            } else if (vertex.m_version != nullptr){
                auto delta_vertex = reinterpret_cast<SparseArray::SegmentDeltaVertex*>(buffer_delta + buffer_delta_len);
                SparseArray::set_vertex(delta_vertex);
                SparseArray::set_type(delta_vertex, vertex.m_is_removed);
                SparseArray::set_undo(delta_vertex, vertex.m_version);
                buffer_delta_len += sizeof(SparseArray::SegmentDeltaVertex) /8;
            }

            if(out_min_key != nullptr){ // report the minimum in the buffer
                out_min_key->set(vertex.m_vertex_id);
                out_min_key = nullptr;
            }

            m_save_space_used += vertex.m_space_estimated;
            m_save_vertex_index++;
        } else { // write an edge
            auto& edge = m_edges[m_save_edge_index];
            if(edge.m_version == nullptr && !edge.m_is_removed){
                if(last_static_vertex == nullptr || last_static_vertex->m_vertex_id != edge.m_source){
                    last_static_vertex = reinterpret_cast<SparseArray::SegmentStaticVertex*>(buffer_static + buffer_static_len);
                    last_static_vertex->m_vertex_id = edge.m_source;
                    last_static_vertex->m_count = 0;
                    buffer_static_len += sizeof(SparseArray::SegmentStaticVertex) /8;
                }
                last_static_vertex->m_count++;
                auto static_edge = reinterpret_cast<SparseArray::SegmentStaticEdge*>(buffer_static + buffer_static_len);
                static_edge->m_destination = edge.m_destination;
                static_edge->m_weight = edge.m_weight;
                last_static_is_empty_vertex = false;
                buffer_static_len += sizeof(SparseArray::SegmentStaticEdge) /8;
            } else { // delta edge
                auto delta_edge = reinterpret_cast<SparseArray::SegmentDeltaEdge*>(buffer_delta + buffer_delta_len);
                SparseArray::set_edge(delta_edge);
                SparseArray::set_type(delta_edge, edge.m_is_removed);
                SparseArray::set_undo(delta_edge, edge.m_version);
                buffer_delta_len += sizeof(SparseArray::SegmentDeltaEdge) /8;
            }

            if(out_min_key != nullptr){ // report the minimum in the buffer
                out_min_key->set(edge.m_source, edge.m_destination);
                out_min_key = nullptr;
            }

            m_save_space_used += edge.m_space_estimated;
            m_save_edge_index++;
        }
    }

    if(last_static_is_empty_vertex){ // do not write a record <vertex_id, 0>, that is without edges
        buffer_static_len -= sizeof(SparseArray::SegmentStaticVertex) /8;
        m_save_space_used -= last_static_estimated_space;
        m_save_vertex_index--;
    }

    *out_buffer_static_len = buffer_static_len;
    *out_buffer_delta_len = buffer_delta_len;
}

} // namespace
