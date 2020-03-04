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

#pragma once

#include <cinttypes>
#include <vector>

#include "sparse_array.hpp"

namespace teseo::internal::context {
class Undo; // forward decl.
}

namespace teseo::internal::memstore {

class Rebalancer {
    Rebalancer(const Rebalancer&) = delete;
    Rebalancer& operator=(const Rebalancer&) = delete;
    using Undo = teseo::internal::context::Undo;

    struct Vertex {
        const uint64_t m_vertex_id;
        Undo* m_version;
        bool m_is_removed;
        int m_space_estimated;

        Vertex(uint64_t vertex_id, Undo* undo, bool is_removed);
    };

    struct Edge {
        const uint64_t m_source;
        const uint64_t m_destination;
        double m_weight;
        Undo* m_version;
        bool m_is_removed;
        int m_space_estimated;

        Edge(uint64_t source, uint64_t destination, double weight, Undo* undo, bool is_removed);
    };

    SparseArray* m_instance;
    std::vector<Vertex> m_vertices;
    std::vector<Edge> m_edges;
    const uint64_t m_num_segments_total; // total number of segments to write
    int64_t m_space_required = 0; // estimate of the space required

    // Write cursor
    uint64_t m_save_vertex_index = 0;
    uint64_t m_save_edge_index = 0;
    int64_t m_save_space_used = 0;
    int64_t m_num_segments_saved = 0; // number of segments written so far
    uint64_t* m_buffer_static = {nullptr};
    uint64_t* m_buffer_delta = {nullptr};



    void do_load(uint64_t* __restrict static_start, uint64_t* __restrict static_end, uint64_t* __restrict delta_start, uint64_t* __restrict delta_end);
    void do_save(SparseArray::Chunk* chunk, uint64_t segment_id);

    void write_buffers(int64_t target_len, uint64_t* __restrict buffer_static, uint64_t* __restrict buffer_delta, int64_t* out_buffer_static_len, int64_t* out_buffer_delta_len, Key* out_min_key);

    void append_vertex(uint64_t vertex_id, Undo* version, bool is_remove);
    void append_edge(uint64_t source, uint64_t destination, double weight, Undo* version, bool is_remove);


public:
    // Constructor
    // @param instance sparse array instance
    // @param total_num_segments the total number of segments that where the loaded content will be spread over
    Rebalancer(SparseArray* instance, uint64_t total_num_segments);

    // Destructor
    ~Rebalancer();

    // Load the content from the sparse arrays into the scratchpad
    void load(SparseArray::Chunk* chunk);
    void load(SparseArray::Chunk* chunk, uint64_t segment_id);
    void load(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length);

    // Replace delta records with static records, where possible
    void compact();

    // Write the records from the scratchpad to the sparse array
    // NB: the procedure does not update the fence keys of the gates
    void save_init();
    void save(SparseArray::Chunk* chunk);
    void save(SparseArray::Chunk* chunk, uint64_t window_start, uint64_t window_length);
    void save_end();
};


} // namespace

