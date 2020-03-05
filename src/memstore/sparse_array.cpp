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

#include <memstore/rebalancer.hpp>
#include "sparse_array.hpp"

#include <cmath>
#include <mutex>
#include <thread>

#include "context.hpp"
#include "error.hpp"
#include "gate.hpp"
#include "index.hpp"
#include "utility.hpp"

using namespace std;
using namespace teseo::internal;
using namespace teseo::internal::context;

namespace teseo::internal::memstore {

/**
 * Density thresholds, to compute the fill factor of the nodes in the calibrator tree associated to the sparse array/PMA
 * The following constraint must be satisfied: 0 < rho_0 < rho_h <= tau_h < tau_0 <= 1.
 * The magic constants below are based on the the work & experiments described in the paper Packed Memory Arrays - Rewired.
 */
constexpr static double DENSITY_RHO_0 = 0.5; // lower bound, leaf
constexpr static double DENSITY_RHO_H = 0.75; // lower bound, root of the calibrator tree
constexpr static double DENSITY_TAU_H = 0.75; // upper bound, root of the calibrator tree
constexpr static double DENSITY_TAU_0 = 1; // upper bound, leaf

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::lock_guard<std::mutex> lock(g_debugging_mutex); std::cout << "[SparseArray::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
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
SparseArray::SparseArray(uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint) :
        SparseArray(compute_alloc_params(num_qwords_per_segment, num_segments_per_gate, memory_footprint)) { }

SparseArray::SparseArray(InitSparseArrayInfo init) : m_num_gates_per_chunk(init.m_num_gates_per_chunk), m_num_segments_per_lock(init.m_num_segments_per_lock), m_num_qwords_per_segment(init.m_num_qwords_per_segment), m_index(new Index()){

}

SparseArray::InitSparseArrayInfo SparseArray::compute_alloc_params(uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint) {
    COUT_DEBUG("memory_budget: " << memory_footprint << " bytes, segments per gate: " << num_segments_per_gate << ", space per segment: " << num_qwords_per_segment << " qwords");

    if(memory_footprint % 8 != 0) RAISE_EXCEPTION(InternalError, "The memory budget is not a multiple of 8 ")
    if((memory_footprint / 8) < (num_qwords_per_segment * 4)) RAISE_EXCEPTION(InternalError, "The memory budget must be at least 4 times the space per segment");
    if(num_segments_per_gate == 0) RAISE_EXCEPTION(InternalError, "Great, 0 segments per gate");
    if(num_qwords_per_segment == 0) RAISE_EXCEPTION(InternalError, "The space per segment is 0");

    // 1) compute the amount of space required by a single gate and all of its associated segments
    double gate_total_sz = Gate::memory_footprint(num_segments_per_gate *2) + num_segments_per_gate * (sizeof(SegmentMetadata) + (num_qwords_per_segment * 8)); // in bytes
    // 2) solve the inequality LeafSize + x * gate_total_sz >= memory_budget, where x will be our final number of gates
    double num_gates = ceil( (static_cast<double>(memory_footprint) - sizeof(Chunk) ) / gate_total_sz);
    // 3) how many bytes we need to remove to each segment from 'space_per_segment', so that our memory budget is satisfied
    double surplus_total = gate_total_sz * num_gates - memory_footprint;
    double surplus_per_segment = ceil(surplus_total / (num_gates * num_segments_per_gate));
    // 4) compute the new amount of space that can be given to each segment
    uint64_t new_space_per_segment = (num_qwords_per_segment*8) - static_cast<uint64_t>(surplus_per_segment);
    // 5) round up to the previous multiple of 8
    new_space_per_segment -= (new_space_per_segment % 8); // in bytes, including the header


    InitSparseArrayInfo init;
    init.m_num_gates_per_chunk = num_gates;
    init.m_num_segments_per_lock = num_segments_per_gate;
    init.m_num_qwords_per_segment = (new_space_per_segment - sizeof(SegmentMetadata)) / 8;

#if defined(DEBUG)
    COUT_DEBUG("num gates: " << num_gates << ", segments per gate: " << num_segments_per_gate << ", qwords per segment (excl. header): " << init.m_num_qwords_per_segment);
    uint64_t space_used = (Gate::memory_footprint(num_segments_per_gate) + num_segments_per_gate * new_space_per_segment) * num_gates + sizeof(Chunk);
    COUT_DEBUG("space used: " << space_used << "/" << memory_footprint << " bytes (" << (((double) space_used) / memory_footprint) * 100.0 << " %)");
#endif

    return init;
}

SparseArray::~SparseArray() {
    delete m_index; m_index = nullptr;
}

// Allocate a new chunk of the sparse array
SparseArray::Chunk* SparseArray::allocate_chunk(){
    uint64_t space_required = sizeof(Chunk) +
            get_num_gates_per_chunk() * Gate::memory_footprint(get_num_segments_per_lock() *2 /* lhs + rhs */) +
            get_num_segments_per_lock() * (sizeof(SegmentMetadata) + get_num_qwords_per_segment() * 8);


    void* heap { nullptr };
    int rc = posix_memalign(&heap, /* alignment = */ 2097152ull /* 2MB */,  /* size = */ space_required);
    if(rc != 0) throw std::runtime_error("SparseArray::allocate_chunk, cannot obtain a chunk of aligned memory");
    Chunk* chunk = new (heap) Chunk();

    // init the gates
    for(int i = 0; i < get_num_gates_per_chunk(); i++){
        new (get_gate(chunk, i)) Gate(i, get_num_segments_per_lock() /* lhs and rhs */ *2);
//        get_gate(i)->m_space_left = (((uint64_t) num_segments_per_gate) * (space_per_segment - sizeof(Segment))) / 8;
    }

    // init the segments
    for(int i = 0; i < get_num_segments_per_chunk(); i++){
        SegmentMetadata* md = get_segment_metadata(chunk, i);
        md->m_delta1_start = 0;
        md->m_empty1_start = 0;
        md->m_empty2_start = get_num_qwords_per_segment();
        md->m_delta2_start = get_num_qwords_per_segment();
    }

    return chunk;
}

void SparseArray::free_chunk(Chunk* chunk){
    for(int i = 0; i < get_num_gates_per_chunk(); i++){
        Gate* gate = get_gate(chunk, i);
        gate->~Gate();
    }

    chunk->~Chunk();

    free(chunk);
}


/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

uint64_t SparseArray::get_num_gates_per_chunk() const {
    return m_num_gates_per_chunk;
}

uint64_t SparseArray::get_num_segments_per_lock() const {
    return m_num_segments_per_lock;
}

uint64_t SparseArray::get_num_segments_per_chunk() const {
    return get_num_gates_per_chunk() * get_num_segments_per_lock();
}

uint64_t SparseArray::get_num_qwords_per_segment() const {
    return m_num_qwords_per_segment;
}

uint64_t SparseArray::get_num_qwords_per_gate() const {
    static_assert(sizeof(SegmentMetadata) % 8 == 0, "Metadata not aligned to a qword boundary");
    return (get_num_qwords_per_segment() + (sizeof(SegmentMetadata) / 8)) * get_num_segments_per_lock() +
            (Gate::memory_footprint(get_num_segments_per_lock() *2) / 8);
}

Key SparseArray::get_key(const Update& u){
    return Key(u.m_source, u.m_destination);
}

SparseArray::Chunk* SparseArray::get_chunk(const IndexEntry entry){
    return reinterpret_cast<Chunk*>(entry.m_chunk_id);
}

const SparseArray::Chunk* SparseArray::get_chunk(const IndexEntry entry) const {
    return reinterpret_cast<const Chunk*>(entry.m_chunk_id);
}

Gate* SparseArray::get_gate(const Chunk* chunk, uint64_t id) {
    assert(id < get_num_gates_per_chunk() && "Invalid gate_id");
    uint64_t* base_ptr = reinterpret_cast<uint64_t*>(const_cast<Chunk*>(chunk +1));
    return reinterpret_cast<Gate*>(base_ptr + get_num_qwords_per_gate() * id);
}

const Gate* SparseArray::get_gate(const Chunk* chunk, uint64_t id) const {
    assert(id < get_num_gates_per_chunk() && "Invalid gate_id");
    const uint64_t* base_ptr = reinterpret_cast<const uint64_t*>(chunk +1);
    return reinterpret_cast<const Gate*>(base_ptr + get_num_qwords_per_gate() * id);
}

SparseArray::SegmentMetadata* SparseArray::get_segment_metadata(const Chunk* chunk, uint64_t segment_id){
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
    uint64_t gate_id = segment_id / get_num_segments_per_lock();
    uint64_t rel_offset_id = segment_id % get_num_segments_per_lock();

    uint64_t* segment_area = reinterpret_cast<uint64_t*>(get_gate(chunk, gate_id) + 1);
    return reinterpret_cast<SegmentMetadata*>(segment_area + rel_offset_id * (sizeof(SegmentMetadata)/8 + get_num_qwords_per_segment()));
}

const SparseArray::SegmentMetadata* SparseArray::get_segment_metadata(const Chunk* chunk, uint64_t segment_id) const {
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
    uint64_t gate_id = segment_id / get_num_segments_per_lock();
    uint64_t rel_offset_id = segment_id % get_num_segments_per_lock();

    const uint64_t* segment_area = reinterpret_cast<const uint64_t*>(get_gate(chunk, gate_id) + 1);
    return reinterpret_cast<const SegmentMetadata*>(segment_area + rel_offset_id * (sizeof(SegmentMetadata)/8 + get_num_qwords_per_segment()));
}

uint64_t* SparseArray::get_segment_lhs_static_start(const Chunk* chunk, uint64_t segment_id) {
    SegmentMetadata* md1 = get_segment_metadata(chunk, segment_id);
    return reinterpret_cast<uint64_t*>(md1 + 1);
}

const uint64_t* SparseArray::get_segment_lhs_static_start(const Chunk* chunk, uint64_t segment_id) const {
    const SegmentMetadata* md1 = get_segment_metadata(chunk, segment_id);
    return reinterpret_cast<const uint64_t*>(md1 + 1);
}

uint64_t* SparseArray::get_segment_lhs_static_end(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_delta1_start;
}

const uint64_t* SparseArray::get_segment_lhs_static_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_delta1_start;
}

uint64_t* SparseArray::get_segment_lhs_delta_start(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_static_end(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_lhs_delta_start(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_static_end(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_lhs_delta_end(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty1_start;
}

const uint64_t* SparseArray::get_segment_lhs_delta_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty1_start;
}

uint64_t* SparseArray::get_segment_rhs_static_start(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_delta2_start;
}

const uint64_t* SparseArray::get_segment_rhs_static_start(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_delta2_start;
}

uint64_t* SparseArray::get_segment_rhs_static_end(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_static_start(chunk, segment_id) + get_num_qwords_per_segment();
}

const uint64_t* SparseArray::get_segment_rhs_static_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_static_start(chunk, segment_id) + get_num_qwords_per_segment();
}

uint64_t* SparseArray::get_segment_rhs_delta_start(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty2_start;
}

const uint64_t* SparseArray::get_segment_rhs_delta_start(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_static_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty2_start;
}

uint64_t* SparseArray::get_segment_rhs_delta_end(const Chunk* chunk, uint64_t segment_id){
    return get_segment_rhs_static_start(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_rhs_delta_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_rhs_static_start(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_static_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs){
    return is_lhs ? get_segment_lhs_static_start(chunk, segment_id) : get_segment_rhs_static_start(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_static_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_static_start(chunk, segment_id) : get_segment_rhs_static_start(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_static_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs){
    return is_lhs ? get_segment_lhs_static_end(chunk, segment_id) : get_segment_rhs_static_end(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_static_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_static_end(chunk, segment_id) : get_segment_rhs_static_end(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_delta_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) {
    return is_lhs ? get_segment_lhs_delta_start(chunk, segment_id) : get_segment_rhs_delta_start(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_delta_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_delta_start(chunk, segment_id) : get_segment_rhs_delta_start(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_delta_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) {
    return is_lhs ? get_segment_lhs_delta_end(chunk, segment_id) : get_segment_rhs_delta_end(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_delta_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_delta_end(chunk, segment_id) : get_segment_rhs_delta_end(chunk, segment_id);
}

uint64_t SparseArray::get_segment_free_space(const Chunk* chunk, uint64_t segment_id) const {
    const SegmentMetadata* md = get_segment_metadata(chunk, segment_id);
    return md->m_empty2_start - md->m_empty1_start;
}

uint64_t SparseArray::get_segment_used_space(const Chunk* chunk, uint64_t segment_id) const {
    assert(get_segment_free_space(chunk, segment_id) <= get_num_qwords_per_segment());
    return get_num_qwords_per_segment() - get_segment_free_space(chunk, segment_id);
}

bool SparseArray::is_segment_empty(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_used_space(chunk, segment_id) == 0;
}

uint64_t SparseArray::get_gate_free_space(const Chunk* chunk, uint64_t gate_id) const {
    return get_gate_free_space(chunk, get_gate(chunk, gate_id));
}

uint64_t SparseArray::get_gate_free_space(const Chunk* chunk, const Gate* gate) const {
    uint64_t total_space = get_num_qwords_per_segment() * get_num_segments_per_lock();
    uint64_t used_space = get_gate_used_space(chunk, gate);
    assert(total_space >= used_space);
    return total_space - used_space;
}

uint64_t SparseArray::get_gate_used_space(const Chunk* chunk, uint64_t gate_id) const {
    return get_gate_used_space(chunk, get_gate(chunk, gate_id));
}

uint64_t SparseArray::get_gate_used_space(const Chunk* chunk, const Gate* gate) const {
    return gate->m_used_space;
}

int64_t SparseArray::get_cb_height_per_chunk() const {
    return floor(log2(get_num_segments_per_chunk())) +1.0;
}

std::pair<int64_t, int64_t> SparseArray::get_thresholds(int height) const {
    // first compute the density for the given height
    double rho {DENSITY_RHO_0}, tau {DENSITY_TAU_0};
    const int tree_height = get_cb_height_per_chunk();

    // avoid diving by zero
    if(tree_height > 1){
        const double scale = static_cast<double>(tree_height - height) / static_cast<double>(tree_height -1);
        rho = /* max density */ DENSITY_RHO_H - /* delta */ (DENSITY_RHO_H - DENSITY_RHO_0) * scale;
        tau = /* min density */ DENSITY_TAU_H + /* delta */ (DENSITY_TAU_0 - DENSITY_TAU_H) * scale;
    }

    int64_t num_segs = std::min<int64_t>(get_num_segments_per_chunk(), pow(2.0, height -1));
    int64_t space_per_segment = get_num_qwords_per_segment();
    int64_t min_space = num_segs * space_per_segment * rho;
    int64_t max_space = num_segs * (space_per_segment - /* always leave 5 qwords of space in each segment */ 5) * tau;
    if(min_space >= max_space) min_space = max_space - 1;

    return std::make_pair(min_space, max_space);
}


bool SparseArray::is_insert(const SegmentDeltaMetadata* metadata) {
    return metadata->m_insdel == 0;
}

bool SparseArray::is_remove(const SegmentDeltaMetadata* metadata){
    return metadata->m_insdel == 1;
}

bool SparseArray::is_vertex(const SegmentDeltaMetadata* metadata){
    return metadata->m_entity == 0;
}

bool SparseArray::is_edge(const SegmentDeltaMetadata* metadata) {
    return metadata->m_entity == 1;
}

bool SparseArray::is_insert(const Update& update){
    return update.m_update_type == Update::Insert;
}

bool SparseArray::is_remove(const Update& update){
    return update.m_update_type == Update::Remove;
}

bool SparseArray::is_vertex(const Update& update){
    return update.m_entry_type == Update::Vertex;
}

bool SparseArray::is_edge(const Update& update) {
    return update.m_entry_type == Update::Edge;
}

void SparseArray::set_vertex(SegmentDeltaMetadata* metadata){
    metadata->m_entity = 0;
    assert(is_vertex(metadata));
}

void SparseArray::set_edge(SegmentDeltaMetadata* metadata){
    metadata->m_entity = 1;
    assert(is_edge(metadata));
}

void SparseArray::set_type(SegmentDeltaMetadata* metadata, bool is_insert){
    metadata->m_insdel = is_insert ? /*insert*/ 0 : /*remove*/ 1;
}

void SparseArray::set_undo(SegmentDeltaMetadata* metadata, Undo* undo) {
    metadata->m_version = reinterpret_cast<uint64_t>(undo);
}

void SparseArray::reset_header(SegmentDeltaMetadata* metadata, const Update& update){
    if(is_vertex(update)){
        set_vertex(metadata);
    } else {
        set_edge(metadata);
    }
    set_type(metadata, is_insert(update));
    set_undo(metadata, nullptr);
}

SparseArray::SegmentStaticVertex* SparseArray::get_static_vertex(uint64_t* ptr){
    return reinterpret_cast<SegmentStaticVertex*>(ptr);
}

const SparseArray::SegmentStaticVertex* SparseArray::get_static_vertex(const uint64_t* ptr){
    return reinterpret_cast<const SegmentStaticVertex*>(ptr);
}

SparseArray::SegmentStaticEdge* SparseArray::get_static_edge(uint64_t* ptr){
    return reinterpret_cast<SegmentStaticEdge*>(ptr);
}

const SparseArray::SegmentStaticEdge* SparseArray::get_static_edge(const uint64_t* ptr){
    return reinterpret_cast<const SegmentStaticEdge*>(ptr);
}

SparseArray::SegmentDeltaMetadata* SparseArray::get_delta_header(uint64_t* ptr){
    return reinterpret_cast<SegmentDeltaMetadata*>(ptr);
}

const SparseArray::SegmentDeltaMetadata* SparseArray::get_delta_header(const uint64_t* ptr){
    return reinterpret_cast<const SegmentDeltaMetadata*>(ptr);
}

SparseArray::SegmentDeltaVertex* SparseArray::get_delta_vertex(uint64_t* ptr){
    assert(is_vertex(get_delta_header(ptr)));
    return reinterpret_cast<SegmentDeltaVertex*>(ptr);
}

const SparseArray::SegmentDeltaVertex* SparseArray::get_delta_vertex(const uint64_t* ptr){
    assert(is_vertex(get_delta_header(ptr)));
    return reinterpret_cast<const SegmentDeltaVertex*>(ptr);
}

SparseArray::SegmentDeltaEdge* SparseArray::get_delta_edge(uint64_t* ptr){
    assert(is_edge(get_delta_header(ptr)));
    return reinterpret_cast<SegmentDeltaEdge*>(ptr);
}

const SparseArray::SegmentDeltaEdge* SparseArray::get_delta_edge(const uint64_t* ptr){
    assert(is_edge(get_delta_header(ptr)));
    return reinterpret_cast<const SegmentDeltaEdge*>(ptr);
}

Undo* SparseArray::get_delta_undo(uint64_t* ptr){
    return get_delta_undo(get_delta_header(ptr));
}

Undo* SparseArray::get_delta_undo(SegmentDeltaMetadata* ptr){
    return reinterpret_cast<Undo*>(ptr->m_version);
}


SparseArray::IndexEntry SparseArray::index_find(uint64_t vertex_id) const {
    void* result = m_index->find(vertex_id);
    if(result == nullptr){
        return IndexEntry{ 0, 0 };
    } else {
        return *reinterpret_cast<IndexEntry*>(&result);
    }
}

SparseArray::IndexEntry SparseArray::index_find(uint64_t edge_source, uint64_t edge_destination) const {
    void* result = m_index->find(edge_source, edge_destination);
    if(result == nullptr){
        return IndexEntry{ 0, 0 };
    } else {
        return *reinterpret_cast<IndexEntry*>(&result);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/

void SparseArray::insert_vertex(uint64_t vertex_id) {
    Update update;
    update.m_entry_type = Update::Vertex;
    update.m_update_type = Update::Insert;
    update.m_source = vertex_id;
    write(update);
}

void SparseArray::remove_vertex(uint64_t vertex_id) {
    Update update;
    update.m_entry_type = Update::Vertex;
    update.m_update_type = Update::Remove;
    update.m_source = vertex_id;
    write(update);
}

void SparseArray::write(Update update) {
    bool done = false;

    do {
        Chunk* chunk {nullptr};
        Gate* gate {nullptr};

        try {
            ScopedEpoch epoch;

            // Acquire an xlock to the gate we're going to alter
            std::tie(chunk, gate) = writer_on_entry(update);
            assert(chunk != nullptr && gate != nullptr);

            // Perform the update, unless the gate is full
            bool is_update_done = do_write_gate(chunk, gate, update);

            // Rebalance the chunk then
            if(!is_update_done){
                rebalance_chunk(chunk, gate);
                // we still need to perform the update ...
            } else {
                writer_on_exit(chunk, gate);
                done = true;
            }

        } catch (::teseo::Exception& e){
            if(gate != nullptr){
                writer_on_exit(chunk, gate); // release the gate
                gate = nullptr;
            }
            throw;
        } catch (Abort) { /* nop */ }
    } while(!done);
}

auto SparseArray::writer_on_entry(const Update& update) -> std::pair<Chunk*, Gate*> {
    ThreadContext* context = thread_context();
    assert(context != nullptr);
    context->epoch_enter();

    IndexEntry leaf_addr = index_find(update.m_source, update.m_destination);
    Chunk* chunk = get_chunk(leaf_addr);
    int64_t gate_id = leaf_addr.m_gate_id;
    Gate* gate = nullptr;
    Key search_key(get_key(update));

    bool done = false;

    do {
        Key index_key;

        gate = get_gate(chunk, gate_id);

        unique_lock<Gate> lock(*gate);
        auto direction = gate->check_fence_keys(search_key);
        switch(direction){
        case Gate::Direction::LEFT:
            gate_id--;
            if(gate_id < 0){ throw Abort{}; } // go to the previous leaf
            break;
        case Gate::Direction::RIGHT:
            gate_id++;
            if(gate_id >= get_num_gates_per_chunk()){ throw Abort{}; } // go to the next leaf
            break;
        case Gate::Direction::INVALID:
            throw Abort{}; // restart from scratch
            break;
        case Gate::Direction::GO_AHEAD: {
            switch(gate->m_state){
            case Gate::State::FREE:
                assert(gate->m_num_active_threads == 0 && "Precondition not satisfied");
                gate->m_state = Gate::State::WRITE;
                gate->m_num_active_threads = 1;

                done = true; // done, proceed with the insertion
                break;
            case Gate::State::READ:
            case Gate::State::WRITE:
            case Gate::State::REBAL:
                writer_wait(*gate, lock);
            }
        } break;
        }
    } while (!done);

    return std::make_pair(chunk, gate);
}

template<typename Lock>
void SparseArray::writer_wait(Gate& gate, Lock& lock) {
    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();
    gate.m_queue.append({ Gate::State::WRITE, &producer } );
    lock.unlock();
    consumer.wait();
}

void SparseArray::writer_on_exit(Chunk* chunk, Gate* gate){
    assert(gate != nullptr);

    gate->lock();
    gate->m_num_active_threads = 0;

    switch(gate->m_state){
    case Gate::State::WRITE:
        // same state as before
        gate->m_state = Gate::State::FREE;
        break;
    case Gate::State::REBAL:
        // the rebalancer wants to process this gate => nop
        break;
    default:
        assert(0 && "Invalid state");
    }

    gate->wake_next();
    gate->unlock();
}

bool SparseArray::do_write_gate(Chunk* chunk, Gate* gate, Update& update) {
    COUT_DEBUG("Gate: " << gate->id() << ", update: " << update);

    uint64_t g2sid = gate->find(get_key(update));
    uint64_t segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
    uint64_t is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment

    bool is_update_done = do_write_segment(chunk, gate, segment_id, is_lhs, update);

    if(!is_update_done){ // try to rebalance locally, inside the gate
        bool rebalance_done = rebalance_gate(chunk, gate, segment_id);
        if(!rebalance_done) return false;

        // try again
        g2sid = gate->find(get_key(update));
        segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
        is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment
        is_update_done = do_write_segment(chunk, gate, segment_id, is_lhs, update);
    }

    return true;
}

/*****************************************************************************
 *                                                                           *
 *   Global rebalance (chunk)                                                *
 *                                                                           *
 *****************************************************************************/
void SparseArray::rebalance_chunk(Chunk* chunk, Gate* gate){
    // first all, check whether we can rebalance this chunk
    bool can_global_rebal = chunk->m_latch.try_lock_write(); // acquire the global latch for the chunk
    if(!can_global_rebal) {
        writer_on_exit(chunk, gate);
        return; // try again ...
    }

    gate->lock();
    assert(gate->m_state == Gate::State::WRITE);
    gate->m_state = Gate::State::REBAL;
    gate->unlock();

    int64_t gate_window_start { 0 }, gate_window_length { 0 };
    bool do_rebalance = rebalance_chunk_find_window(chunk, gate, &gate_window_start, &gate_window_length);

    // Load the elements to rebalance
    int64_t window_start = gate_window_start * get_num_segments_per_lock();
    int64_t window_length = gate_window_length * get_num_segments_per_lock();
    int64_t final_length = do_rebalance ? window_length : /* resize */ get_num_segments_per_chunk() * 2;

    Rebalancer spad { this, (uint64_t) final_length };
    spad.load(chunk, window_start, window_length);
    spad.compact();

    Chunk* sibling {nullptr};

    if(do_rebalance){
        spad.save(chunk, window_start, window_length);
        rebalance_chunk_update_fence_keys(chunk, gate_window_start, gate_window_length);

    } else { // split leaf
        assert(window_start == 0);
        assert(window_length == get_num_segments_per_chunk());
        sibling = allocate_chunk();
        spad.save(chunk, 0, get_num_segments_per_chunk());
        spad.save(sibling, 0, get_num_segments_per_chunk());

        // Fence keys
        rebalance_chunk_update_fence_keys(chunk, 0, get_num_gates_per_chunk());
        rebalance_chunk_update_fence_keys(sibling, 0, get_num_gates_per_chunk());
        Gate* previous = get_gate(chunk, get_num_gates_per_chunk() -1);
        Gate* next = get_gate(sibling, 0);
        Key min = get_minimum(sibling, 0);
        previous->m_fence_high_key = next->m_fence_low_key = min;
    }

    // Release the acquired gates
    for(uint64_t gate_id = gate_window_start, end = gate_window_start + gate_window_length; gate_id < end; gate_id++){
        rebalance_chunk_release_lock(chunk, gate_id);
    }

    chunk->m_latch.unlock_write();
}


bool SparseArray::rebalance_chunk_find_window(Chunk* chunk, Gate* gate, int64_t* out_gate_window_start, int64_t* out_gate_window_length) {
    bool do_rebalance = false;
    double height { 0. }; // current height at the calibrator tree
    int64_t lock_start = gate->id(); // the start of the window, in terms of gates
    int64_t lock_length = 1; // the length of the window, in terms of number of gates
    int64_t index_left = lock_start -1; // next gate to read from the left
    int64_t index_right = lock_start + lock_length; // next gate to read from the right
    int64_t space_filled = get_gate_used_space(chunk, gate); // how many slots have been filled in the window so far. 1 slot = 1 qword = 8 bytes
    std::vector<std::promise<void>> threads2wait; // list of threads (readers/writers) that are currently owning a gate and we need to wait before performing the rebalance

    while(!do_rebalance && lock_length <= get_num_gates_per_chunk()){
        height = log2(lock_length) +1.;

        // readjust the window
        int64_t lock_start_new = (gate->id() / static_cast<int64_t>(pow(2, (height -1)))) * lock_length;
        if(lock_start_new + lock_length >= get_num_gates_per_chunk()){
            lock_start_new = get_num_gates_per_chunk() - lock_length;
        }

        COUT_DEBUG("(begin iteration) height: " << height << ", previous start position: " << lock_start << ", new start position: " << lock_start_new << ", window: [" << lock_start_new << ", " << lock_start_new + lock_length << ")");
        assert(lock_start_new <= lock_start);
        lock_start = lock_start_new;
        int64_t lock_end = lock_start + lock_length;

        // read the amount of space filled
        int64_t index = lock_end -1;
        while(index >= index_right){
            space_filled += rebalance_chunk_acquire_lock(chunk, index, /* inout */ threads2wait);
            index--;
        }
        index_right = lock_end; // for the next round
        index = index_left;
        while(index >= lock_start){
            space_filled += rebalance_chunk_acquire_lock(chunk, index, /* inout */ threads2wait);
        }
        index_right = lock_end; // for the next round

        // compute the density
        height = log2(get_num_segments_per_lock() * lock_length) +1.;
        int64_t min_space_filled { 0 }, max_space_filled { 0 };
        std::tie(min_space_filled, max_space_filled) = get_thresholds(height);
        if(space_filled <= max_space_filled){
            do_rebalance = true;
        } else {
            // next window
            if(lock_length == get_num_segments_per_chunk()) break;
            lock_length = std::min<int64_t>( lock_length *= 2, get_num_segments_per_chunk() );
        }

    }

    // wait for the threads in the wait list to leave their gate
    for(auto& consumer : threads2wait){
        auto f = consumer.get_future(); // allocate the future object
        f.get(); // wait to be released by the other reader/writer
    }

    *out_gate_window_start = lock_start;
    *out_gate_window_length = lock_length;
    return do_rebalance;
}



int64_t SparseArray::rebalance_chunk_acquire_lock(Chunk* chunk, uint64_t gate_id, std::vector<std::promise<void>>& waitlist){
    assert(chunk != nullptr && "Null pointer");
    assert(gate_id < get_num_gates_per_chunk() && "Overflow");
    Gate* gate = get_gate(chunk, gate_id);

    gate->lock();
    uint64_t space_filled = gate->m_used_space;
    switch(gate->m_state){
    case Gate::State::FREE:
        gate->m_state = Gate::State::REBAL;
        break;
    case Gate::State::WRITE:
        // if a writer is currently processing a gate, then the (pessimistic) assumption is that it's going to add a new single entry
        space_filled += std::max(sizeof(SegmentDeltaVertex) / 8, sizeof(SegmentDeltaEdge) / 8);
        // continue to the next statement, ignore the warning
    case Gate::State::READ:
        waitlist.emplace_back();
        gate->m_queue.prepend({ Gate::State::REBAL, & waitlist.back() } );
        break;
    default:
        assert(0 && "Unexpected case");
    }

    gate->unlock();

    return space_filled;
}


void SparseArray::rebalance_chunk_release_lock(Chunk* chunk, uint64_t gate_id){
    assert(gate_id < get_num_gates_per_chunk() && "Invalid gate/lock ID");
    Gate* gate = get_gate(chunk, gate_id);

    // acquire the spin lock associated to this gate
    gate->lock();
    assert(gate->m_state == Gate::State::REBAL && "This gate was supposed to be acquired previously");
    assert(gate->m_num_active_threads == 0 && "This gate should be closed for rebalancing");

    gate->m_state = Gate::State::FREE;
    // gate->m_time_last_rebal = time_last_rebal; // There is no Timeout manager in this impl~

    // Use #wake_all rather than #wake_next! Potentially the fence keys have been changed, threads
    // upon wake up might move to other gates. If there are other threads in the wait list, they
    // might potentially end up blocked forever.
    gate->wake_all();

    // done
    gate->unlock();
}


void SparseArray::rebalance_chunk_update_fence_keys(Chunk* chunk, uint64_t gate_window_start, uint64_t gate_window_length){
    Gate* previous = get_gate(chunk, gate_window_start);
    for(int64_t i = 1; i < gate_window_length; i++){
        Gate* next = get_gate(chunk, gate_window_start + i);
        uint64_t segment_id = (gate_window_start + i) * get_num_segments_per_lock();
        Key next_min = get_minimum(chunk, segment_id);
        previous->m_fence_high_key = next_min;
        next->m_fence_low_key = next_min;

        previous = next;
    }
}


Key SparseArray::get_minimum(const Chunk* chunk, uint64_t segment_id) const {
    if(is_segment_empty(chunk, segment_id)) return KEY_MIN;

    // start with the left hand side of the segment
    const uint64_t* __restrict lhs_static_start = get_segment_lhs_static_start(chunk, segment_id);
    const uint64_t* __restrict lhs_static_end = get_segment_lhs_static_end(chunk, segment_id);
    const uint64_t* __restrict lhs_delta_start = get_segment_lhs_delta_start(chunk, segment_id);
    const uint64_t* __restrict lhs_delta_end = get_segment_lhs_delta_end(chunk, segment_id);

    if(lhs_static_start != lhs_static_end && lhs_delta_start != lhs_delta_end){ // otherwise this section is empty
        Key lhs_static_min = KEY_MAX;
        Key lhs_delta_min = KEY_MAX;

        // Read the first entry from the static part
        if(lhs_static_start < lhs_static_end){
            const SegmentStaticVertex* vertex = get_static_vertex(lhs_static_start);
            if(vertex->m_first){
                lhs_static_min = Key{ vertex->m_vertex_id };
            } else {
                // as this is not the first vertex in the chain, then it must contain some edges
                assert(vertex->m_count > 0);
                const SegmentStaticEdge* edge = get_static_edge(lhs_static_start + sizeof(SegmentStaticVertex)/8);
                lhs_static_min = Key { vertex->m_vertex_id, edge->m_destination };
            }
        }

        // Read the first entry from the delta section
        if(lhs_delta_start < lhs_delta_end){
            const SegmentDeltaMetadata* descr = get_delta_header(lhs_delta_start);
            if(is_vertex(descr)){
                lhs_delta_min = Key{ get_delta_vertex(lhs_delta_start)->m_vertex_id };
            } else {
                const SegmentDeltaEdge* edge = get_delta_edge(lhs_delta_start);
                lhs_delta_min = Key { edge->m_source, edge->m_destination };
            }
        }

        assert(lhs_static_min != KEY_MAX || lhs_delta_min != KEY_MAX); // otherwise this section wouldn't have been empty
        return lhs_static_min < lhs_delta_min ? lhs_static_min : lhs_delta_min;
    }

    // If we're at this point, then the LHS of this segment is empty
    assert(lhs_static_start == lhs_static_end && lhs_delta_start == lhs_delta_end);
    const uint64_t* __restrict rhs_static_start = get_segment_rhs_static_start(chunk, segment_id);
    const uint64_t* __restrict rhs_static_end = get_segment_rhs_static_end(chunk, segment_id);
    const uint64_t* __restrict rhs_delta_start = get_segment_rhs_delta_start(chunk, segment_id);
    const uint64_t* __restrict rhs_delta_end = get_segment_rhs_delta_end(chunk, segment_id);
    assert(rhs_static_start != rhs_static_end || rhs_delta_start != rhs_delta_end); // otherwise this segment would be empty

    Key rhs_static_min = KEY_MAX;
    Key rhs_delta_min = KEY_MAX;

    // as above, read the first entry from the static part
    if(rhs_static_start < rhs_static_end){
        const SegmentStaticVertex* vertex = get_static_vertex(rhs_static_start);
        if(vertex->m_first){
            rhs_static_min = Key{ vertex->m_vertex_id };
        } else {
            // as this is not the first vertex in the chain, then it must contain some edges
            assert(vertex->m_count > 0);
            const SegmentStaticEdge* edge = get_static_edge(rhs_static_start + sizeof(SegmentStaticVertex)/8);
            rhs_static_min = Key { vertex->m_vertex_id, edge->m_destination };
        }
    }

    // read the first item in the delta section
    if(rhs_delta_start < rhs_delta_end){
        const SegmentDeltaMetadata* descr = get_delta_header(rhs_delta_start);
        if(is_vertex(descr)){
            rhs_delta_min = Key{ get_delta_vertex(rhs_delta_start)->m_vertex_id };
        } else {
            const SegmentDeltaEdge* edge = get_delta_edge(rhs_delta_start);
            rhs_delta_min = Key { edge->m_source, edge->m_destination };
        }
    }

    assert(rhs_static_min != KEY_MAX || rhs_delta_min != KEY_MAX); // otherwise this section wouldn't have been empty
    return rhs_static_min < rhs_delta_min ? rhs_static_min : rhs_delta_min;
}

/*****************************************************************************
 *                                                                           *
 *   Local rebalance (gate)                                                  *
 *                                                                           *
 *****************************************************************************/
bool SparseArray::rebalance_gate(Chunk* chunk, Gate* gate, uint64_t segment_id) {
    // find whether we can rebalance this gate
    int64_t window_start = gate->window_start() /2; // lhs + rhs
    int64_t window_length = gate->window_length() /2;
    bool can_rebalance = rebalance_gate_find_window(chunk, gate, segment_id, &window_start, &window_length);
    if(!can_rebalance) return false;

    // Rebalance the gate
    Rebalancer spad { this, (uint64_t) window_length };
    spad.load(chunk, window_start, window_length);
    spad.compact();
    spad.save(chunk, window_start, window_length);

    // As the delta records have been compacted, update the amount of used space in the gate
    uint64_t used_space = 0;
    for(uint64_t i = gate->id() * get_num_segments_per_lock(), end = i + get_num_segments_per_lock(); i < end; i++){
        used_space += get_segment_used_space(chunk, i);
    }
    gate->m_used_space = used_space;

    return true;
}

bool SparseArray::rebalance_gate_find_window(Chunk* chunk, Gate* gate, uint64_t segment_id, int64_t* inout_window_start, int64_t* inout_window_length) const {
    assert(inout_window_start != nullptr && inout_window_length != nullptr);
    const int64_t max_window_start = *inout_window_start; // inclusive
    const int64_t max_window_end = *inout_window_start + *inout_window_length; // exclusive

    int64_t window_length = 1;
    int64_t window_id = segment_id;
    int64_t window_start = segment_id /* incl */, window_end = segment_id +1 /* excl */;
    int64_t space_filled = get_segment_used_space(chunk, segment_id);
    int height = 1;
    int max_height = floor(log2(max_window_end - max_window_start)) +1.0;
    int64_t min_space_filled = 0, max_space_filled = numeric_limits<int64_t>::max();

    // determine the window to rebalance
    if(get_cb_height_per_chunk() > 1){
        int64_t index_left = segment_id -1;
        int64_t index_right = segment_id + 1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;

            // re-align the calibrator tree
            if(window_end > max_window_end){
                int64_t offset = window_end - max_window_end;
                window_start -= offset;
                window_end -= offset;
                if(window_start < max_window_start){ window_start = max_window_start; }
            } else if (window_start < max_window_start){
                int64_t offset = max_window_start - window_start;
                window_start += offset;
                window_end += offset;
                if(window_end > max_window_end){ window_end = max_window_end; }
            }

            // find the number of elements in the interval
            while(index_left >= window_start){
                space_filled += get_segment_used_space(chunk, index_left);
                index_left--;
            }
            while(index_right < window_end){
                space_filled += get_segment_used_space(chunk, index_right);
                index_right++;
            }

            std::tie(min_space_filled, max_space_filled) = get_thresholds(height);

        } while(space_filled > max_space_filled && height < max_height);
    }

    COUT_DEBUG("min space: " << min_space_filled << ", space filled: " << space_filled << ", max space: " << max_space_filled << ", height: " << height << ", max height: " << max_height);

    if(space_filled <= max_space_filled){ // spread
        *inout_window_start = window_start;
        *inout_window_length = window_end - window_start;
        return true;
    } else { // resize/split
        return false;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Raw writes in the segment                                               *
 *                                                                           *
 *****************************************************************************/

bool SparseArray::do_write_segment(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update){
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
    uint64_t empty_space = get_segment_free_space(chunk, segment_id);

    if(is_vertex(update)){
        // we need at least 2 qwords of empty space
        if(empty_space < (sizeof(SegmentDeltaVertex) / 8)) return false;

        do_write_segment_vertex(chunk, gate, segment_id, is_lhs, update);
        return true;
    } else {
        assert(update.m_entry_type == Update::Edge);
        if(empty_space < (sizeof(SegmentDeltaEdge) / 8)) return false;

        do_write_segment_edge(chunk, gate, segment_id, is_lhs, update);
        return true;
    }
}

void SparseArray::do_write_segment_vertex(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update) {
    assert(get_segment_free_space(chunk, segment_id) >= sizeof(SegmentDeltaVertex) / 8); // Otherwise there isn't enough space to create a delta entry
    const uint64_t vertex_id = update.m_source;

    // pointers to the static & delta portions of the segment
    SegmentMetadata* __restrict segmentcb = get_segment_metadata(chunk, segment_id);
    uint64_t* __restrict static_start = get_segment_static_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict static_end = get_segment_static_end(chunk, segment_id, is_lhs);
    uint64_t* __restrict delta_start = get_segment_delta_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict delta_end = get_segment_delta_end(chunk, segment_id, is_lhs);

    // The new record we're going to add
    SegmentDeltaVertex* ptr_record { nullptr } ;

    // first, find in the delta where to store the new record
    bool stop = false; uint64_t delta_pos = 0, end = delta_end - delta_start;
    while(delta_pos < end && !stop){
        auto header = get_delta_header(delta_start + delta_pos);
        if(is_vertex(header)){
            auto delta_vertex = get_delta_vertex(delta_start + delta_pos);
            if(delta_vertex->m_vertex_id < vertex_id ){
                delta_pos += sizeof(SegmentDeltaVertex) / 8; // move ahead
            } else if ( delta_vertex->m_vertex_id == vertex_id ){
                if(! transaction()->can_write(get_delta_undo(delta_vertex)) ){
                    RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the vertex ID " << vertex_id << " is currently locked by another transaction. Restart this transaction to alter this object");
                } else if( is_insert(update) && is_insert(delta_vertex) ){
                    RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " already exists");
                } else if( is_remove(update) && is_remove(delta_vertex) ){
                    RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " does not exist");
                }

                ptr_record = delta_vertex;
            }

            stop = delta_vertex->m_vertex_id >= vertex_id;
        } else { // it's an edge
            assert(is_edge(header));
            auto delta_edge = get_delta_edge(delta_start + delta_pos);

            if(delta_edge->m_source < vertex_id){
                delta_pos += sizeof(SegmentDeltaEdge) / 8; // move ahead
            } else {
                stop = true;
            }
        }
    }


    // second, check the static storage to see whether the vertex to insert already exists
    if(ptr_record == nullptr){
        bool is_vertex_in_static_storage = false;
        stop = false; uint64_t static_pos = 0; end = static_end - static_start;
        while(!stop && static_pos < end){
            auto static_vertex = get_static_vertex(static_start + static_pos);
            if(static_vertex->m_vertex_id < vertex_id){
                static_pos += sizeof(SegmentStaticVertex)/8 + static_vertex->m_count * sizeof(SegmentStaticEdge)/8;
            } else if (static_vertex->m_vertex_id == vertex_id){
                is_vertex_in_static_storage = true;
                if(is_insert(update)){
                    RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " already exists");
                }

                stop = true;
            } else {
                stop = true;
            }
        }
    }

    // corner case: this is the same transaction that is reverting a change previously performed by itself
    if(ptr_record != nullptr && transaction()->owns(get_delta_undo(ptr_record))){

        Undo* undo = get_delta_undo(ptr_record);
        Undo* next_undo = undo->next();
        if(next_undo == nullptr){ // there are no further changes in the chain, simply remove the delta record from the segment
            if(is_lhs){
                memmove(delta_start + delta_pos, delta_start + delta_pos + sizeof(SegmentDeltaVertex),
                        static_cast<int64_t>(segmentcb->m_empty1_start - delta_pos) * 8);
                segmentcb->m_empty1_start -= sizeof(SegmentDeltaVertex) / 8;
            } else {
                memmove(delta_start, delta_start - sizeof(SegmentDeltaVertex),
                        static_cast<int64_t>(delta_pos - segmentcb->m_empty2_start) * 8);
                segmentcb->m_empty2_start += sizeof(SegmentDeltaVertex) / 8;
            }

            gate->m_used_space -= sizeof(SegmentDeltaVertex) / 8;
        } else {
            { // otherwise, restore the previous entry in the undo chain
                Transaction* next_tx = next_undo->transaction();
                assert(next_tx != nullptr && next_tx->is_terminated());
                WriteLatch xlock(next_tx->latch());
                next_undo->mark_first(this);
            }

            Update* next_update = reinterpret_cast<Update*>(next_undo->payload());
            set_type(ptr_record, is_insert(*next_update));
            set_undo(ptr_record, next_undo);
        }
        undo->ignore();

    // standard case: we are inserting/removing a vertex, not locked by this or another transaction
    } else {
        Undo* prev_undo = ptr_record == nullptr ? nullptr : get_delta_undo(ptr_record);

        // do we need to create a new delta chain?
        if(ptr_record == nullptr){
            if(is_lhs){ // push subsequent records ahead
                memmove(delta_start + delta_pos + sizeof(SegmentDeltaVertex), delta_start + delta_pos,
                        static_cast<int64_t>(segmentcb->m_empty1_start - delta_pos) * 8);
                segmentcb->m_empty1_start += sizeof(SegmentDeltaVertex) / 8;
            } else { // move previous records back
                memmove(delta_start - sizeof(SegmentDeltaVertex), delta_start,
                        static_cast<int64_t>(delta_pos - segmentcb->m_empty2_start) * 8);
                segmentcb->m_empty2_start -= sizeof(SegmentDeltaVertex) / 8;
            }
            gate->m_used_space += sizeof(SegmentDeltaVertex) / 8;

            ptr_record = get_delta_vertex(delta_start + delta_pos);
            ptr_record->m_vertex_id = vertex_id;
        }

        reset_header(ptr_record, update);

        // Transaction management
        set_undo(ptr_record, transaction()->add_undo(
                /* data structure */ this,
                /* next change */ prev_undo,
                /* type */ UndoType::SparseArrayUpdate,
                /* payload */ update)
        );
    }
}

void SparseArray::do_write_segment_edge(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update) {
    assert(get_segment_free_space(chunk, segment_id) >= sizeof(SegmentDeltaEdge) / 8); // Otherwise there isn't enough space to create a delta entry

    // pointers to the static & delta portions of the segment
    SegmentMetadata* __restrict segmentcb = get_segment_metadata(chunk, segment_id);
    uint64_t* __restrict static_start = get_segment_static_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict static_end = get_segment_static_end(chunk, segment_id, is_lhs);
    uint64_t* __restrict delta_start = get_segment_delta_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict delta_end = get_segment_delta_end(chunk, segment_id, is_lhs);

    // The new record we're going to add
    SegmentDeltaEdge* ptr_record { nullptr } ;

    // first, find in the delta where to store the new record
    bool stop = false; uint64_t delta_pos = 0, end = delta_end - delta_start;
    while(delta_pos < end && !stop){
        auto header = get_delta_header(delta_start + delta_pos);
        if(is_vertex(header)){
            auto delta_vertex = get_delta_vertex(delta_start + delta_pos);
            if(delta_vertex->m_vertex_id <= update.m_source ){
                delta_pos += sizeof(SegmentDeltaVertex) / 8; // move ahead
            } else {
                stop = true;
            }
        } else { // it's an edge
            assert(is_edge(header));
            auto delta_edge = get_delta_edge(delta_start + delta_pos);

            if(delta_edge->m_source < update.m_source || (delta_edge->m_source == update.m_source && delta_edge->m_destination < update.m_destination)){
                delta_pos += sizeof(SegmentDeltaEdge) / 8; // move ahead
            } else if (delta_edge->m_source == update.m_source){
                if(!transaction()->can_write(get_delta_undo(header))){
                    RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the edge " << update.m_source << " -> " <<
                            update.m_destination << " is currently locked by another transaction. Restart this transaction to alter this object");
                } else if( is_insert(update) && is_insert(delta_edge) ){
                    RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " already exists");
                } else if( is_remove(update) && is_remove(delta_edge) ){
                    RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " does not exist");
                }

                ptr_record = delta_edge;
                stop = true;
            } else {
                stop = true;
            }
        }
    }

    // corner case: this is the same transaction that is reverting a change previously performed by itself
    if(ptr_record != nullptr && transaction()->owns(get_delta_undo(ptr_record))){
        Undo* undo = get_delta_undo(ptr_record);
        Undo* next_undo = undo->next();
        if(next_undo == nullptr){ // there are no further changes in the chain, simply remove the delta record from the segment
            if(is_lhs){
                memmove(delta_start + delta_pos, delta_start + delta_pos + sizeof(SegmentDeltaVertex),
                        static_cast<int64_t>(segmentcb->m_empty1_start - delta_pos) * 8);
                segmentcb->m_empty1_start -= sizeof(SegmentDeltaEdge) / 8;
            } else {
                memmove(delta_start, delta_start - sizeof(SegmentDeltaVertex),
                        static_cast<int64_t>(delta_pos - segmentcb->m_empty2_start) * 8);
                segmentcb->m_empty2_start += sizeof(SegmentDeltaEdge) / 8;
            }

            gate->m_used_space -= sizeof(SegmentDeltaEdge) / 8;

        } else {
            { // otherwise, restore the previous entry in the undo chain
                Transaction* next_tx = next_undo->transaction();
                assert(next_tx != nullptr && next_tx->is_terminated());
                WriteLatch xlock(next_tx->latch());
                next_undo->mark_first(this);
            }

            Update* next_update = reinterpret_cast<Update*>(next_undo->payload());
            set_type(ptr_record, is_insert(*next_update));
            set_undo(ptr_record, next_undo);
        }
        undo->ignore();

    } else { // standard case: we are inserting/removing an edge, not locked by this or another transaction
        Undo* next_undo { nullptr };

        if(ptr_record == nullptr){ // before creating a new delta change:
            // in case of an insertion => check the edge does not exist yet
            // in case of a deletion => check the edge already exists
            bool stop = false; bool edge_found = false;
            uint64_t static_pos = 0, end = static_end - static_start;
            while(static_pos < end && !stop){
                auto static_vertex = get_static_vertex(static_start + static_pos);
                if(static_vertex->m_vertex_id < update.m_source){ // move ahead
                    static_pos += (sizeof(SegmentStaticVertex) / 8) + (static_vertex->m_count * sizeof(SegmentStaticEdge)) / 8;
                } else if (static_vertex->m_vertex_id == update.m_source){ // good check the edges
                    static_pos += sizeof(SegmentStaticVertex) /8;
                    end = static_pos + (static_vertex->m_count * sizeof(SegmentStaticEdge)) / 8;
                    while(!stop && static_pos < end){
                        SegmentStaticEdge* edge = get_static_edge(static_start + static_pos);
                        if(edge->m_destination < update.m_destination){ // move ahead
                            static_pos += sizeof(SegmentStaticEdge) /8;
                        } else if(edge->m_destination == update.m_destination){
                            edge_found = true;
                        }
                        stop = edge->m_destination >= update.m_destination;
                    }
                } else { // we already went over
                    stop = true;
                }
            }

            // consistency check
            if(edge_found && is_insert(update)){
                RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " already exists");
            } else if (!edge_found && !is_remove(update)){
                RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " does not exist");
            }

            // create a new delta record
            if(is_lhs){ // push subsequent records ahead
                memmove(delta_start + delta_pos + sizeof(SegmentDeltaEdge), delta_start + delta_pos,
                        static_cast<int64_t>(segmentcb->m_empty1_start - delta_pos) * 8);
                segmentcb->m_empty1_start += sizeof(SegmentDeltaEdge) / 8;
            } else { // move previous records back
                memmove(delta_start - sizeof(SegmentDeltaEdge), delta_start,
                        static_cast<int64_t>(delta_pos - segmentcb->m_empty2_start) * 8);
                segmentcb->m_empty2_start -= sizeof(SegmentDeltaEdge) / 8;
            }
            gate->m_used_space += sizeof(SegmentDeltaEdge) / 8;
            ptr_record = get_delta_edge(delta_start + delta_pos);
            ptr_record->m_source = update.m_source;
            ptr_record->m_destination = update.m_destination;
            ptr_record->m_weight = update.m_weight;
        } else {
            next_undo = get_delta_undo(ptr_record);
        }

        reset_header(ptr_record, update);

        // Transaction management
        set_undo(ptr_record,
            transaction()->add_undo(
                /* data structure */ this,
                /* next change */ next_undo,
                /* type */ UndoType::SparseArrayUpdate,
                /* payload */ update
            )
        );
    }
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void SparseArray::dump_undo(void* undo_payload) const {
    cout << *( reinterpret_cast<const SparseArray::Update*>(undo_payload) );
}

std::ostream& operator<<(std::ostream& out, const SparseArray::Update& update){
    if(update.m_update_type == SparseArray::Update::Insert){
        out << "Insert ";
    } else {
        out << "Remove ";
    }
    if(update.m_entry_type == SparseArray::Update::Vertex){
        out << "vertex " << update.m_source;
    } else {
        out << "edge " << update.m_source << " -> " << update.m_destination << " (weight: " << update.m_weight << ")";
    }
    return out;
}



} // namespace

