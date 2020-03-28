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

#include "sparse_array.hpp"

#include <cmath>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

#include "util/miscellaneous.hpp"
#include "context.hpp"
#include "error.hpp"
#include "gate.hpp"
#include "index.hpp"
#include "rebalancer.hpp"
#include "splock.hpp"

using namespace std;
using namespace teseo::internal;
using namespace teseo::internal::context;
using namespace teseo::internal::util;

namespace teseo::internal::memstore {

struct ConsistencyCheckFailed { };

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
SparseArray::SparseArray(bool is_directed, uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint) :
        SparseArray(compute_alloc_params(is_directed, num_qwords_per_segment, num_segments_per_gate, memory_footprint)) { }

SparseArray::SparseArray(InitSparseArrayInfo init) : m_is_directed(init.m_is_directed), m_num_gates_per_chunk(init.m_num_gates_per_chunk), m_num_segments_per_lock(init.m_num_segments_per_lock), m_num_qwords_per_segment(init.m_num_qwords_per_segment), m_index(new Index()){
    COUT_DEBUG("qwords per segment (excl header): " << get_num_qwords_per_segment());
    COUT_DEBUG("num segments per gate: " << get_num_segments_per_lock());
    COUT_DEBUG("qwords per gate: " << get_num_qwords_per_gate());

    // Create an empty leaf
    Chunk* chunk = allocate_chunk();
    // get_gate(chunk, 0)->set_separator_key(0, KEY_MIN); // there is no sep. key for segment 0 => it uses fence_low_key
    get_gate(chunk, 0)->m_fence_low_key = KEY_MIN;
    ScopedEpoch epoch; // before operating in the index, we always must have already a thread context and a gc running ...
    index_insert(KEY_MIN, chunk, 0);
}

SparseArray::InitSparseArrayInfo SparseArray::compute_alloc_params(bool is_directed, uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint) {
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
    init.m_is_directed = is_directed;
    init.m_num_gates_per_chunk = num_gates;
    init.m_num_segments_per_lock = num_segments_per_gate;
    init.m_num_qwords_per_segment = (new_space_per_segment - sizeof(SegmentMetadata)) / 8;

#if defined(DEBUG)
    COUT_DEBUG("gate static size: " << sizeof(Gate) << " bytes, including " << (num_segments_per_gate *2) << " separator keys: " << Gate::memory_footprint(num_segments_per_gate*2) << " bytes");
    COUT_DEBUG("num gates: " << num_gates << ", segments per gate: " << num_segments_per_gate << ", qwords per segment (excl. header): " << init.m_num_qwords_per_segment);
    uint64_t space_used = (Gate::memory_footprint(num_segments_per_gate *2) + num_segments_per_gate * new_space_per_segment) * num_gates + sizeof(Chunk);
    COUT_DEBUG("space used: " << space_used << "/" << memory_footprint << " bytes (" << (((double) space_used) / memory_footprint) * 100.0 << " %)");
#endif

    return init;
}

SparseArray::~SparseArray() {
    delete m_index; m_index = nullptr;
}

void SparseArray::clear(){
    auto deleter = [this](Chunk* chunk){ free_chunk(chunk); };
    ScopedEpoch epoch; // index_find() requires being inside an epoch

    Key key = KEY_MIN; // KEY_MIN is always present in the index
    IndexEntry e = index_find(key);
    do {
        Chunk* chunk = get_chunk(e);
        key = get_gate(chunk, get_num_gates_per_chunk() -1)->m_fence_high_key; // next key
        global_context()->gc()->mark(chunk, deleter);
    } while(key != KEY_MAX);
}

// Allocate a new chunk of the sparse array
SparseArray::Chunk* SparseArray::allocate_chunk(){
    uint64_t space_required = sizeof(Chunk) +
            get_num_gates_per_chunk() * Gate::memory_footprint(get_num_segments_per_lock() *2 /* lhs + rhs */) +
            get_num_segments_per_chunk() * (sizeof(SegmentMetadata) + get_num_qwords_per_segment() * 8);

    void* heap { nullptr };
    int rc = posix_memalign(&heap, /* alignment = */ 2097152ull /* 2MB */,  /* size = */ space_required);
    if(rc != 0) throw std::runtime_error("SparseArray::allocate_chunk, cannot obtain a chunk of aligned memory");
    Chunk* chunk = new (heap) Chunk();

    // init the gates
    for(int i = 0; i < get_num_gates_per_chunk(); i++){
        //COUT_DEBUG("Gate: " << i << ", offset: " << reinterpret_cast<uint64_t*>(get_gate(chunk, i)) - reinterpret_cast<uint64_t*>(chunk));
        new (get_gate(chunk, i)) Gate(i, get_num_segments_per_lock() /* lhs and rhs */ *2);
    }

    // init the segments
    for(int i = 0; i < get_num_segments_per_chunk(); i++){
        //COUT_DEBUG("Segment " << i << ", offset: " << reinterpret_cast<uint64_t*>(get_segment_metadata(chunk, i)) - reinterpret_cast<uint64_t*>(chunk));
        SegmentMetadata* md = get_segment_metadata(chunk, i);
        md->m_versions1_start = 0;
        md->m_empty1_start = 0;
        md->m_empty2_start = get_num_qwords_per_segment();
        md->m_versions2_start = get_num_qwords_per_segment();
    }

    COUT_DEBUG("chunk: " << chunk);
    return chunk;
}

void SparseArray::free_chunk(Chunk* chunk){
    COUT_DEBUG("chunk: " << chunk);

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

bool SparseArray::is_directed() const {
    return m_is_directed;
}

bool SparseArray::is_undirected() const {
    return !is_directed();
}

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

Key SparseArray::get_key(const Update* u){
    assert(u != nullptr && "Null pointer");
    return get_key(*u);
}

SparseArray::Chunk* SparseArray::get_chunk(const IndexEntry entry){
    return reinterpret_cast<Chunk*>(entry.m_chunk_id);
}

const SparseArray::Chunk* SparseArray::get_chunk(const IndexEntry entry) const {
    return reinterpret_cast<const Chunk*>(entry.m_chunk_id);
}

Gate* SparseArray::get_gate(const Chunk* chunk, uint64_t id) const {
    const uint64_t* base_ptr = reinterpret_cast<const uint64_t*>(chunk +1);
    return const_cast<Gate*>( reinterpret_cast<const Gate*>(base_ptr + get_num_qwords_per_gate() * id) );
}

SparseArray::SegmentMetadata* SparseArray::get_segment_metadata(const Chunk* chunk, uint64_t segment_id){
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
    uint64_t gate_id = segment_id / get_num_segments_per_lock();
    uint64_t rel_offset_id = segment_id % get_num_segments_per_lock();

    uint64_t* segment_area_next_gate = reinterpret_cast<uint64_t*>(get_gate(chunk, gate_id +1));
    return reinterpret_cast<SegmentMetadata*>(segment_area_next_gate -
            (get_num_segments_per_lock() - rel_offset_id) * (sizeof(SegmentMetadata)/8 + get_num_qwords_per_segment()));
}

const SparseArray::SegmentMetadata* SparseArray::get_segment_metadata(const Chunk* chunk, uint64_t segment_id) const {
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
    uint64_t gate_id = segment_id / get_num_segments_per_lock();
    uint64_t rel_offset_id = segment_id % get_num_segments_per_lock();

    const uint64_t* segment_area = reinterpret_cast<const uint64_t*>(get_gate(chunk, gate_id) + 1);
    return reinterpret_cast<const SegmentMetadata*>(segment_area + rel_offset_id * (sizeof(SegmentMetadata)/8 + get_num_qwords_per_segment()));
}

uint64_t* SparseArray::get_segment_lhs_content_start(const Chunk* chunk, uint64_t segment_id) {
    SegmentMetadata* md1 = get_segment_metadata(chunk, segment_id);
    return reinterpret_cast<uint64_t*>(md1 + 1);
}

const uint64_t* SparseArray::get_segment_lhs_content_start(const Chunk* chunk, uint64_t segment_id) const {
    const SegmentMetadata* md1 = get_segment_metadata(chunk, segment_id);
    return reinterpret_cast<const uint64_t*>(md1 + 1);
}

uint64_t* SparseArray::get_segment_lhs_content_end(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_versions1_start;
}

const uint64_t* SparseArray::get_segment_lhs_content_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_versions1_start;
}

uint64_t* SparseArray::get_segment_lhs_versions_start(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_content_end(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_lhs_versions_start(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_content_end(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_lhs_versions_end(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty1_start;
}

const uint64_t* SparseArray::get_segment_lhs_versions_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty1_start;
}

uint64_t* SparseArray::get_segment_rhs_content_start(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_versions2_start;
}

const uint64_t* SparseArray::get_segment_rhs_content_start(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_versions2_start;
}

uint64_t* SparseArray::get_segment_rhs_content_end(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_content_start(chunk, segment_id) + get_num_qwords_per_segment();
}

const uint64_t* SparseArray::get_segment_rhs_content_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_content_start(chunk, segment_id) + get_num_qwords_per_segment();
}

uint64_t* SparseArray::get_segment_rhs_versions_start(const Chunk* chunk, uint64_t segment_id) {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty2_start;
}

const uint64_t* SparseArray::get_segment_rhs_versions_start(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_lhs_content_start(chunk, segment_id) + get_segment_metadata(chunk, segment_id)->m_empty2_start;
}

uint64_t* SparseArray::get_segment_rhs_versions_end(const Chunk* chunk, uint64_t segment_id){
    return get_segment_rhs_content_start(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_rhs_versions_end(const Chunk* chunk, uint64_t segment_id) const {
    return get_segment_rhs_content_start(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_content_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs){
    return is_lhs ? get_segment_lhs_content_start(chunk, segment_id) : get_segment_rhs_content_start(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_content_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_content_start(chunk, segment_id) : get_segment_rhs_content_start(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_content_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs){
    return is_lhs ? get_segment_lhs_content_end(chunk, segment_id) : get_segment_rhs_content_end(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_content_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_content_end(chunk, segment_id) : get_segment_rhs_content_end(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_versions_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) {
    return is_lhs ? get_segment_lhs_versions_start(chunk, segment_id) : get_segment_rhs_versions_start(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_versions_start(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_versions_start(chunk, segment_id) : get_segment_rhs_versions_start(chunk, segment_id);
}

uint64_t* SparseArray::get_segment_versions_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) {
    return is_lhs ? get_segment_lhs_versions_end(chunk, segment_id) : get_segment_rhs_versions_end(chunk, segment_id);
}

const uint64_t* SparseArray::get_segment_versions_end(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    return is_lhs ? get_segment_lhs_versions_end(chunk, segment_id) : get_segment_rhs_versions_end(chunk, segment_id);
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

bool SparseArray::is_segment_empty(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    if(is_lhs) {
        return is_segment_lhs_empty(chunk, segment_id);
    } else {
        return is_segment_rhs_empty(chunk, segment_id);
    }
}

bool SparseArray::is_segment_lhs_empty(const Chunk* chunk, uint64_t segment_id) const {
    return is_segment_lhs_empty(chunk, get_segment_metadata(chunk, segment_id));
}

bool SparseArray::is_segment_lhs_empty(const Chunk* chunk, const SegmentMetadata* md) const {
    return md->m_empty1_start == 0;
}

bool SparseArray::is_segment_rhs_empty(const Chunk* chunk, uint64_t segment_id) const {
    return is_segment_rhs_empty(chunk, get_segment_metadata(chunk,  segment_id));
}

bool SparseArray::is_segment_rhs_empty(const Chunk* chunk, const SegmentMetadata* md) const {
    return md->m_empty2_start == get_num_qwords_per_segment();
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


bool SparseArray::is_insert(const SegmentVersion* version) {
    return version->m_insdel == 0;
}

bool SparseArray::is_remove(const SegmentVersion* version){
    return version->m_insdel == 1;
}

bool SparseArray::is_insert(const Update& update){
    return update.m_update_type == Update::Insert;
}

bool SparseArray::is_insert(const Update* update){
    assert(update != nullptr);
    return update->m_update_type == Update::Insert;
}

bool SparseArray::is_remove(const Update& update){
    return update.m_update_type == Update::Remove;
}

bool SparseArray::is_remove(const Update* update){
    assert(update != nullptr);
    return update->m_update_type == Update::Remove;
}

bool SparseArray::is_vertex(const Update& update){
    return update.m_entry_type == Update::Vertex;
}

bool SparseArray::is_edge(const Update& update) {
    return update.m_entry_type == Update::Edge;
}

void SparseArray::reset_header(uint64_t* version){
    *version = 0;
}

void SparseArray::reset_header(SegmentVersion* version){
    reset_header( reinterpret_cast<uint64_t*>(version) );
}

void SparseArray::set_type(SegmentVersion* version, bool value){
    /* 0 = insert, 1 = remove */
    version->m_insdel = !value;
}

void SparseArray::set_type(SegmentVersion* version, const Update& update){
    set_type(version, is_insert(update));
}

void SparseArray::set_backptr(SegmentVersion* version, uint64_t offset){
    version->m_backptr = offset;
}

void SparseArray::set_undo(SegmentVersion* version, teseo::internal::context::Undo* undo){
    if(undo == nullptr){
        version->m_undo_length = 0;
    } else if(version->m_undo_length < MAX_UNDO_LENGTH){
        version->m_undo_length ++;
    }
    version->m_version = reinterpret_cast<uint64_t>(undo);
}

void SparseArray::unset_undo(SegmentVersion* version, teseo::internal::context::Undo* undo){
    assert(version->m_undo_length > 0 && "The are no versions associated to this version");
    assert(undo != nullptr && "Just remove the record all together from the sparse array");

    if(version->m_undo_length < MAX_UNDO_LENGTH){
        version->m_undo_length--;
        assert(version->m_undo_length > 0 && "Well, we assume that the given `undo' was the pointer to the previous head => length >= 2");
    }
    version->m_version = reinterpret_cast<uint64_t>(undo);
}

void SparseArray::prune_on_write(SegmentVersion* version, bool force){
    if(!force && version->m_undo_length < MAX_UNDO_LENGTH) return;
    auto result = Undo::prune(get_undo(version), thread_context()->all_active_transactions());
    version->m_version = reinterpret_cast<uint64_t>(result.first);
    version->m_undo_length = min(result.second, MAX_UNDO_LENGTH);
}

SparseArray::SegmentVertex* SparseArray::get_vertex(uint64_t* ptr){
    return reinterpret_cast<SegmentVertex*>(ptr);
}

const SparseArray::SegmentVertex* SparseArray::get_vertex(const uint64_t* ptr){
    return reinterpret_cast<const SegmentVertex*>(ptr);
}

SparseArray::SegmentEdge* SparseArray::get_edge(uint64_t* ptr){
    return reinterpret_cast<SegmentEdge*>(ptr);
}

const SparseArray::SegmentEdge* SparseArray::get_edge(const uint64_t* ptr){
    return reinterpret_cast<const SegmentEdge*>(ptr);
}

SparseArray::SegmentVersion* SparseArray::get_version(uint64_t* ptr){
    return reinterpret_cast<SegmentVersion*>(ptr);
}

const SparseArray::SegmentVersion* SparseArray::get_version(const uint64_t* ptr){
    return reinterpret_cast<const SegmentVersion*>(ptr);
}

uint64_t SparseArray::get_backptr(uint64_t* ptr){
    return get_backptr(get_version(ptr));
}

uint64_t SparseArray::get_backptr(const uint64_t* ptr){
    return get_backptr(get_version(ptr));
}

uint64_t SparseArray::get_backptr(SegmentVersion* ptr){
    return ptr->m_backptr;
}

uint64_t SparseArray::get_backptr(const SegmentVersion* ptr){
    return ptr->m_backptr;
}

Undo* SparseArray::get_undo(uint64_t* ptr){
    return get_undo(get_version(ptr));
}

const Undo* SparseArray::get_undo(const uint64_t* ptr){
    return get_undo(get_version(ptr));
}

Undo* SparseArray::get_undo(SegmentVersion* ptr){
    return reinterpret_cast<Undo*>(ptr->m_version);
}

const Undo* SparseArray::get_undo(const SegmentVersion* ptr){
    return reinterpret_cast<const Undo*>(ptr->m_version);
}


bool SparseArray::read_delta(const Transaction* transaction, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* ptr, Update* out_update){
    return read_delta_impl(transaction, vertex, edge, ptr, get_undo(ptr), out_update);
}

bool SparseArray::read_delta_optimistic(Gate* gate, uint64_t version, const Transaction* transaction, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* ptr, Update* out_update){
    const Undo* undo = get_undo(ptr);

    // is the pointer we just read still valid
    gate->m_latch.validate_version(version); // throws Abort{}

    // if so, then proceed to the implementation
    return read_delta_impl(transaction, vertex, edge, ptr, undo, out_update);
}

bool SparseArray::read_delta_impl(const Transaction* transaction, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* version, const Undo* undo, Update* out_update){
    Update* payload = nullptr;

    bool response = transaction->can_read(undo, (void**) &payload);

    if(response == true){ // fetch from the storage
        out_update->m_update_type = is_insert(version) ? Update::Insert : Update::Remove;
        out_update->m_source = vertex->m_vertex_id;
        if(edge == nullptr){ // this is a vertex;
            out_update->m_entry_type = Update::Vertex;
            out_update->m_destination = 0;
            out_update->m_weight = 0;
        } else { // this is an edge
            out_update->m_entry_type = Update::Edge;
            out_update->m_destination = edge->m_destination;
            out_update->m_weight = edge->m_weight;
        }

        return true;
    } else { // fetch from the undo log
        if(payload != nullptr){
            *out_update = *payload;

            return true;
        } else { // this record is not visible to the given transaction
            return false;
        }
    }
}

bool SparseArray::check_fence_keys(Gate* gate, int64_t& gate_id, Key key) const {
    assert(gate->m_locked && "To invoke this method the gate's lock must be acquired first");

    switch(gate->check_fence_keys(key)){
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
    case Gate::Direction::GO_AHEAD:
        return true;
    }

    return false;
}

/*****************************************************************************
 *                                                                           *
 *   Index                                                                   *
 *                                                                           *
 *****************************************************************************/

void SparseArray::index_insert(Key key, Chunk* chunk, uint64_t gate_id) {
    union {
        IndexEntry m_entry;
        void* m_raw_data;
    } e;

    assert((gate_id < (1ull <<16)) && "Overflow, m_entry.m_gate_id is a bitfield of 16 bits");
    e.m_entry.m_gate_id = gate_id;
    e.m_entry.m_chunk_id = reinterpret_cast<uint64_t>(chunk);
    m_index->insert(key.get_source(), key.get_destination(), e.m_raw_data);
}

void SparseArray::index_insert(Chunk* chunk){
    index_insert(chunk, 0, get_num_gates_per_chunk());
}

void SparseArray::index_insert(Chunk* chunk, int64_t gate_window_start, int64_t gate_window_length) {
    const int64_t gate_window_end = gate_window_start + gate_window_length;
    assert(0 <= gate_window_start && gate_window_start < gate_window_end && gate_window_end <= get_num_gates_per_chunk() && "Invalid interval");

    for(int64_t i = gate_window_start; i < gate_window_end; i++){
        Gate* gate = get_gate(chunk, i);
        if(gate->m_fence_low_key < gate->m_fence_high_key){ // otherwise it's an empty interval
            index_insert(gate->m_fence_low_key, chunk, i);
        }
    }
}

void SparseArray::index_remove(Key key){
    m_index->remove(key.get_source(), key.get_destination());
}

void SparseArray::index_remove(Chunk* chunk){
    index_remove(chunk, 0, get_num_gates_per_chunk());
}

void SparseArray::index_remove(Chunk* chunk, int64_t gate_window_start, int64_t gate_window_length){
    const int64_t gate_window_end = gate_window_start + gate_window_length;
    assert(0 <= gate_window_start && gate_window_start < gate_window_end && gate_window_end <= get_num_gates_per_chunk() && "Invalid interval");

    for(int64_t i = gate_window_start; i < gate_window_end; i++){
        Gate* gate = get_gate(chunk, i);
        if(gate->m_fence_low_key < gate->m_fence_high_key){ // otherwise it's an empty interval
            index_remove(gate->m_fence_low_key);
        }
    }
}

SparseArray::IndexEntry SparseArray::index_find(uint64_t vertex_id) const {
    void* result = m_index->find(vertex_id);
    if(result == nullptr){
        return IndexEntry{ 0, 0 };
    } else {
        return *reinterpret_cast<IndexEntry*>(&result);
    }
}

SparseArray::IndexEntry SparseArray::index_find(Key key) const {
    return index_find(key.get_source(), key.get_destination());
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
 *   Insert/Remove interface                                                 *
 *                                                                           *
 *****************************************************************************/

void SparseArray::insert_vertex(Transaction* transaction, uint64_t vertex_id) {
    Update update;
    update.m_entry_type = Update::Vertex;
    update.m_update_type = Update::Insert;
    update.m_source = vertex_id;
    write(transaction, update, true);
}

void SparseArray::remove_vertex(Transaction* transaction, uint64_t vertex_id) {
    Update update;
    update.m_entry_type = Update::Vertex;
    update.m_update_type = Update::Remove;
    update.m_source = vertex_id;
    write(transaction, update, true);
}

void SparseArray::insert_edge(Transaction* transaction, uint64_t source, uint64_t destination, double weight){
    Update update;
    update.m_entry_type = Update::Edge;
    update.m_update_type = Update::Insert;
    update.m_source = source;
    update.m_destination = destination;
    update.m_weight = weight;

    if(is_directed()){
        // explicitly check whether the destination vertex exists
        if(!has_vertex(transaction, destination)){ RAISE_EXCEPTION(LogicalError, "The destination vertex " << destination << " does not exist"); }

        // perform the update, the routine #update_edge ensures  that the source vertex exists
        do_insert_edge(transaction,  update);
    } else {
        // first, insert the edge source -> destination. The first call will ensure that source exists
        do_insert_edge(transaction, update);

        // second, insert the edge destination -> source. This call will ensure that destination exists
        std::swap(update.m_source, update.m_destination);
        do_insert_edge(transaction, update);
    }
}

void SparseArray::do_insert_edge(Transaction* transaction, Update& update){
    // we don't make any assumption whether the destination of the edge already exists, this need to be checked independently
    try {
        // first try to insert/remove the edge, the writer will try to ensure the source already exists `a la best effort'
        write(transaction, update, /* force ? */ false);
    } catch (ConsistencyCheckFailed){
        // okay, this means that the writer is not sure whether the source vertex exists, we need to check for it explicitly
        if(has_vertex(transaction, update.m_source)){
            // force the update to the edge, the source vertex definitely exists
            write(transaction, update, /* force ? */ true);
        } else {
            RAISE_EXCEPTION(LogicalError, "The source vertex " << update.m_source << " does not exist");
        }
    }
}

void SparseArray::remove_edge(Transaction* transaction, uint64_t source, uint64_t destination){
    Update update;
    update.m_entry_type = Update::Edge;
    update.m_update_type = Update::Remove;
    update.m_source = source;
    update.m_destination = destination;
    update.m_weight = 0; // it doesn't matter, ignored

    // Differently from edge insertions, we don't explicitly check whether the source & destination vertices exist in the array.
    // If the edge does not exist in the sparse array, the underlying routine will raise an error anyway, without chances of creating dangling links.
    write(transaction, update, /* force ? */ true);

    if(is_undirected()){ // undirected graphs actually store two edges a -> b and b -> a
        std::swap(update.m_source, update.m_destination);
        write(transaction, update, /* force ? */ true);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Writers                                                                 *
 *                                                                           *
 *****************************************************************************/

void SparseArray::write(Transaction* transaction, Update& update, bool is_consistent) {
    assert(transaction != nullptr && "Transaction not given");
    assert(!transaction->is_terminated() && "The given transaction is already terminated");

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
            bool is_update_done = do_write_gate(transaction, chunk, gate, update, is_consistent);

            // Rebalance the chunk then
            if(!is_update_done){
                rebalance_chunk(chunk, gate);
                // we still need to perform the update ...
            } else {
                writer_on_exit(chunk, gate);
                done = true;
            }

        } catch (Abort) {
            /* nop */
        } catch (...) {
            if(gate != nullptr){
                writer_on_exit(chunk, gate); // release the gate
                gate = nullptr;
            }
            throw;
        }
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
        gate = get_gate(chunk, gate_id);
        unique_lock<Gate> lock(*gate);

        if(check_fence_keys(gate, gate_id, search_key)){
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
        }
    } while(!done);

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

bool SparseArray::do_write_gate(Transaction* transaction, Chunk* chunk, Gate* gate, Update& update, bool is_consistent) {
    COUT_DEBUG("Gate: " << gate->id() << ", update: " << update);

    uint64_t g2sid = gate->find(get_key(update));
    uint64_t segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
    uint64_t is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment

    bool is_update_done = do_write_segment(transaction, chunk, gate, segment_id, is_lhs, update, is_consistent);

    if(!is_update_done){ // try to rebalance locally, inside the gate
        bool rebalance_done = rebalance_gate(chunk, gate, segment_id);
        if(!rebalance_done) return false;

        // try again
        g2sid = gate->find(get_key(update));
        segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
        is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment
        is_update_done = do_write_segment(transaction, chunk, gate, segment_id, is_lhs, update, is_consistent);
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

    Chunk* sibling {nullptr};

    if(do_rebalance){
        spad.save(chunk, window_start, window_length);

        // Fence keys
        index_remove(chunk, gate_window_start, gate_window_length);
        auto hfkey = get_gate(chunk, gate_window_start + gate_window_length)->m_fence_high_key;
        update_fence_keys(chunk, gate_window_start, gate_window_length, hfkey);
        index_insert(chunk, gate_window_start, gate_window_length);

    } else { // split leaf
        assert(window_start == 0);
        assert(window_length == get_num_segments_per_chunk());
        sibling = allocate_chunk();
        spad.save(chunk, 0, get_num_segments_per_chunk());
        spad.save(sibling, 0, get_num_segments_per_chunk());

        // Fence keys
        index_remove(chunk);
        auto hfkey = get_gate(chunk, gate_window_start + gate_window_length)->m_fence_high_key;
        hfkey = update_fence_keys(sibling, 0, get_num_gates_per_chunk(), hfkey);
        update_fence_keys(chunk, 0, get_num_gates_per_chunk(), hfkey);
        index_insert(chunk);
        index_insert(sibling);
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
        space_filled += OFFSET_VERTEX /* with m_first = 0 */ + OFFSET_EDGE + OFFSET_VERSION;
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

Key SparseArray::update_fence_keys(Chunk* chunk, int64_t gate_window_start, int64_t gate_window_length, Key max) {
    const int64_t gate_window_end = gate_window_start + gate_window_length;
    assert(gate_window_start >= 0);
    assert(gate_window_end <= get_num_gates_per_chunk());
    assert(gate_window_start < gate_window_end && "Invalid interval");

    for(int64_t i = gate_window_end - 1; i >= gate_window_start; i--){
        Gate* gate = get_gate(chunk, i);
        gate->m_fence_high_key = max;
        Key min = update_separator_keys(chunk, gate, 0, get_num_segments_per_lock() * /* lhs + rhs */ 2);
        gate->m_fence_low_key = min;

        // next iteration
        max = min;
    }

    return max;
}

Key SparseArray::get_minimum(const Chunk* chunk, uint64_t segment_id) const {
    return get_minimum(chunk, segment_id, /* is lhs ? */ !is_segment_lhs_empty(chunk, segment_id));
}

Key SparseArray::get_minimum(const Chunk* chunk, uint64_t segment_id, bool is_lhs) const {
    if(is_segment_empty(chunk, segment_id, is_lhs)) return KEY_MIN;

    const uint64_t* __restrict content = get_segment_content_start(chunk, segment_id, is_lhs);

    const SegmentVertex* vertex = get_vertex(content);
    if(vertex->m_first){ // first vertex entry in the edge list
        return Key { vertex->m_vertex_id };
    } else { // as this is not the first vertex in the chain, then it must contain some edges
        assert(vertex->m_count > 0);
        const SegmentEdge* edge = get_edge(content + OFFSET_VERTEX);
        return Key { vertex->m_vertex_id, edge->m_destination };
    }
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
    spad.save(chunk, window_start, window_length);

    // Update the separator keys inside the gate
    int64_t sep_key_start = window_start - gate->id() * get_num_segments_per_lock();
    int64_t sep_key_end = sep_key_start + window_length * /* lhs + rhs */ 2;
    update_separator_keys(chunk, gate, sep_key_start, sep_key_end);

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

Key SparseArray::update_separator_keys(Chunk* chunk, Gate* gate, int64_t sep_key_start, int64_t sep_key_end){
    const int64_t window_start = gate->id() * get_num_segments_per_lock();
    const int64_t num_sep_keys_per_gate = get_num_segments_per_lock() *2;
    assert(sep_key_start < sep_key_end && "Invalid interval");
    assert(sep_key_end <= num_sep_keys_per_gate && "sep_key_end refers to the absolute index of the separator keys in the gate");

    Key key = (sep_key_end == num_sep_keys_per_gate) ? gate->m_fence_high_key : gate->get_separator_key(sep_key_end -1);
    for(int64_t i = sep_key_end -1; i >= sep_key_start; i--){
        uint64_t segment_id = window_start + i/2;
        bool is_lhs = (i % 2) == 0;
        if(is_segment_empty(chunk, segment_id, is_lhs)){
            gate->set_separator_key(i, key);
        } else {
            key = get_minimum(chunk, segment_id, is_lhs);
            gate->set_separator_key(segment_id, key);
        }
    }

    return key;
}

/*****************************************************************************
 *                                                                           *
 *   Raw writes in the segment                                               *
 *                                                                           *
 *****************************************************************************/

bool SparseArray::do_write_segment(Transaction* transaction, Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update, bool is_consistent){
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");

    if(is_vertex(update)){
        return do_write_segment_vertex(transaction, chunk, gate, segment_id, is_lhs, update);
    } else {
        return do_write_segment_edge(transaction, chunk, gate, segment_id, is_lhs, update, is_consistent);
    }
}

bool SparseArray::do_write_segment_vertex(Transaction* transaction, Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update) {
    const uint64_t vertex_id = update.m_source;

    // pointers to the static & delta portions of the segment
    SegmentMetadata* __restrict segmentcb = get_segment_metadata(chunk, segment_id);
    uint64_t* __restrict c_start = get_segment_content_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict c_end = get_segment_content_end(chunk, segment_id, is_lhs);
    uint64_t* __restrict v_start = get_segment_versions_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict v_end = get_segment_versions_end(chunk, segment_id, is_lhs);

    // first, find the position in the content area where to insert the new vertex
    int64_t v_backptr = 0;
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    bool c_found = false;
    bool stop = false;
    while(c_index < c_length && !stop){
        SegmentVertex* vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < vertex_id){
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else {
            c_found = vertex->m_vertex_id == vertex_id;
            stop = vertex->m_vertex_id >= vertex_id;
        }
    }

    // second, find the position in the versions area
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    bool v_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        SegmentVersion* version = get_version(v_start + v_index);
        uint64_t backptr = get_backptr(version);
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            v_found = backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    // three, consistency checks
    assert((!c_found || get_vertex(v_start + v_index)->m_first == 1) && "This is not the first vertex in the chain");
    if(v_found){
        SegmentVersion* version = get_version(v_start + v_index);
        if(!transaction->can_write(get_undo(version))){
            RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the vertex ID " << vertex_id << " is currently locked by another transaction. Restart this transaction to alter this object");
        } else if( is_insert(update) && is_insert(version) ){
            RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " already exists");
        } else if( is_remove(update) && is_remove(version) ){
            RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " does not exist");
        }
    } else if(c_found && is_insert(update)){ // the static vertex exists
        RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " already exists");
    } else if(!c_found && is_remove(update)){ // the static vertex doesn't exist
        RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " does not exist");
    }

    // four, check we have enough space to add the necessary entries
    int64_t c_shift = (!c_found) * OFFSET_VERTEX;
    int64_t v_shift = (!v_found) * OFFSET_VERSION + c_shift;
    if( get_segment_free_space(chunk, segment_id) < v_shift) return false;
    gate->m_used_space += v_shift;

    // okay, we have three possible cases to account, depending on the values of c_found and v_shift:
    // c_found  v_found  possible?
    //    F        F         T
    //    F        T         F
    //    T        F         T
    //    T        T         T
    assert(c_found || !v_found); // see above

    if(!v_found){
        static_assert(OFFSET_VERSION == 1, "Otherwise the code below is broken");

        if(c_found){
            // we only need to shift the versions after v_index by 1 (OFFSET_VERSION), without incrementing their backwards ptr
            assert(c_shift == 0);
            if(is_lhs){ // shift forwards
                for(int64_t i = v_length; i > v_index; i--){
                    v_start[i] = v_start[i - 1];
                }

                segmentcb->m_empty1_start += v_shift;

            } else { // shift backwards
                for(int64_t i = 0; i < v_index; i++){
                    v_start[i - 1] = v_start[i];
                }

                segmentcb->m_empty2_start -= v_shift;
            }

        } else {
            // we need to shift both the content and the versions

            if(is_lhs){
                // let's start with the versions
                for(int64_t i = v_length -1; i >= v_index; i--){
                    v_start[i + v_shift] = v_start[i];
                    get_version(v_start + i + v_shift)->m_backptr++;
                }
                // now the content
                int64_t shift_length = (v_start - c_start) + v_index - c_index;
                memmove(c_start + c_index + c_shift, c_start + c_index, shift_length * sizeof(uint64_t));

                v_index += c_shift;
            } else { // right hand side
                // again, the versions first
                for(int64_t i = 0; i < v_index; i++){
                    v_start[i - v_shift] = v_start[i];
                    // do not change the back pointer
                }
                for(int64_t i = v_index; i < v_length; i++){
                    v_start[i - c_shift] = v_start[i];
                    get_version(v_start + i - c_shift)->m_backptr++;
                }

                // now the content
                memmove(c_start - c_shift, c_start, c_index * sizeof(uint64_t));

                v_index -= c_shift;
            }

            SegmentVertex* vertex = get_vertex(c_start + c_index);
            vertex->m_vertex_id = vertex_id;
            vertex->m_first = 1;
            vertex->m_count = 0;
        }

        // update the pointers in the segment metadata
        if(is_lhs){
            segmentcb->m_versions1_start += c_shift;
            segmentcb->m_empty1_start += v_shift;
        } else {
            segmentcb->m_empty2_start -= v_shift;
            segmentcb->m_versions2_start -= c_shift;
        }

        reset_header(v_start + v_index);
    } else {
        prune_on_write(get_version(v_start + v_index));
    }

    // fifth, update the record's version with this change
    SegmentVersion* version = get_version(v_start + v_index);
    set_type(version, update);
    set_backptr(version, v_backptr);
    set_undo(version, transaction->add_undo(this, get_undo(version), update));

    // done
    return true;
}

bool SparseArray::do_write_segment_edge(Transaction* transaction, Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update, bool is_consistent) {
    // pointers to the static & delta portions of the segment
    SegmentMetadata* __restrict segmentcb = get_segment_metadata(chunk, segment_id);

    uint64_t* __restrict c_start = get_segment_content_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict c_end = get_segment_content_end(chunk, segment_id, is_lhs);
    uint64_t* __restrict v_start = get_segment_versions_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict v_end = get_segment_versions_end(chunk, segment_id, is_lhs);

    // first, find the position in the content area where to insert the new vertex
    int64_t v_backptr = 0;
    int64_t c_index_vertex = 0;
    int64_t c_index_edge = 0;
    int64_t c_length = c_end - c_start;
    bool vertex_found = false;
    bool edge_found = false;
    bool stop = false;
    while(c_index_vertex < c_length && !stop){
        SegmentVertex* vertex = get_vertex(c_start + c_index_vertex);
        if(vertex->m_vertex_id < update.m_source){
            c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else if(vertex->m_vertex_id == update.m_source){
            vertex_found = true;

            c_index_edge = c_index_vertex + OFFSET_VERTEX;
            v_backptr++;

            int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
            while(c_index_edge < e_length && !stop){
                SegmentEdge* edge = get_edge(c_start + c_index_edge);
                if(edge->m_destination < update.m_destination){
                    c_index_edge += OFFSET_EDGE;
                    v_backptr++;
                } else { // edge->m_destination >= update.m_destination
                    edge_found = edge->m_destination == update.m_destination;
                    stop = true;
                }
            }

            stop = true;
        } else { // vertex->m_vertex_id > update.m_source
            c_index_edge = c_index_vertex;
            stop = true;
        }
    }

    // we are not sure whether the source vertex exists
    if(!vertex_found && !is_consistent){ throw ConsistencyCheckFailed{ }; }

    // second, find the position in the versions area
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    bool version_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        SegmentVersion* version = get_version(v_start + v_index);
        uint64_t backptr = get_backptr(version);
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            version_found = backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    // third, consistency checks
    if(version_found){
        SegmentVersion* version = get_version(v_start + v_index);

        if(!transaction->can_write(get_undo(version))){
            RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the edge " << update.m_source << " -> " <<
                    update.m_destination << " is currently locked by another transaction. Restart this transaction to alter this object");
        } else if( is_insert(update) && is_insert(version) ){
            RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " already exists");
        } else if( is_remove(update) && is_remove(version) ){
            RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " does not exist");
        }
    } else if(edge_found && is_insert(update)) {
        RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " already exists");
    } else if(!edge_found && is_remove(update)){
        RAISE_EXCEPTION(LogicalError, "The edge " << update.m_source << " -> " << update.m_destination << " does not exist");
    }

    // fourth, check we have enough space to add the necessary entries
    int64_t c_shift = (!vertex_found) * OFFSET_VERTEX + (!edge_found) * OFFSET_EDGE;
    int64_t v_shift = (!version_found) * OFFSET_VERSION + c_shift;
    if( get_segment_free_space(chunk, segment_id) < v_shift) return false;
    gate->m_used_space += v_shift;

    // fifth, insert the record into the sparse array
    // similar to #do_write_segment_vertex()
    if(!version_found){
        static_assert(OFFSET_VERSION == 1, "Otherwise the code below is broken");

        if(edge_found){
            // we only need to shift the versions after v_index by 1 (OFFSET_VERSION), without incrementing their backwards ptr
            assert(vertex_found == true);
            assert(c_shift == 0);

            if(is_lhs){ // shift forwards
                for(int64_t i = v_length; i > v_index; i--){
                    v_start[i] = v_start[i - 1];
                }

                segmentcb->m_empty1_start += v_shift;

            } else { // shift backwards
                for(int64_t i = 0; i < v_index; i++){
                    v_start[i - 1] = v_start[i];
                }

                segmentcb->m_empty2_start -= v_shift;
            }

        } else {
            // we need to shift both the content and the versions

            if(is_lhs){
                // let's start with the versions
                for(int64_t i = v_length -1; i >= v_index; i--){
                    v_start[i + v_shift] = v_start[i];
                    get_version(v_start + i + v_shift)->m_backptr++;
                }
                // now the content
                int64_t shift_length = (v_start - c_start) + v_index - c_index_edge;
                memmove(c_start + c_index_edge + c_shift, c_start + c_index_edge, shift_length * sizeof(uint64_t));

                v_index += c_shift;
            } else { // right hand side
                // again, the versions first
                for(int64_t i = 0; i < v_index; i++){
                    v_start[i - v_shift] = v_start[i];
                    // do not change the back pointer
                }
                for(int64_t i = v_index; i < v_length; i++){
                    v_start[i - c_shift] = v_start[i];
                    get_version(v_start + i - c_shift)->m_backptr++;
                }

                // now the content
                memmove(c_start - c_shift, c_start, c_index_edge * sizeof(uint64_t));

                v_index -= c_shift;
            }

            // update the source vertex attached to this edge
            if(!vertex_found){
                c_index_vertex = c_index_edge;
                c_index_edge = c_index_vertex + OFFSET_VERTEX;

                SegmentVertex* vertex = get_vertex(c_start + c_index_vertex);
                vertex->m_vertex_id = update.m_source;
                vertex->m_first = 0;
                vertex->m_count = 0;
            } else {
                SegmentVertex* vertex = get_vertex(c_start + c_index_vertex);
                vertex->m_count++;
            }
        }

        // update the pointers in the segment metadata
        if(is_lhs){
            segmentcb->m_versions1_start += c_shift;
            segmentcb->m_empty1_start += v_shift;
        } else {
            segmentcb->m_empty2_start -= v_shift;
            segmentcb->m_versions2_start -= c_shift;
        }

        reset_header(v_start + v_index);
    } else {
        prune_on_write(get_version(v_start + v_index));
    }

    // sixth, update the content part of the record
    SegmentEdge* edge = get_edge(c_start + c_index_edge);
    edge->m_destination = update.m_destination;
    edge->m_weight = update.m_weight;

    // seventh, update the record's version with this change
    SegmentVersion* version = get_version(v_start + v_index);
    set_type(version, update);
    set_backptr(version, v_backptr);
    set_undo(version, transaction->add_undo(this, get_undo(version), update));

    // done
    return true;
}

/*****************************************************************************
 *                                                                           *
 *   Roll back/undo                                                          *
 *                                                                           *
 *****************************************************************************/
void SparseArray::do_rollback(void* undo_payload, teseo::internal::context::Undo* next) {
    if(undo_payload == nullptr) RAISE_EXCEPTION(InternalError, "Undo record missing");
    Update& update = *(reinterpret_cast<Update*>(undo_payload));

    // similarly to #write, we need to gain exclusive access to the interested segment in sparse array
    bool done = false;

    do {
        Chunk* chunk {nullptr};
        Gate* gate {nullptr};

        try {
            ScopedEpoch epoch;

            // Acquire the xlock to the interested gate
            std::tie(chunk, gate) = writer_on_entry(update);
            assert(chunk != nullptr && gate != nullptr);

            // Find the segment where the undo records belong
            uint64_t g2sid = gate->find(get_key(update));
            uint64_t segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
            uint64_t is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment

            // Perform the actual rollback in the identified segment
            do_rollback_segment(chunk, gate, segment_id, is_lhs, update, next);

            // Release the xlock in the gate
            writer_on_exit(chunk, gate);
            done = true;
        } catch (Abort) {
            /* try again */
        } catch (...) {
            if(gate != nullptr){
                writer_on_exit(chunk, gate); // release the gate
                gate = nullptr;
            }
            throw;
        }
    } while(!done);
}

void SparseArray::do_rollback_segment(Chunk* chunk, Gate* gate, uint64_t segment_id, bool is_lhs, Update& update, teseo::internal::context::Undo* next){
    SegmentMetadata* __restrict segmentcb = get_segment_metadata(chunk, segment_id);
    uint64_t* __restrict c_start = get_segment_content_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict c_end = get_segment_content_end(chunk, segment_id, is_lhs);
    uint64_t* __restrict v_start = get_segment_versions_start(chunk, segment_id, is_lhs);
    uint64_t* __restrict v_end = get_segment_versions_end(chunk, segment_id, is_lhs);

    // we need to find the vertex/edge in the content section and its version in the versions area
    // let's start with the content area
    int64_t c_index_vertex = 0;
    int64_t c_index_edge = -1;
    int64_t c_length = c_end - c_start;
    int64_t v_backptr = 0;
    SegmentVertex* vertex = nullptr;
    bool vertex_found = false;
    bool edge_found = false;
    bool stop = false;

    while(c_index_vertex < c_length && !stop){
        vertex = get_vertex(c_start + c_index_vertex);
        if(vertex->m_vertex_id < update.m_source){
            c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            v_backptr += 1 + vertex->m_count;
        } else if (vertex->m_vertex_id == update.m_source){
            vertex_found = true;
            if(is_vertex(update)){
                stop = true; // done
            } else {
                v_backptr++; // skip the vertex

                // find the edge
                c_index_edge = c_index_vertex + OFFSET_VERTEX;
                int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
                while(c_index_edge < e_length && !stop){
                    SegmentEdge* edge = get_edge(c_start + c_index_edge);
                    if(edge->m_destination < update.m_destination){
                        c_index_edge += OFFSET_EDGE;
                        v_backptr++;
                    } else { // edge->m_destination >= update.m_destination
                        edge_found = edge->m_destination == update.m_destination;
                        stop = true;
                    }
                }

                stop = true;
            }
        } else {
            // uh?
            stop = true;
        }
    }

    assert(vertex_found == true && "Vertex not found in the content area?");
    assert((!is_edge(update) || edge_found == true) && "Edge not found in the content area?");

    // find the version in the versions area
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    bool version_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        SegmentVersion* version = get_version(v_start + v_index);
        uint64_t backptr = get_backptr(version);
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            version_found = backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    assert(version_found == true && "Version missing?");

    if(next == nullptr){
        // Great, we can remove the records from the content area
        vertex->m_count -= 1;
        bool remove_vertex = is_vertex(update) || (vertex->m_first == 0 && vertex->m_count == 0);
        int64_t c_index = remove_vertex ? c_index_vertex : c_index_edge;
        int64_t c_shift = is_edge(update) * OFFSET_EDGE + (remove_vertex) * OFFSET_VERTEX;
        int64_t v_shift = c_shift + OFFSET_VERSION;
        gate->m_used_space -= v_shift;

        static_assert(OFFSET_VERSION == 1, "Otherwise the code below is broken");


        if(is_lhs){ // left hand side
            // shift the content by c_shift
            int64_t c_shift_length = (v_start - c_start) + v_index - c_index;
            memmove(c_start + c_index, c_start + c_index + c_shift, c_shift_length * sizeof(uint64_t));

            // shift the versions
            for(int64_t i = v_index +1; i < v_length; i++){
                v_start[i - v_shift] = v_start[i];
                get_version(v_start + i - v_shift)->m_backptr--;
            }

            segmentcb->m_versions1_start -= c_shift;
            segmentcb->m_empty1_start -= v_shift;

        } else { // right hand side
            // shift the content
            memmove(c_start + c_shift, c_start, c_index * sizeof(uint64_t));

            // shift the versions
            for(int64_t i = v_length -1; i >= v_index +1; i--){
                v_start[i + c_shift] = v_start[i];
                get_version(v_start + i + c_shift)->m_backptr--;
            }
            for(int64_t i = v_index -1; i >= 0; i--){
                v_start[i + v_shift] = v_start[i];
                // do not alter the backptr
            }

            segmentcb->m_versions2_start += c_shift;
            segmentcb->m_empty2_start += v_shift;
        }

    } else { // keep the record alive as other versions exist
        SegmentVersion* version = get_version(v_start + v_index);
        set_type(version, is_insert(reinterpret_cast<Update*>(next->payload())));
        unset_undo(version, next);
    }

    // and that's it...
}

/*****************************************************************************
 *                                                                           *
 *   Search                                                                  *
 *                                                                           *
 *****************************************************************************/
bool SparseArray::has_vertex(Transaction* transaction, uint64_t vertex_id) const {
    return has_item(transaction, /* is vertex ? */ true, Key{ vertex_id });
}

bool SparseArray::has_edge(Transaction* transaction, uint64_t source, uint64_t destination) const {
    return has_item(transaction, /* is edge ? */ false, Key{source, destination});
}

bool SparseArray::has_item(Transaction* transaction, bool is_vertex, Key key) const {
    do {
        const Chunk* chunk {nullptr};
        Gate* gate {nullptr};
        uint64_t version = 0;

        try {
            ScopedEpoch epoch;

            // Fetch the chunk & the gate we need to operate
            std::tie(chunk, gate, version) = reader_on_entry_optimistic(key); // opt latch
            assert(chunk != nullptr && gate != nullptr);

            // Select the segment to inspect
            uint64_t g2sid = gate->find(key);

            uint64_t segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
            uint64_t is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment

            // Search in the segment
            bool result = has_item_segment_optimistic(transaction, chunk, gate, version, segment_id, is_lhs, is_vertex, key);

            // Check we haven't read gibberish
            gate->m_latch.validate_version(version);

            return result; // done
        } catch (Abort) { /* nop */ }
    } while(true);
}

bool SparseArray::has_item_segment_optimistic(Transaction* transaction, const Chunk* chunk, Gate* gate, uint64_t gate_version, uint64_t segment_id, bool is_lhs, bool is_key_vertex, Key key) const {
    const uint64_t* __restrict c_start = get_segment_content_start(chunk, segment_id, is_lhs);
    const uint64_t* __restrict c_end = get_segment_content_end(chunk, segment_id, is_lhs);
    const uint64_t* __restrict v_start = get_segment_versions_start(chunk, segment_id, is_lhs);
    const uint64_t* __restrict v_end = get_segment_versions_end(chunk, segment_id, is_lhs);

    // search in the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_backptr = 0;
    const SegmentVertex* vertex = nullptr;
    const SegmentEdge* edge = nullptr;
    bool vertex_found = false;
    bool edge_found = false;
    bool stop = false;

    while(c_index < c_length && !stop){
        vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < key.get_source()){
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            v_backptr += 1 + vertex->m_count;
        } else if (vertex->m_vertex_id == key.get_source()){
            vertex_found = true;
            if(is_key_vertex){
                stop = true; // done
            } else {
                // find the edge
                c_index += OFFSET_VERTEX;
                v_backptr++; // skip the vertex

                int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
                while(c_index < e_length && !stop){
                    edge = get_edge(c_start + c_index);
                    if(edge->m_destination < key.get_destination()){
                        c_index += OFFSET_EDGE;
                        v_backptr++;
                    } else { // edge->m_destination >= update.m_destination
                        edge_found = edge->m_destination == key.get_destination();
                        stop = true;
                    }
                }

                stop = true;
            }
        } else {
            stop = true;
        }
    }

    if(!vertex_found || (!is_key_vertex && !edge_found)) return false;

    // okay, at this point it exists a record with the given vertex/edge, but we need to check whether
    // the transaction can actually read id
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    const SegmentVersion* version = nullptr;
    bool version_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        version = get_version(v_start + v_index);
        uint64_t backptr = get_backptr(version);
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            version_found = backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    if(!version_found) return true; // there is not a version around for this record

    Update stored_content;
    bool visible = read_delta_optimistic(gate, gate_version, transaction, vertex, edge, version, &stored_content);
    return visible && is_insert(stored_content);
}

/*****************************************************************************
 *                                                                           *
 *   Readers                                                                 *
 *                                                                           *
 *****************************************************************************/



auto SparseArray::reader_on_entry(Key key) const -> std::pair<const Chunk*, Gate*> {
    ThreadContext* context = thread_context();
    assert(context != nullptr);
    context->epoch_enter();

    IndexEntry leaf_addr = index_find(key.get_source(), key.get_destination());
    const Chunk* chunk = get_chunk(leaf_addr);
    int64_t gate_id = leaf_addr.m_gate_id;
    Gate* gate = nullptr;

    bool done = false;
    do {
        gate = get_gate(chunk, gate_id);
        unique_lock<Gate> lock(*gate);

        if(check_fence_keys(gate, gate_id, key)){
            switch(gate->m_state){
            case Gate::State::FREE:
                assert(gate->m_num_active_threads == 0 && "Precondition not satisfied");
                gate->m_state = Gate::State::READ;
                gate->m_num_active_threads = 1;

                done = true; // done, proceed with the insertion
                break;
            case Gate::State::READ:
                if(gate->m_queue.empty()){ // as above
                    gate->m_num_active_threads ++;
                    done = true;
                } else {
                    reader_wait(gate, lock);
                }
                break;
            case Gate::State::WRITE:
            case Gate::State::REBAL:
                // add the thread in the queue
                reader_wait(gate, lock);
                break;
            default:
                assert(0 && "Invalid case");
            }
        }
    } while(!done);

    return std::make_pair(chunk, gate);
}

 auto SparseArray::reader_on_entry_optimistic(Key key) const -> std::tuple<const Chunk*, Gate*, uint64_t>{
     ThreadContext* context = thread_context();
     assert(context != nullptr);
     context->epoch_enter();

     IndexEntry leaf_addr = index_find(key.get_source(), key.get_destination());
     const Chunk* chunk = get_chunk(leaf_addr);
     int64_t gate_id = leaf_addr.m_gate_id;
     Gate* gate = nullptr;
     uint64_t version = 0;

     bool done = false;
     do {
         gate = get_gate(chunk, gate_id);
         ScopedPhantomLock lock(gate->m_latch);

         if(check_fence_keys(gate, gate_id, key)){
             switch(gate->m_state){
             case Gate::State::FREE:
             case Gate::State::READ:
                 version = lock.unlock();
                 done = true;
                 break;
             case Gate::State::WRITE:
             case Gate::State::REBAL:
                 // add the thread in the queue
                 reader_wait(gate, lock);
                 break;
             default:
                 assert(0 && "Invalid case");
             }
         }
     } while(!done);

     return std::make_tuple(chunk, gate, version);
 }

template<typename Lock>
void SparseArray::reader_wait(Gate* gate, Lock& lock) const {
    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();
    gate->m_queue.append({ Gate::State::READ, &producer } );
    lock.unlock();
    consumer.wait();
}

void SparseArray::reader_on_exit(const Chunk* chunk, Gate* gate) const {
    gate->lock();
    assert(gate->m_num_active_threads > 0 && "This reader should have been registered");
    gate->m_num_active_threads--;
    if(gate->m_num_active_threads == 0){
        switch(gate->m_state){
        case Gate::State::READ:
            gate->m_state = Gate::State::FREE;
            gate->wake_next();
            break;
        case Gate::State::REBAL:
            gate->wake_next();
            break;
        default:
            assert(0 && "Invalid state");
        }
    }
    gate->unlock();
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void SparseArray::dump() const {
    ScopedEpoch epoch; // index find requires being inside an epoch

    uint64_t chunk_sz = sizeof(Chunk) +
            get_num_gates_per_chunk() * Gate::memory_footprint(get_num_segments_per_lock() *2 /* lhs + rhs */) +
            get_num_segments_per_lock() * (sizeof(SegmentMetadata) + get_num_qwords_per_segment() * 8);

    cout << "[Sparse Array] directed: " << boolalpha << is_directed() << ", num gates per chunk: " << get_num_gates_per_chunk() << ", segments per chunk: " << get_num_segments_per_chunk() << ", segments per gate: " << get_num_segments_per_lock() << ", chunk size " << chunk_sz << " bytes\n";

    cout << "Index: \n";
    m_index->dump();

    uint64_t num_chunks = 0;
    bool integrity_check = true;
    cout << "\nChunks: " << endl;

    IndexEntry entry = index_find(0);
    const Chunk* chunk = get_chunk(entry);
    while(chunk != nullptr && integrity_check){
        dump_chunk(cout, chunk, num_chunks, &integrity_check);

        num_chunks++; // number of chunks visited so far

        // next chunk
        auto next_key = get_gate(chunk, get_num_gates_per_chunk() -1)->m_fence_high_key;
        if(next_key != KEY_MAX){
            chunk = get_chunk( index_find(next_key) );
        } else { // done;
            chunk = nullptr;
        }
    }

    cout << "Number of visited chunks: " << num_chunks << "\n";
    if(!integrity_check){
        cout << "\n!!! INTEGRITY CHECK FAILED !!!" << endl;
        assert(false && "Integrity check failed");
    }
}

static void print_tabs(std::ostream& out, int tabs){
    auto flags = out.flags();
    out << setw(tabs * 2) << setfill(' ') << ' ';
    out.setf(flags);
}

void SparseArray::dump_chunk(std::ostream& out, const Chunk* chunk, uint64_t chunk_no, bool* integrity_check) const {
    out << "[CHUNK #" << chunk_no << "] " << chunk << "\n";
    Gate* previous = nullptr;
    for(uint64_t gate_id = 0; gate_id < get_num_gates_per_chunk(); gate_id++){
        Gate* current = get_gate(chunk, gate_id);
        print_tabs(out, 1);
        out << "[GATE #" << gate_id << "] ";

        out << "state: ";
        switch(current->m_state){
        case Gate::State::FREE: out << "FREE"; break;
        case Gate::State::READ: out << "READ"; break;
        case Gate::State::WRITE: out << "WRITE"; break;
        case Gate::State::REBAL: out << "REBAL"; break;
        default: out << "UNKNOWN (" << (int) current->m_state << ")"; break;
        }
        out << ", # active threads: " << current->m_num_active_threads;
#if !defined(NDEBUG)
        out << ", locked: ";
        if(current->m_locked){
            out << "yes, by thread id " << current->m_owned_by;
        } else {
            out << "no";
        }
#endif
        out << ", fence keys: < " << current->m_fence_low_key << ", " << current->m_fence_high_key << ">\n";

        if(gate_id != current->id()){
            out << "--> ERROR, the gate id retrieved is " << current->id() << ", expected: " << gate_id << "\n";
            if(integrity_check) *integrity_check = false;
        }
        if(previous != nullptr && current->m_fence_low_key != previous->m_fence_high_key){
            out << "--> ERROR, the low fence key is: " << current->m_fence_low_key << " != from the high fence key of the previous gate: " << previous->m_fence_high_key << "\n";
            if(integrity_check) *integrity_check = false;
        }

        print_tabs(out, 1);
        out << "Separator keys:\n";
        Key key_previous = KEY_MIN;
        for(uint64_t i = 0; i < current->m_num_segments; i++){
            uint64_t segment_id = current->id() * get_num_segments_per_lock() + i / 2;
            uint64_t is_lhs = i % 2 == 0; // whether to use the lhs or rhs of the segment
            Key key_current = current->get_separator_key(i);
            print_tabs(out, 2);
            out << "[" << i << "] segment_id: " << segment_id;
            if(is_lhs) out << " (lhs)"; else out << " (rhs)";
            out << ", key: " << key_current << "\n";

            if(key_previous != KEY_MIN && key_previous > key_current){
                out << "--> ERROR, the separator key " << key_current << " is less than the previous separator key " << key_previous << "\n";
                if(integrity_check) *integrity_check = false;
            }
            key_previous = key_current;
        }

        // dump the segments
        uint64_t segment_start = current->id() * get_num_segments_per_lock();
        uint64_t segment_end = segment_start + get_num_segments_per_lock();
        uint64_t segments_used_space = 0;
        uint64_t sid2g = 0; // correlate the separator keys in the gate with those of the
        for(uint64_t segment_id = segment_start; segment_id < segment_end; segment_id ++) {
            print_tabs(out, 1);
            const SegmentMetadata* segmentcb = get_segment_metadata(chunk, segment_id);
            out << "+-- [SEGMENT #"  << segment_id << "] " << ((void*) segmentcb) << ", free space: " << get_segment_free_space(chunk, segment_id) << " qwords, used space: " << get_segment_used_space(chunk, segment_id) << "\n";

            // separator keys
            Key key_low = current->get_separator_key(sid2g);
            Key key_middle = current->get_separator_key(sid2g +1);
            Key key_high = (sid2g +2 < current->m_num_segments) ? current->get_separator_key(sid2g +2) : current->m_fence_high_key;

            // dump the entries in the segment
            print_tabs(out, 2); out << "Left hand side: \n";
            dump_segment(out, chunk, current, segment_id, /* lhs ? */ true, key_low, key_middle, integrity_check);
            print_tabs(out, 2); out << "Right hand side: \n";
            dump_segment(out, chunk, current, segment_id, /* lhs ? */ false, key_middle, key_high, integrity_check);

            segments_used_space += get_segment_used_space(chunk, segment_id);
            sid2g += 2; // there are two separator keys for each segment: one for the lhs, one for the rhs
        }

        if(segments_used_space != current->m_used_space){
            out << "--> ERROR, the used space registered for the gate (" << current->m_used_space << " qwords) is not equal to the sum of the used spaces for the underlying segments (" << segments_used_space << " qwords)\n";
            if(integrity_check) *integrity_check = false;
        }

        previous = current;
    }
}

void SparseArray::dump_segment(std::ostream& out, const Chunk* chunk, const Gate* gate, uint64_t segment_id, bool is_lhs, Key fence_key_low, Key fence_key_high, bool* integrity_check) const {
    const uint64_t* __restrict c_start = get_segment_content_start(chunk, segment_id, is_lhs);
    const uint64_t* __restrict c_end = get_segment_content_end(chunk, segment_id, is_lhs);
    const uint64_t* __restrict v_start = get_segment_versions_start(chunk, segment_id, is_lhs);
    const uint64_t* __restrict v_end = get_segment_versions_end(chunk, segment_id, is_lhs);

    // iterate over the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    int64_t v_backptr = 0;
    const SegmentVertex* vertex = nullptr;
    const SegmentEdge* edge = nullptr;
    const SegmentVersion* version = nullptr;

    while(c_index < c_length){
        // Fetch a vertex
        vertex = get_vertex(c_start + c_index);
        edge = nullptr;
        version = nullptr;

        if(v_index < v_length && get_backptr(get_version(v_start + v_index)) == v_backptr){
            version = get_version(v_start + v_index);
            v_index += OFFSET_VERSION;
        }

        dump_segment_item(out, v_backptr, vertex, edge, version, integrity_check);
        dump_validate_key(out, vertex, edge, fence_key_low, fence_key_high, integrity_check);

        c_index += OFFSET_VERTEX;
        v_backptr++;

        // Fetch its edges
        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
        while(c_index < e_length){
            edge = get_edge(c_start + c_index);
            version = nullptr;

            if(v_index < v_length && get_backptr(get_version(v_start + v_index)) == v_backptr){
                version = get_version(v_start + v_index);
                v_index += OFFSET_VERSION;
            }

            dump_segment_item(out, v_backptr, vertex, edge, version, integrity_check);
            dump_validate_key(out, vertex, edge, fence_key_low, fence_key_high, integrity_check);

            // next iteration
            c_index += OFFSET_EDGE;
            v_backptr++;
        }
    }

    if(v_index != v_length){
        out << "--> ERROR, not all version records have been read: v_index: " << v_index << ", v_length: " << v_length << "\n";
        if(integrity_check) *integrity_check = false;
    }
}

void SparseArray::dump_segment_item(std::ostream& out, uint64_t position, const SegmentVertex* vertex, const SegmentEdge* edge, const SegmentVersion* version, bool* integrity_check) const {
    print_tabs(out, 3);
    out << "[" << position << "] ";
    if(edge == nullptr){
        out << "Vertex " << vertex->m_vertex_id;
    } else {
        out << "Edge " << vertex->m_vertex_id << " -> " << edge->m_destination << ", weight: " << edge->m_weight;
    }

    if(version != nullptr){
        out << ", [version present] ";
        out << "type: " << (is_insert(version) ? "insert" : "remove") << ", ";
        out << "back pointer: " << get_backptr(version) << ", ";
        out << "chain length: ";
        if(version->m_undo_length == MAX_UNDO_LENGTH){
            out << ">= " << MAX_UNDO_LENGTH << ", ";
        } else {
            out << version->m_undo_length << ", ";
        }
        out << "undo pointer: " << get_undo(version) << "\n";

        if(get_backptr(version) != position){
            out << "--> ERROR, the back pointer (" << get_backptr(version) << ") does not match the position of the record (" << position << ")\n";
            if(integrity_check) *integrity_check = false;
        }

        dump_unfold_undo(out, get_undo(version)); // do not insert a "\n" above
    }
    else {
        out << "\n";
    }
}


void SparseArray::dump_unfold_undo(std::ostream& out, const teseo::internal::context::Undo* undo) const {
    while(undo != nullptr) {
        const Transaction* tx = undo->transaction();
        uint64_t read_id = tx->ts_read();
        uint64_t write_id = tx->ts_write();

        out << ", version: " << read_id;
        if(read_id != write_id){
            out << " (locked tx " << write_id << ")";
        }
        out << "\n";

        Update* update = reinterpret_cast<Update*>(undo->payload());
        Undo* next = undo->next();
        print_tabs(out, 4); out << " update: {" << *update << "}, next: ";

        undo = next;
    }

    out << ", version: 0 (nullptr)\n";

}

void SparseArray::dump_validate_key(std::ostream& out, const SegmentVertex* vertex, const SegmentEdge* edge, Key fence_key_low, Key fence_key_high, bool* integrity_check) const {
    Key key;
    if(edge == nullptr){
        key.set(vertex->m_vertex_id);
    } else {
        key.set(vertex->m_vertex_id, edge->m_destination);
    }

    if(key < fence_key_low){
        out << "--> ERROR, the key above is lesser than the low fence key: " << fence_key_low << "\n";
        if(integrity_check != nullptr) *integrity_check = false;
    } else if (key >= fence_key_high){
        out << "--> ERROR, the key above is greater or equal than the high fence key: " << fence_key_high << "\n";
        if(integrity_check != nullptr) *integrity_check = false;
    }
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

