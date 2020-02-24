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

#include "context.hpp"
#include "error.hpp"
#include "gate.hpp"
#include "index.hpp"
#include "utility.hpp"

using namespace std;
using namespace teseo::internal;


namespace teseo::internal::memstore {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_CLASS_NAME "SparseArray"
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[" << COUT_CLASS_NAME << "::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
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

SparseArray::SparseArray(InitSparseArrayInfo init) : m_num_gates_per_chunk(init.m_num_gates_per_chunk), m_num_segments_per_lock(init.m_num_segments_per_lock), m_num_qwords_per_segment(init.m_num_qwords_per_segment){

}

SparseArray::InitSparseArrayInfo SparseArray::compute_alloc_params(uint64_t num_qwords_per_segment, uint64_t num_segments_per_gate, uint64_t memory_footprint) {
    InitSparseArrayInfo init;

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

SparseArray::Chunk* SparseArray::get_chunk(const IndexEntry& entry){
    return reinterpret_cast<Chunk*>(entry.m_chunk_id);
}

const SparseArray::Chunk* SparseArray::get_chunk(const IndexEntry& entry) const {
    return reinterpret_cast<const Chunk*>(entry.m_chunk_id);
}

Gate* SparseArray::get_gate(const Chunk* chunk, uint64_t id) {
    assert(id < get_num_gates_per_chunk() && "Invalid gate_id");
    uint64_t* base_ptr = reinterpret_cast<uint64_t*>(const_cast<Chunk*>(chunk +1));
    return reinterpret_cast<Gate*>(get_num_qwords_per_gate() * id);
}

const Gate* SparseArray::get_gate(const Chunk* chunk, uint64_t id) const {
    assert(id < get_num_gates_per_chunk() && "Invalid gate_id");
    const uint64_t* base_ptr = reinterpret_cast<const uint64_t*>(chunk +1);
    return reinterpret_cast<Gate*>(get_num_qwords_per_gate() * id);
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
    SegmentMetadata* md = get_segment_metadata(chunk, segment_id);
    return md->m_empty2_start - md->m_empty1_start;
}

uint64_t SparseArray::get_segment_used_space(const Chunk* chunk, uint64_t segment_id) const {
    assert(get_segment_free_space(chunk, segment_id) <= get_num_qwords_per_segment());
    return get_num_qwords_per_segment() - get_segment_free_space(chunk, segment_id);
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

void SparseArray::set_undo(SegmentDeltaMetadata* metadata, UndoEntry* undo) {
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

SparseArray::SegmentStaticEdge* SparseArray::get_static_edge(uint64_t* ptr){
    return reinterpret_cast<SegmentStaticEdge*>(ptr);
}

SparseArray::SegmentDeltaMetadata* SparseArray::get_delta_header(uint64_t* ptr){
    return reinterpret_cast<SegmentDeltaMetadata*>(ptr);
}

SparseArray::SegmentDeltaVertex* SparseArray::get_delta_vertex(uint64_t* ptr){
    assert(is_vertex(get_delta_header(ptr)));
    return reinterpret_cast<SegmentDeltaVertex*>(ptr);
}

SparseArray::SegmentDeltaEdge* SparseArray::get_delta_edge(uint64_t* ptr){
    assert(is_edge(get_delta_header(ptr)));
    return reinterpret_cast<SegmentDeltaEdge*>(ptr);
}

UndoEntry* SparseArray::get_delta_undo(uint64_t* ptr){
    return get_delta_undo(get_delta_header(ptr));
}

UndoEntry* SparseArray::get_delta_undo(SegmentDeltaMetadata* ptr){
    return reinterpret_cast<UndoEntry*>(ptr->m_version);
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

            if(!update_done){
                do_rebalance(gate);
            } else {
                writer_on_exit(gate, object);
            }

//            bool inserted = do_insert(gate, key, value);
//            if(!inserted){ // this is going to take a while
//                rebalance_global(gate);
//            } else {
//                insert_on_exit(gate);
//                done = true;
//            }
        } catch (Abort) { }
    } while(!done);
}

auto SparseArray::writer_on_entry(const Update& update) -> std::pair<Chunk*, Gate*> {
    ThreadContext* context = ThreadContext::context();
    assert(context != nullptr);
    context->epoch_enter();

    IndexEntry leaf_addr = reinterpret_cast<IndexEntry>(m_index->find(update.m_source, update.m_destination));
    Chunk* chunk = get_chunk(leaf_addr);
    int64_t gate_id = leaf_addr.m_gate_id;
    Gate* gate = nullptr;
    Key search_key(get_key(update));

    bool done = false;

    do {
        bool neighbour_leaf = false;
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

bool SparseArray::do_write_gate(Chunk* chunk, Gate* gate, Update& update) {
    COUT_DEBUG("Gate: " << gate->id() << ", update: " << update);

    uint64_t g2sid = gate->find(get_key(update));
    uint64_t segment_id = gate->id() * get_num_segments_per_lock() + g2sid / 2;
    uint64_t is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment
    bool is_minimum_updated = false;

    bool is_update_done = do_write_segment(chunk, segment_id, is_lhs, update, /* out */ &is_minimum_updated);


//    Segment* segment = leaf->get_segment_rel(gate->id(), segment_id /2);
//
//    bool update_done = segment->update(object);
//
//    if(!update_done){
//        bool rebalance_done = leaf->rebalance_gate(gate->id(), segment_id /2);
//        if(!rebalance_done) return false; // there is not enough space in this segment
//
//        // try again ...
//        segment_id = gate->find(Key(object));
//        object.m_segment_lhs = (segment_id % 2) == 0;
//        segment = leaf->get_segment_rel(gate->id(), segment_id /2);
//#if !defined(NDEBUG)
//        update_done = segment->update(object);
//        assert(update_done == true && "We just rebalanced!");
//#else
//        segment->update(object);
//#endif
//
//    }
//
//    if(object.m_minimum_updated){
//        gate->set_separator_key(segment_id, Key(object));
//    }

    return true; // done
}


/*****************************************************************************
 *                                                                           *
 *   Raw writes in the segment                                               *
 *                                                                           *
 *****************************************************************************/

bool SparseArray::do_write_segment(Chunk* chunk, uint64_t segment_id, bool is_lhs, Update& update, bool* out_minimum_updated){
    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
    uint64_t empty_space = get_segment_free_space(chunk, segment_id);

    if(is_vertex(update)){
        // we need at least 2 qwords of empty space
        if(empty_space < (sizeof(SegmentDeltaVertex) / 8)) return false;

        do_write_segment_vertex(chunk, segment_id, is_lhs, update, out_minimum_updated);
        return true;
    } else {
        assert(update.m_entry_type == Update::Edge);
        if(empty_space < (sizeof(SegmentDeltaEdge) / 8)) return false;

        do_write_segment_edge(chunk, segment_id, is_lhs, update, out_minimum_updated);
        return true;
    }
}

void SparseArray::do_write_segment_vertex(Chunk* chunk, uint64_t segment_id, bool is_lhs, Update& update, bool* out_minimum_updated) {
    assert(get_segment_free_space(chunk, segment_id) >= sizeof(SegmentDeltaVertex) / 8); // Otherwise there isn't enough space to create a delta entry
    const uint64_t vertex_id = update.m_source;
    bool is_new_minimum = true; // whether the record inserted will be the new segment minimum

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
                is_new_minimum = false;
                delta_pos += sizeof(SegmentDeltaVertex) / 8; // move ahead
            } else if ( delta_vertex->m_vertex_id == vertex_id ){
                if(! UndoEntry::can_write(get_delta_undo(delta_vertex)) ){
                    RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the vertex ID " << vertex_id << " has been modified by another transaction. Restart this transaction to alter this object");
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
                is_new_minimum = false;
                delta_pos += sizeof(SegmentDeltaEdge) / 8; // move ahead
            } else {
                stop = true;
            }
        }
    }

    if(is_new_minimum && static_start < static_end){
        is_new_minimum = get_static_vertex(static_start)->m_vertex_id > vertex_id;
    }

    // second, check the static storage whether the vertex to insert already exists
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
        ptr_record = get_delta_vertex(delta_start + delta_pos);
        ptr_record->m_vertex_id = vertex_id;
        reset_header(ptr_record, update);
    }

    // Transaction management
    set_undo(ptr_record, ThreadContext::transaction()->create_undo_entry<UndoEntryVertex>(
            get_delta_undo(ptr_record),
            is_insert(update) ? UndoType::VERTEX_REMOVE : UndoType::VERTEX_ADD,
            vertex_id)
    );

    if(out_minimum_updated){
        *out_minimum_updated = is_new_minimum;
    }
}

void SparseArray::do_write_segment_edge(Chunk* chunk, uint64_t segment_id, bool is_lhs, Update& update, bool* out_minimum_updated) {
    assert(get_segment_free_space(chunk, segment_id) >= sizeof(SegmentDeltaEdge) / 8); // Otherwise there isn't enough space to create a delta entry

    bool is_new_minimum = true; // whether the record inserted will be the new segment minimum

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
                is_new_minimum = false;
                delta_pos += sizeof(SegmentDeltaVertex) / 8; // move ahead
            } else {
                stop = true;
            }
        } else { // it's an edge
            assert(is_edge(header));
            auto delta_edge = get_delta_edge(delta_start + delta_pos);

            if(delta_edge->m_source < update.m_source || (delta_edge->m_source == update.m_source && delta_edge->m_destination < update.m_destination)){
                is_new_minimum = false;
                delta_pos += sizeof(SegmentDeltaEdge) / 8; // move ahead
            } else if (delta_edge->m_source == update.m_source){
                if(!UndoEntry::can_write(get_delta_undo(header))){
                    RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the edge " << update.m_source << " -> " <<
                            update.m_destination << " has been modified by another transaction. Restart this transaction to alter this object");
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

    if(is_new_minimum && static_start < static_end){
        is_new_minimum = get_static_vertex(static_start)->m_vertex_id > update.m_source;
    }
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/


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

