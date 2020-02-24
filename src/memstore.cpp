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

#include "memstore.hpp"

#include <cassert>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>

#include "context.hpp"
#include "index.hpp"
#include "utility.hpp"

using namespace std;

namespace teseo::internal {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex g_debugging_mutex [[maybe_unused]]; // context.cpp
#define DEBUG
#define COUT_CLASS_NAME "Unknown"
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[" << COUT_CLASS_NAME << "::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   MemStore                                                                *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "MemStore"

void MemStore::write(Object& object) {
    bool done = false;
    do {
        Leaf* leaf {nullptr};
        Gate* gate {nullptr};
        int64_t space_used = 0; //


        try {
            ScopedEpoch epoch;
            std::tie(leaf, gate) = writer_on_entry(object); // lock the gate where we're going to update the new element
            assert(leaf != nullptr && "Null leaf");
            assert(gate != nullptr && "Null lock");

            bool update_done = do_write(leaf, gate, object);

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

auto MemStore::writer_on_entry(Object& object) -> pair<Leaf*, Gate*>  {
    ThreadContext* context = ThreadContext::context();
    assert(context != nullptr);
    context->epoch_enter();

    IndexEntry leaf_addr = reinterpret_cast<IndexEntry>(m_index->find(object.m_source, object.m_destination));
    Leaf* leaf = reinterpret_cast<Leaf*>(leaf_addr.m_leaf_address);
    int64_t gate_id = leaf_addr.m_gate_id;
    Gate* gate = nullptr;
    Key user_key(object);

    bool done = false;

    do {
        bool neighbour_leaf = false;
        Key index_key;

        gate = leaf->get_gate(gate_id);

        unique_lock<Gate> lock(*gate);
        auto direction = gate->check_fence_keys(user_key);
        switch(direction){
        case Gate::Direction::LEFT:
            gate_id--;
            if(gate_id < 0){ throw Abort{}; } // go to the previous leaf
            break;
        case Gate::Direction::RIGHT:
            gate_id++;
            if(gate_id >= leaf->num_gates()){ throw Abort{}; } // go to the next leaf
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

    return std::make_pair(leaf, gate);
}

bool MemStore::do_write(Leaf* leaf, Gate* gate, Object& object) {
    assert(gate != nullptr && "Null pointer");
    COUT_DEBUG("Gate: " << gate->id() << ", object: " << object);

    uint64_t segment_id = gate->find(Key(object));
    object.m_segment_lhs = (segment_id % 2) == 0;
    Segment* segment = leaf->get_segment_rel(gate->id(), segment_id /2);

    bool update_done = segment->update(object);

    if(!update_done){
        bool rebalance_done = leaf->rebalance_gate(gate->id(), segment_id /2);
        if(!rebalance_done) return false; // there is not enough space in this segment

        // try again ...
        segment_id = gate->find(Key(object));
        object.m_segment_lhs = (segment_id % 2) == 0;
        segment = leaf->get_segment_rel(gate->id(), segment_id /2);
#if !defined(NDEBUG)
        update_done = segment->update(object);
        assert(update_done == true && "We just rebalanced!");
#else
        segment->update(object);
#endif

    }

    if(object.m_minimum_updated){
        gate->set_separator_key(segment_id, Key(object));
    }

    return true; // done
}

//void MemStore::writer_on_exit(Leaf* leaf, Gate* gate, int64_t space_left_diff, bool rebalance){
//    assert(gate != nullptr);
//    bool unlock_rebalancer { false };
//
//    gate->lock();
//    gate->m_space_left += space_left_diff;
//    gate->m_num_active_threads = 0;
//
//    switch(gate->m_state){
//    case Gate::State::WRITE:
//        // same state as before
//
//        if(rebalance){
//            gate->m_state = Gate::State::REBAL;
//        } else {
//            gate->m_state = Gate::State::FREE;
//            gate->wake_next();
//        }
//
//        break;
//    case Gate::State::REBAL:
//        // another thread wants to rebalance this gate/segment
//        unlock_rebalancer = true;
//        break;
//    default:
//        assert(0 && "Invalid state");
//    }
//
//    gate->unlock();
//
//    const size_t gate_id = gate->lock_id();
//    if(unlock_rebalancer){
//        m_rebalancer->exit(gate_id);
//    } else if (rebalance){
//        m_rebalancer->rebalance(gate_id);
//    }
//}

template<typename Lock>
void MemStore::writer_wait(Gate& gate, Lock& lock){
    std::promise<void> producer;
    std::future<void> consumer = producer.get_future();
    gate.m_queue.append({ Gate::State::WRITE, &producer } );
    lock.unlock();
    consumer.wait();
}


/*****************************************************************************
 *                                                                           *
 *   Key                                                                     *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "MemStore::Key"

// Key, convenience class
MemStore::Key::Key() : Key(numeric_limits<uint64_t>::max(), numeric_limits<uint64_t>::max()) { }
MemStore::Key::Key(uint64_t vertex_id) : Key(vertex_id, 0) { }
MemStore::Key::Key(uint64_t src, uint64_t dst) : m_source(src), m_destination(dst) { }
MemStore::Key::Key(const Object& object): Key(object.m_source, object.m_destination) { }
uint64_t MemStore::Key::get_source() const { return m_source; }
uint64_t MemStore::Key::get_destination() const { return m_destination; }
void MemStore::Key::set(uint64_t vertex_id) { m_source = vertex_id; m_destination = 0; }
void MemStore::Key::set(uint64_t source, uint64_t destination) { m_source = source; m_destination = destination; }
bool MemStore::Key::operator==(const Key& other) const { return get_source() == other.get_source() && get_destination() == other.get_destination(); }
bool MemStore::Key::operator!=(const Key& other) const { return !(*this == other); }
bool MemStore::Key::operator<(const Key& other) const { return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() < other.get_destination()); }
bool MemStore::Key::operator<=(const Key& other) const { return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() <= other.get_destination()); }
bool MemStore::Key::operator>(const Key& other) const { return !(*this <= other); }
bool MemStore::Key::operator>=(const Key& other) const { return !(*this < other); }
MemStore::Key MemStore::Key::min(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::min(), numeric_limits< decltype(Key{}.get_destination()) >::min()); }
MemStore::Key MemStore::Key::max(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::max(), numeric_limits< decltype(Key{}.get_destination()) >::max()); }
std::ostream& operator<<(std::ostream& out, const MemStore::Key& key){
    out << key.get_source() << " -> " << key.get_destination();
    return out;
}

/*****************************************************************************
 *                                                                           *
 *   Gate                                                                    *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "MemStore::Gate"

MemStore::Gate::Gate(uint64_t gate_id, uint64_t num_segments) : m_gate_id(gate_id), m_num_segments(num_segments) {
    m_num_active_threads = 0;
    m_space_left = 0;
    m_fence_low_key = m_fence_high_key = Key::max();

    // Init the separator keys
    for(int64_t i = 0; i < window_length(); i++){
        set_separator_key(window_start() + i, Key::max());
    }
}

MemStore::Gate::~Gate() {
    // nop
}

int64_t MemStore::Gate::id() const noexcept {
    return m_gate_id;
}

int64_t MemStore::Gate::window_start() const noexcept {
    return id() * window_length();
}

int64_t MemStore::Gate::window_length() const noexcept {
    return m_num_segments;
}

auto MemStore::Gate::separator_keys() -> Key* {
    return reinterpret_cast<Key*>( reinterpret_cast<uint8_t*>(this) + sizeof(Gate) );
}

auto MemStore::Gate::separator_keys() const -> const Key* {
    return const_cast<MemStore::Gate*>(this)->separator_keys();
}

void MemStore::Gate::lock(){
    m_spin_lock.lock();
#if !defined(NDEBUG)
    barrier();
    assert(m_locked == false && "Spin lock already acquired");
    m_locked = true;
    m_owned_by = get_thread_id();
    barrier();
#endif
}

void MemStore::Gate::unlock(){
#if !defined(NDEBUG)
    barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    barrier();
#endif
    m_spin_lock.unlock();
}

uint64_t MemStore::Gate::find(Key key) const {
    assert(m_fence_low_key <= key && key <= m_fence_high_key && "Fence keys check: the key does not belong to this gate");
    int64_t i = 0, sz = window_length() -1;
    const Key* __restrict keys = separator_keys();
    while(i < sz && keys[i] <= key) i++;
    return i;
}

void MemStore::Gate::set_separator_key(size_t segment_id, Key key){
//    assert(segment_id >= window_start() && segment_id < window_start() + window_length());
    assert(segment_id >= 0 && segment_id < window_length());

    if(segment_id > 0){
        separator_keys()[segment_id -1] = key;
    }

#if !defined(NDEBUG)
    if(segment_id > 0) { assert(get_separator_key(segment_id) == key); } // otherwise it's given by the fence key
#endif
}

auto MemStore::Gate::get_separator_key(uint64_t segment_id) const -> Key {
    assert(segment_id >= 0 && segment_id < window_length());

    if(segment_id == 0)
        return m_fence_low_key;
    else
        return separator_keys()[segment_id -1];
}

MemStore::Gate::Direction MemStore::Gate::check_fence_keys(Key key) const {
    assert(m_locked && m_owned_by == get_thread_id() && "To perform this check the lock must have been acquired by the same thread currently operating");
    if(m_fence_low_key == Key::max())  // this array is not valid anymore, restart the operation
        return Direction::INVALID;
    else if(key < m_fence_low_key)
        return Direction::LEFT;
    else if(key > m_fence_high_key)
        return Direction::RIGHT;
    else
        return Direction::GO_AHEAD;
}

void MemStore::Gate::set_fence_keys(Key min, Key max){
    m_fence_low_key = min;
    m_fence_high_key = max;
}

uint64_t MemStore::Gate::memory_footprint(uint64_t num_segments){
    if(num_segments > 0) num_segments--; // because the first separator key is implicitly stored as fence key
    uint64_t min_space = sizeof(Gate) + num_segments * sizeof(Key);
    assert(min_space % 8 == 0 && "Expected at least to be aligned to the word");
    return min_space;
}

/*****************************************************************************
 *                                                                           *
 *   Segment                                                                 *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "MemStore::Segment"

MemStore::Segment::Segment(uint64_t space) : m_delta1_start(0), m_delta2_start(space), m_empty1_start(0), m_empty2_start(space) { }

void MemStore::Segment::set_section_offsets(uint64_t delta1_start, uint64_t delta2_start, uint64_t empty1_start, uint64_t empty2_start){
    m_delta1_start = delta1_start;
    m_delta2_start = delta2_start;
    m_empty1_start = empty1_start;
    m_empty2_start = empty2_start;
}

uint64_t* MemStore::Segment::data() { return reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(this) + sizeof(Segment)); }
const uint64_t* MemStore::Segment::data() const { return const_cast<Segment*>(this)->data(); }

uint64_t MemStore::Segment::space_left() const { return m_empty2_start - m_empty1_start; }

bool MemStore::Segment::insert_lhs(uint64_t vertex_id){
    assert(space_left() >= sizeof(DynamicVertex) && "There is no space left in this segment");

    // find the position where to insert the item
    uint64_t* __restrict data_delta = data() + m_delta1_start;
    bool stop = false;
    uint64_t i = 0, end = m_empty1_start - m_delta1_start;
    DynamicVertex* previous_vertex_entry = nullptr;
    UndoEntryVertex* previous_undo_entry = nullptr;
    while(i < end && !stop){
        auto entry = reinterpret_cast<DynamicEntry*>(data_delta + i);
        switch(entry->m_entity){
        case 0: { // vertex id
            DynamicVertex* vertex_entry = reinterpret_cast<DynamicVertex*>(data_delta + i);
            if(vertex_entry->m_vertex_id < vertex_id){
                i += sizeof(DynamicVertex); // go on
            } else if (vertex_entry->m_vertex_id == vertex_id){
                previous_vertex_entry = vertex_entry;
                if(! UndoEntry::can_write(vertex_entry->m_version)) {
                    RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the vertex ID " <<  vertex_id << " has been modified by another transaction. Restart the transaction to alter this object");
                } else if(vertex_entry->m_insdel == 0){ // this vertex is already present
                    RAISE_EXCEPTION(LogicalError, "The vertex ID " << vertex_id << " already exists");
                }
                previous_undo_entry = reinterpret_cast<UndoEntryVertex*>(vertex_entry->m_version);
            }

            stop = vertex_entry->m_vertex_id >= vertex_id;
            break;
        }
        case 1: { // edge
            // FIXME
            break;
        }
        }
    }

    // shift all existing entries of 2 words (== sizeof(DynamicEntry)) to the right
    if(previous_vertex_entry == nullptr) {
        memmove(data_delta + i + sizeof(DynamicEntry), data_delta + i, static_cast<int64_t>(m_empty1_start - m_delta1_start - i) * 8);
        m_empty1_start += sizeof(DynamicEntry) / 8;
    }

    // insert the item
    auto new_vertex = reinterpret_cast<DynamicVertex*>(data_delta + i);
    new_vertex->m_insdel = 0; // 0 = insertion, 1 = deletion
    new_vertex->m_entity = 0; // 0 = vertex, 1 = edge
    new_vertex->m_version = reinterpret_cast<uint64_t>(ThreadContext::transaction()->create_undo_entry<UndoEntryVertex>(previous_undo_entry, UndoType::VERTEX_REMOVE, vertex_id)); // UNDO: REMOVE 42
    new_vertex->m_vertex_id = vertex_id;

    bool minimum = (i == 0); // TODO, check the static part as well

    return minimum;
}

bool MemStore::Segment::update(Object& object) {
    if(object.m_type == Object::Type::VERTEX){
        if(space_left() < 2){ return false; }
        switch(object.m_action){
        case Object::Action::INSERT:{
            if(object.m_segment_lhs){
                object.m_minimum_updated = insert_lhs(object.m_source);
            } else {
                assert(0 && "Not implemented yet");
            }
        } break;
        case Object::Action::REMOVE: {
            if(object.m_segment_lhs){
                assert(0 && "Not implemented yet");
            } else {
                assert(0 && "Not implemented yet");
            }
        } break;

        }

        object.m_space_diff -= 2;
        return true;
    }

    assert(0 && "Not implemented yet");
    return true;
}

void MemStore::Segment::dump() const {

}

/*****************************************************************************
 *                                                                           *
 *   Leaf                                                                    *
 *                                                                           *
 *****************************************************************************/

#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "MemStore::Leaf"

MemStore::Leaf::Leaf(uint16_t num_gates, uint16_t num_segments_per_gate, uint32_t space_per_segment) : m_num_gates(num_gates), m_num_segments_per_gate(num_segments_per_gate), m_space_per_segment(space_per_segment){
    // init the gates
    for(int i = 0, N = num_gates; i < N; i++){
        new (get_gate(i)) Gate(i, num_segments_per_gate /* lhs and rhs */ *2);
        get_gate(i)->m_space_left = (((uint64_t) num_segments_per_gate) * (space_per_segment - sizeof(Segment))) / 8;
    }

    // init the segments
    for(int i = 0; i < num_segments(); i++){
        new (get_segment_abs(i)) Segment((m_space_per_segment - sizeof(Segment)) / 8);
    }
}

MemStore::Leaf::~Leaf(){
    for(int i = 0, N = num_segments(); i < N; i++){
        get_segment_abs(i)->~Segment();
    }

    for(int i = 0, N = num_gates(); i < N; i++){
        get_gate(i)->~Gate();
    }
}

MemStore::Leaf* MemStore::Leaf::allocate(uint64_t memory_budget, uint64_t num_segments_per_gate, uint64_t space_per_segment){
    COUT_DEBUG("memory_budget: " << memory_budget << " bytes, segments per gate: " << num_segments_per_gate << ", space per segment: " << space_per_segment << " bytes");
    if(memory_budget % 8 != 0) RAISE_EXCEPTION(InternalError, "The memory budget is not a multiple of 8 ")
    if(memory_budget < (space_per_segment * 4)) RAISE_EXCEPTION(InternalError, "The memory budget must be at least 4 times the space per segment");
    if(num_segments_per_gate == 0) RAISE_EXCEPTION(InternalError, "Great, 0 segments per gates");
    if(space_per_segment % 8 != 0) RAISE_EXCEPTION(InternalError, "The space per segment should also be a multiple of 8");
    if(space_per_segment == 0) RAISE_EXCEPTION(InternalError, "The space per segment is 0");

    // 1. Decide the memory layout of the leaf
    // 1a) compute the amount of space required by a single gate and all of its associated segments
    double gate_total_sz = Gate::memory_footprint(num_segments_per_gate) + num_segments_per_gate * (sizeof(Segment) + space_per_segment);
    // 1b) solve the inequality LeafSize + x * gate_total_sz >= memory_budget, where x will be our final number of gates
    double num_gates = ceil( (static_cast<double>(memory_budget) - sizeof(Leaf) ) / gate_total_sz);
    // 1c) how many bytes we need to remove to each segment from 'space_per_segment', so that our memory budget is satisfied
    double surplus_total = gate_total_sz * num_gates - memory_budget;
    double surplus_per_segment = ceil(surplus_total / (num_gates * num_segments_per_gate));
    // 1d) compute the new amount of space that can be given to each segment
    uint64_t new_space_per_segment = space_per_segment - static_cast<uint64_t>(surplus_per_segment);
    // 1e) round up to the previous multiple of 8
    new_space_per_segment -= (new_space_per_segment % 8);

#if defined(DEBUG)
    COUT_DEBUG("num gates: " << num_gates << ", segments per gates: " << num_segments_per_gate << ", bytes per segments (incl. header): " << new_space_per_segment + sizeof(Segment));
    uint64_t space_used = (Gate::memory_footprint(num_segments_per_gate) + num_segments_per_gate * new_space_per_segment) * num_gates + sizeof(Leaf);
    COUT_DEBUG("space used: " << space_used << "/" << memory_budget << " bytes (" << (((double) space_used) / memory_budget) * 100.0 << " %)");
#endif


    // 2. Allocate the leaf
    void* heap { nullptr };
    int rc = posix_memalign(&heap, /* alignment = */ memory_budget,  /* size = */ memory_budget);
    if(rc != 0) throw std::runtime_error("MemStore::Leaf::allocate, cannot obtain a chunk of aligned memory");
    Leaf* leaf = new (heap) Leaf((uint16_t) num_gates, (uint16_t) num_segments_per_gate, (uint32_t) new_space_per_segment + /* header */ sizeof(Segment));

    return leaf;
}

void MemStore::Leaf::deallocate(Leaf* leaf){
    leaf->~Leaf();
    free(leaf);
}

int64_t MemStore::Leaf::num_gates() const {
    return m_num_gates;
}

int64_t MemStore::Leaf::num_segments() const {
    return num_gates() * m_num_segments_per_gate;
}

int64_t MemStore::Leaf::get_space_per_segment_in_qwords() const {
    assert((m_space_per_segment - sizeof(Segment)) % 8 == 0);
    return (m_space_per_segment - sizeof(Segment)) / 8;
}

int MemStore::Leaf::height_calibrator_tree() const noexcept {
    return floor(log2(num_segments())) +1.0;
}

uint64_t MemStore::Leaf::get_total_gate_size() const {
    return Gate::memory_footprint(m_num_segments_per_gate) + ((uint64_t) m_num_segments_per_gate) * m_space_per_segment;
}

MemStore::Gate* MemStore::Leaf::get_gate(uint64_t gate_id){
    assert((gate_id >= 0 && gate_id < num_gates()) && "Invalid gate_id");
    return reinterpret_cast<Gate*>( reinterpret_cast<uint8_t*>(this) + sizeof(Leaf) + get_total_gate_size() * gate_id );
}

MemStore::Segment* MemStore::Leaf::get_segment_abs(uint64_t segment_id){
    assert((segment_id >= 0 && segment_id < num_segments()) && "Invalid segment_id");

    Gate* gate = get_gate(segment_id / num_segments_per_gate());
    uint64_t relative_segment_id = segment_id % m_num_segments_per_gate;
    uint8_t* segment_start_offset = reinterpret_cast<uint8_t*>(gate) + Gate::memory_footprint(m_num_segments_per_gate) + relative_segment_id * m_space_per_segment;
    return reinterpret_cast<Segment*>(segment_start_offset);
}

const MemStore::Segment* MemStore::Leaf::get_segment_abs(uint64_t segment_id) const {
    return const_cast<MemStore::Leaf*>(this)->get_segment_abs(segment_id);
}

MemStore::Segment* MemStore::Leaf::get_segment_rel(uint64_t gate_id, uint64_t segment_id){
    return get_segment_abs(gate_id * num_segments_per_gate() + segment_id);
}

int64_t MemStore::Leaf::get_space_filled_in_qwords(uint64_t segment_id) const {
    return get_space_per_segment_in_qwords() - get_segment_abs(segment_id)->space_left();
}

std::pair<int64_t, int64_t> MemStore::Leaf::get_thresholds(int height) const {
    // first compute the density for the given height
    double rho {DENSITY_RHO_0}, tau {DENSITY_TAU_0};
    const int tree_height = height_calibrator_tree();

    // avoid diving by zero
    if(tree_height > 1){
        const double scale = static_cast<double>(tree_height - height) / static_cast<double>(tree_height -1);
        rho = /* max density */ DENSITY_RHO_H - /* delta */ (DENSITY_RHO_H - DENSITY_RHO_0) * scale;
        tau = /* min density */ DENSITY_TAU_H + /* delta */ (DENSITY_TAU_0 - DENSITY_TAU_H) * scale;
    }

    int64_t num_segs = std::min<int64_t>(num_segments(), pow(2.0, height -1));
    int64_t words_per_segments = get_space_per_segment_in_qwords();
    int64_t min_space = num_segs * words_per_segments * rho;
    int64_t max_space = num_segs * (words_per_segments - /* always leave 5 qwords of space in each segment */ 5) * tau;
    if(min_space >= max_space) min_space = max_space - 1;

    return std::make_pair(min_space, max_space);
}

MemStore::RebalancePlan MemStore::Leaf::rebalance_plan(int64_t segment_id, int64_t max_window_start, int64_t max_window_length) const {
    int64_t max_window_end = max_window_start + max_window_length;
    assert(0 <= max_window_start && "Undeflow");
    assert(max_window_start < max_window_end)
    assert(max_window_end <= num_segments() && "Overflow");
    assert(max_window_start <= segment_id && segment_id < max_window_end && "The segment is not in the provided segment");

    int64_t window_length = 1;
    int64_t window_id = segment_id;
    int64_t window_start = segment_id /* incl */, window_end = segment_id +1 /* excl */;
    int64_t cardinality = get_space_filled_in_qwords(segment_id); /* misnomer, it's the space used in truth */
    int height = 1;
    int max_height = floor(log2(max_window_end - max_window_start)) +1.0;
    int64_t min_cardinality = 0, max_cardinality = numeric_limits<int64_t>::max();

    // determine the window to rebalance
    if(height_calibrator_tree() > 1){
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
                int offset = max_window_end - num_segments;
                window_start -= offset;
                window_end -= offset;
                if(window_start < max_window_start){ window_start = max_window_start; }
            } else if (window_start < max_window_start){
                int offset = max_window_start - window_start;
                window_start += offset;
                window_end += offset;
                if(window_end > max_window_end){ window_end = max_window_end; }
            }

            // find the number of elements in the interval
            while(index_left >= window_start){
                cardinality += get_space_filled_in_qwords(index_left);
                index_left--;
            }
            while(index_right < window_end){
                cardinality += get_space_filled_in_qwords(index_right);
                index_right++;
            }


            std::tie(min_cardinality, max_cardinality) = get_thresholds(height);

        } while(cardinality > max_cardinality && height < max_height);
    }

    COUT_DEBUG("min card: " << min_cardinality << ", cardinality: " << cardinality << ", max card: " << max_cardinality << ", height: " << height << ", max height: " << max_height);

    if(cardinality < max_cardinality){
        return RebalancePlan{ RebalanceAction::SPREAD, window_start, window_end };
    } else {
        return RebalancePlan{ RebalanceAction::SPLIT, 0, 0 };
    }
}

bool MemStore::Leaf::rebalance_gate(uint64_t gate_id, uint64_t segment_id) {
    assert(gate_id >= 0 && gate_id < num_gates() && "Overflow");
    assert(segment_id >= 0 && segment_id < num_segments_per_gate() && "Overflow");
    assert(get_gate(gate_id)->m_locked == true && get_gate(gate_id)->m_owned_by == get_thread_id());

    auto plan = rebalance_plan(segment_id, gate_id * num_segments_per_gate(), (gate_id +1) * num_segments_per_gate() -1);
    if(plan != RebalanceAction::SPREAD) return false;


    return true;
}



void MemStore::Leaf::dump() const {
    cout << "LEAF, num gates: " << num_gates() << ", num segments: " << num_segments() << ", segments per gate: " << m_num_segments_per_gate << ", space per segment (incl. header): " << m_space_per_segment << " bytes, space used by each gate: " << get_total_gate_size() << " bytes\n";
}


std::ostream& operator<<(std::ostream& out, const MemStore::RebalancePlan& plan){
    out << "PLAN " << plan.m_action << ", window: [" << plan.m_window_start << ", " << plan.m_window_end << ")";
    return out;
}

std::ostream& operator<<(std::ostream& out, const MemStore::RebalanceAction& action){
    switch(action){
    case MemStore::RebalanceAction::SPREAD:
        out << "SPREAD"; break;
    case MemStore::RebalanceAction::SPLIT:
        out << "SPLIT"; break;
    case MemStore::RebalanceAction::MERGE:
        out << "MERGE"; break;
    default:
        out << "UNKNOWN (" << (int) action << ")";
    }
    return out;
}


} // namespace


