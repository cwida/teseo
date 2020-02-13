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

#include "storage.hpp"

#include <cassert>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>

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
 *   Key                                                                     *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "Storage::Key"

// Key, convenience class
Storage::Key::Key() : Key(numeric_limits<uint64_t>::max(), numeric_limits<uint64_t>::max()) { }
Storage::Key::Key(uint64_t vertex_id) : Key(vertex_id, 0) { }
Storage::Key::Key(uint64_t src, uint64_t dst) : m_source(src), m_destination(dst) { }
uint64_t Storage::Key::get_source() const { return m_source; }
uint64_t Storage::Key::get_destination() const { return m_destination; }
void Storage::Key::set(uint64_t vertex_id) { m_source = vertex_id; m_destination = 0; }
void Storage::Key::set(uint64_t source, uint64_t destination) { m_source = source; m_destination = destination; }
bool Storage::Key::operator==(const Key& other){ return get_source() == other.get_source() && get_destination() == other.get_destination(); }
bool Storage::Key::operator!=(const Key& other){ return !(*this == other); }
bool Storage::Key::operator<(const Key& other){ return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() < other.get_destination()); }
bool Storage::Key::operator<=(const Key& other){ return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() <= other.get_destination()); }
bool Storage::Key::operator>(const Key& other){ return !(*this <= other); }
bool Storage::Key::operator>=(const Key& other){ return !(*this < other); }
Storage::Key Storage::Key::min(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::min(), numeric_limits< decltype(Key{}.get_destination()) >::min()); }
Storage::Key Storage::Key::max(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::max(), numeric_limits< decltype(Key{}.get_destination()) >::max()); }
std::ostream& operator<<(std::ostream& out, const Storage::Key& key){
    out << key.get_source() << " -> " << key.get_destination();
    return out;
}

/*****************************************************************************
 *                                                                           *
 *   Static Index                                                            *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME
#define COUT_CLASS_NAME "Storage::StaticIndex"

Storage::StaticIndex::StaticIndex(uint64_t node_size, uint64_t num_segments) :
        m_node_size(node_size), m_height(0), m_capacity(0), m_keys(reinterpret_cast<Key*>((reinterpret_cast<uint8_t*>(this) + 64))) {
    static_assert(sizeof(this) <= 64, "Expecting to store the keys after the first 64 bytes of where the class is allocated");
    assert(reinterpret_cast<uint64_t>(this) % 64 == 0 && "The instance is not aligned to a 64 byte boundary");
    assert(reinterpret_cast<uint64_t>(m_keys) % 64 == 0 && "The instance is not aligned to a 64 byte boundary");
    if(node_size > (uint64_t) numeric_limits<uint16_t>::max()){ throw std::invalid_argument("Invalid node size: too big"); }

    rebuild(num_segments);
}

Storage::StaticIndex::StaticIndex::~StaticIndex(){
    // free(m_keys); m_keys = nullptr; // Not anymore, the space for the keys is now embedded in the B+ leaf
}

int64_t Storage::StaticIndex::node_size() const noexcept {
    // cast to int64_t
    return m_node_size;
}

void Storage::StaticIndex::rebuild(uint64_t N){
    if(N == 0) throw std::invalid_argument("Invalid number of keys: 0");
    int height = ceil( log2(N) / log2(node_size()) );
    if(height > m_rightmost_sz){ throw std::invalid_argument("Invalid number of keys/segments: too big"); }
    uint64_t tree_sz = pow(node_size(), height) -1; // don't store the minimum, segment 0

    // Not anymore, the space for the keys is statically allocated in advance.
//    if(height != m_height){
//        free(m_keys); m_keys = nullptr;
//        int rc = posix_memalign((void**) &m_keys, /* alignment */ 64,  /* size */ tree_sz * sizeof(int64_t));
//        if(rc != 0) { throw std::bad_alloc(); }
//        m_height = height;
//    }

    m_height = height;
    m_capacity = N;
    COUT_DEBUG("capacity: " << m_capacity << ", height: " << m_height);

    // set the height of all rightmost subtrees
    while(height > 0){
        assert(height > 0);
        uint64_t subtree_sz = pow(node_size(), height -1);
        m_rightmost[height - 1].m_root_sz = (N -1) / subtree_sz;
        assert(m_rightmost[height -1].m_root_sz > 0);
        uint64_t rightmost_subtree_sz = (N -1) % subtree_sz;
        int rightmost_subtree_height = 0;
        if(rightmost_subtree_sz > 0){
            rightmost_subtree_sz += 1; // with B-1 keys we index B entries
            rightmost_subtree_height = ceil(log2(rightmost_subtree_sz) / log2(m_node_size));
        }
        m_rightmost[height -1].m_right_height = rightmost_subtree_height;

        COUT_DEBUG("height: " << height << ", rightmost subtree height: " << rightmost_subtree_height << ", root_sz: " << m_rightmost[height - 1].m_root_sz);

        // next subtree
        N = rightmost_subtree_sz;
        height = rightmost_subtree_height;
    }
}

int Storage::StaticIndex::height() const noexcept {
    return m_height;
}

//size_t StaticIndex::memory_footprint() const {
//    return (pow(node_size(), height()) -1) * sizeof(int64_t);
//}

Storage::Key* Storage::StaticIndex::get_slot(uint64_t segment_id) const {
    COUT_DEBUG("segment_id: " << segment_id);
    assert(segment_id > 0 && "The segment 0 is not explicitly stored");
    assert(segment_id < static_cast<uint64_t>(m_capacity) && "Invalid slot");

    Key* __restrict base = m_keys;
    int64_t offset = segment_id;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        int64_t subtree_id = offset / subtree_sz;
        int64_t modulo = offset % subtree_sz;
        COUT_DEBUG("height: " << height << ", base: " << (base - m_keys) << ", subtree_id: " << subtree_id << ", modulo: " << modulo << ", rightmost: " << rightmost);

        if(modulo == 0){ // found, this is an internal node
            assert(subtree_id > 0 && "Otherwise this would have been an internal element on an ancestor");
            return base + subtree_id -1;
        }

        // traverse the children
        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset -= subtree_id * subtree_sz;

        // is this the rightmost subtree ?
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
            COUT_DEBUG("rightmost, height: " << height << ", subtree_sz: " << subtree_sz);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    return base + offset;
}

void Storage::StaticIndex::set_separator_key(uint64_t segment_id, Key key){
    if(segment_id == 0) {
        m_key_minimum = key;
    } else {
        get_slot(segment_id)[0] = key;
    }

    assert(get_separator_key(segment_id) == key);
}

auto Storage::StaticIndex::get_separator_key(uint64_t segment_id) const -> Key {
    if(segment_id == 0)
        return m_key_minimum;
    else
        return get_slot(segment_id)[0];
}

uint64_t Storage::StaticIndex::find(Key key) const noexcept {
    COUT_DEBUG("key: " << key);
    if(key <= m_key_minimum) return 0; // easy!

    Key* __restrict base = m_keys;
    int64_t offset = 0;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        uint64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
        uint64_t subtree_id = 0;
        while(subtree_id < root_sz && base[subtree_id] <= key) subtree_id++;

        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset += subtree_id * subtree_sz;

        COUT_DEBUG("height: " << height << ", base: " << (base - m_keys) << ", subtree_id: " << subtree_id << ", offset: " << offset << ", rightmost: " << rightmost);

        // similar to #get_slot
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
            COUT_DEBUG("rightmost, height: " << height << ", subtree_sz: " << subtree_sz);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    COUT_DEBUG("offset: " << offset);
    return offset;
}

uint64_t Storage::StaticIndex::find_first(Key key) const noexcept {
    if(key < m_key_minimum) return 0; // easy!

    Key* __restrict base = m_keys;
    int64_t offset = 0;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        uint64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
        uint64_t subtree_id = 0;
        while(subtree_id < root_sz && base[subtree_id] < key) subtree_id++;

        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset += subtree_id * subtree_sz;

        // similar to #get_slot
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    return offset;
}

uint64_t Storage::StaticIndex::find_last(Key key) const noexcept {
    if(key < m_key_minimum) return 0; // easy!

    int64_t* __restrict base = m_keys;
    int64_t offset = 0;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        uint64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
        uint64_t subtree_id = root_sz;
        while(subtree_id > 0 && key < base[subtree_id -1]) subtree_id--;

        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset += subtree_id * subtree_sz;

        // similar to #get_slot
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    return offset;
}

auto Storage::StaticIndex::minimum() const noexcept -> Key {
    return m_key_minimum;
}

void Storage::StaticIndex::dump_tabs(std::ostream& out, int depth){
    auto flags = out.flags();
    out << setw((depth-1) * 2 + 5) << setfill(' ') << ' ';
    out.setf(flags);
}

void Storage::StaticIndex::dump_subtree(std::ostream& out, Key* root, int height, bool rightmost, Key fence_min, Key fence_max, bool* integrity_check) const {
    if(height <= 0) return; // base case

    int depth = m_height - height +1;
    int64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
    int64_t subtree_sz = pow(node_size(), height -1);

    // preamble
    auto flags = out.flags();
    if(depth > 1) out << ' ';
    out << setw((depth -1) * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << "offset: " << root - m_keys << ", root size: " << root_sz << ", fence keys (interval): [" << fence_min << ", " << fence_max << "]\n";
    out.setf(flags);

    dump_tabs(out, depth);
    out << "keys: ";
    for(size_t i = 0; i < root_sz; i++){
        if(i > 0) out << ", ";
        out << (i+1) << " => k:" << (i+1) * subtree_sz << ", v:" << root[i];

        if(i == 0 && root[0] < fence_min){
            out << " (ERROR: smaller than the min fence key: " << fence_min << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(i > 0 && root[i] < root[i-1]){
            out << " (ERROR: sorted order not respected: " << root[i-1] << " > " << root[i] << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(i == root_sz -1 && root[i] > fence_max){
            out << " (ERROR: greater than the max fence key: " << fence_max << ")";
            if(integrity_check) *integrity_check = false;
        }

    }
    out << "\n";

    if(height > 1) { // internal node?
        Key* base = root + node_size() -1;

        dump_tabs(out, depth);
        out << "offsets: ";
        for(size_t i = 0; i <= root_sz; i++){
          if(i > 0) out << ", ";
          out << i << ": " << (base + i * (subtree_sz -1)) - m_keys;
        }
        out << "\n";

        // recursively dump the children
        for(size_t i = 0; i < root_sz; i++){
            Key fmin = (i == 0) ? fence_min : root[i-1];
            Key fmax = root[i];

            dump_subtree(out, base + (i* (subtree_sz -1)), height -1, false, fmin, fmax, integrity_check);
        }

        // dump the rightmost subtree
        dump_subtree(out, base + root_sz * (subtree_sz -1), m_rightmost[height -1].m_right_height, rightmost, root[root_sz -1], fence_max, integrity_check);
    }
}

void Storage::StaticIndex::dump(std::ostream& out, bool* integrity_check) const {
    out << "[Index] block size: " << node_size() << ", height: " << height() <<
            ", capacity (number of entries indexed): " << m_capacity << ", minimum: " << minimum() << "\n";

    if(m_capacity > 1)
        dump_subtree(out, m_keys, height(), true, m_key_minimum, Key::max(), integrity_check);
}

void Storage::StaticIndex::dump() const {
    dump(cout);
}

std::ostream& operator<<(std::ostream& out, const Storage::StaticIndex& index){
    index.dump(out);
    return out;
}

} // namespace


