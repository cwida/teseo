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

#include "teseo/memstore/direct_pointer.hpp"

#include <cassert>
#include <iostream>
#include <limits>
#include <sstream>

#include "teseo/memstore/context.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/util/compiler.hpp"
#include "teseo/util/error.hpp"

using namespace std;

namespace teseo::memstore {

DirectPointer::DirectPointer() : m_leaf(nullptr), m_segment(0), m_filepos(0) {

}

DirectPointer::DirectPointer(const DirectPointer& ptr) : m_leaf(ptr.m_leaf), m_segment(ptr.m_segment), m_filepos(ptr.m_filepos) {
    // this is the default basically
}

DirectPointer::DirectPointer(const Context& context) : m_leaf(context.m_leaf), m_segment(0), m_filepos(0){
    if(context.m_leaf != nullptr && context.m_segment != nullptr){
        // version +1 because this is supposed to be set by a writer before leaving a segment
        set_segment(context.segment_id(), context.m_segment->get_version() +1 );
    }
}

DirectPointer::DirectPointer(const Context& context, uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr) : DirectPointer(context) {
    set_filepos(pos_vertex, pos_edge, pos_backptr);
}

DirectPointer& DirectPointer::operator=(const DirectPointer& ptr){
    if(this != &ptr){
        // follow this order to unset the components so that concurrent readers can still detect an invalid (corrupt) pointer
        unset_filepos();
        util::compiler_barrier();
        unset_segment();
        util::compiler_barrier();

        // now from the other way round, first the leaf, then the segment, finally the filepos
        m_leaf = ptr.m_leaf;
        util::compiler_barrier();
        m_segment = ptr.m_segment;
        util::compiler_barrier();
        m_filepos = ptr.m_filepos;
    }

    return *this;
}

void DirectPointer::restore_context(Context* context) noexcept{
    assert(context != nullptr);
    context->m_leaf = leaf();
    context->m_segment = segment();
}

void DirectPointer::set_context(const Context& context) noexcept {
    m_leaf = context.m_leaf;
    set_segment(context.segment_id(), context.m_segment->get_version() + 0 /* this method is used by readers */ );
}

void DirectPointer::set_leaf(Leaf* leaf) noexcept {
    m_leaf = leaf;
}

void DirectPointer::unset_leaf() noexcept {
    m_leaf = nullptr;
}

uint64_t DirectPointer::get_segment_version() const noexcept {
    return m_segment & MASK_SEGMENT_VERSION;
}

uint64_t DirectPointer::get_segment_id() const noexcept {
    return m_segment & MASK_SEGMENT_OFFSET >> __builtin_ctzl(MASK_SEGMENT_OFFSET);
}

Segment* DirectPointer::segment() const noexcept {
    if(leaf() != nullptr){
        return leaf()->get_segment(get_segment_id());
    } else {
        return nullptr;
    }
}

void DirectPointer::set_segment(uint64_t offset, uint64_t version) noexcept {
    assert(offset <= (uint64_t) numeric_limits<uint16_t>::max() && "Offset overflow");
    assert(version <= (MASK_SEGMENT_VERSION +1) && "Version overflow");
    m_segment = (offset << __builtin_ctzl(MASK_SEGMENT_OFFSET)) | (version & MASK_SEGMENT_VERSION);
}

void DirectPointer::unset_segment() noexcept {
    m_segment = 0;
}

void DirectPointer::get_filepos(uint64_t* out_pos_vertex, uint64_t* out_pos_edge, uint64_t* out_pos_backptr) const{
    assert(out_pos_vertex != nullptr && "reference to out_pos_vertex null");
    assert(out_pos_edge != nullptr && "reference to out_pos_edge null");
    assert(out_pos_backptr != nullptr && "reference to out_pos_backptr null");

    uint64_t filepos = m_filepos;
    if((filepos & FLAG_HAS_FILEPOS) == 0){ RAISE(InternalError, "filepos not set"); }
    *out_pos_vertex = (filepos & MASK_FILEPOS_VERTEX) >> __builtin_ctzl(MASK_FILEPOS_VERTEX); // 48
    *out_pos_edge = filepos & MASK_FILEPOS_EDGE >> __builtin_ctzl(MASK_FILEPOS_EDGE); // 32
    *out_pos_backptr = filepos & MASK_FILEPOS_BACKPTR >> __builtin_ctzl(MASK_FILEPOS_BACKPTR); // 16
}

void DirectPointer::set_filepos(uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr) noexcept {
    assert(pos_vertex <= numeric_limits<uint16_t>::max() && "Overflow (pos_vertex)");
    assert(pos_edge <= numeric_limits<uint16_t>::max() && "Overflow (pos_edge)");
    assert(pos_backptr <= numeric_limits<uint16_t>::max() && "Overflow (pos_backptr)");

    uint64_t word = pos_vertex << __builtin_ctzl(MASK_FILEPOS_VERTEX) |
                    pos_edge << __builtin_ctzl(MASK_FILEPOS_EDGE) |
                    pos_backptr << __builtin_ctzl(MASK_FILEPOS_BACKPTR) |
                    FLAG_HAS_FILEPOS;
    m_filepos = word; // atomic update
}

void DirectPointer::unset_filepos() noexcept {
    m_filepos = 0;
}

void DirectPointer::unset() noexcept {
    unset_filepos();
    util::compiler_barrier();
    unset_segment();
    util::compiler_barrier();
    unset_leaf();
}

void DirectPointer::set_latch(bool value) noexcept {
    set_flag(m_filepos, FLAG_LATCH_HELD, value);
}

string DirectPointer::to_string() const {
    stringstream ss;
    ss << "leaf: " << m_leaf << ", ";
    ss << "segment offset: " << get_segment_id();
    if(m_leaf != nullptr){
        ss << "(" << m_leaf->get_segment(get_segment_id()) << ")";
    }
    ss << ", ";
    ss << "segment version: " << get_segment_version();
    if(m_leaf != nullptr){
        Segment* segment = m_leaf->get_segment(get_segment_id());
        ss << " [match: ";
        if(segment->get_version() == get_segment_version()){
            ss << "yes";
        } else {
            ss << "no, current: " << segment->get_version();
        }
        ss << "]";
    }
    ss << "filepos: ";
    if(has_filepos()){
        ss << "<not set>";
    } else {
        uint64_t pos_vertex, pos_edge, pos_backptr;
        get_filepos(&pos_vertex, &pos_edge, &pos_backptr);
        ss << "{vertex: " << pos_vertex << ", edge: " << pos_edge << ", backptr: " << pos_backptr << "}";
    }
    ss << ", flags: ";
    bool has_flags = false;
    if(has_filepos()){
        if(has_flags){ ss << ", "; } else { has_flags = true; }
        ss << "HAS_FILEPOS";
    }
    if(has_latch()){
        if(has_flags){ ss << ", "; } else { has_flags = true; }
        ss << "HAS_LATCH";
    }
    if(!has_flags){
        ss << "n/a";
    }

    return ss.str();
}

void DirectPointer::dump() const {
    cout << "Direct Pointer: " << to_string() << "\n";
}

ostream& operator<<(ostream& out, const DirectPointer& ptr){
    out << ptr.to_string();
    return out;
}

} // namespace

