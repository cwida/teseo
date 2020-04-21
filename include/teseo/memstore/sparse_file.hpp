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

#include <cassert>
#include <cinttypes>
#include <ostream>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"

namespace teseo::transaction { class Undo; } // forward decl.

namespace teseo::memstore {

// Forward declarations
class Context;
class Edge;
class Update;
class Version;
class Vertex;

/**
 * A sorted file consisting of a sorted dense area, followed by gaps, followed by another dense area
 */
class SparseFile {
    // Remove all versions from either the LHS or RHS of the sparse file
    template<bool is_lhs> void clear_versions0();

    // Insert or remove a vertex in the sparse file
    bool update_vertex(Context& context, const Update& update, bool is_lhs);

    // Insert or remove an edge in the sparse file
    bool update_edge(Context& context, const Update& update, bool is_lhs, bool has_source_vertex);

    // Check whether there exists any edge in the current segment, with the given vertex as source, being visible by the current transaction
    bool is_source_visible(Context& context, const Vertex* vertex, const uint64_t* versions, uint64_t versions_sz, uint64_t vertex_backptr) const;

    // Helper, dump either the lhs or the rhs to the output stream
    void dump_section(std::ostream& out, bool is_lhs, const Key& fence_key_low, const Key& fence_key_high, bool* integrity_check) const;

    // Helper, dump the given element to the output stream
    static void dump_element(std::ostream& out, uint64_t position, const Vertex* vertex, const Edge* edge, const Version* version, bool* integrity_check);

    // Helper, check the given element is in the interval set by the fence keys
    static void dump_validate_key(std::ostream& out, const Vertex* vertex, const  Edge* edge, const Key& fence_key_low, const Key& fence_key_high, bool* integrity_check);

public:
    uint16_t m_versions1_start; // the offset where the changes for the LHS of the segment start, in qwords
    uint16_t m_versions2_start; // the offset where the changes for the RHS of the segment start, in qwords
    uint16_t m_empty1_start; // the offset where the empty space for the LHS of the segment start, in qwords
    uint16_t m_empty2_start; // the offset where the empty space for the RHS of the segment start, in qwords

    /**
     * Initialise the sparse file
     */
    SparseFile();

    /**
     * Reset (clear) the content of the sparse file
     */
    void reset();

    /**
     * Retrieve the minimum of the sparse file
     */
    Key get_minimum() const;

    /**
     * Retrieve the minimum of the LHS or RHS section
     */
    Key get_minimum(bool is_lhs) const;

    /**
     * Retrieve the pivot, that is the minimum of the RHS side
     */
    Key get_pivot() const;

    /**
     * Attempt to perform the given update
     * @param context the memstore context
     * @param update the update to perform
     * @param has_source_edge, if the update involves the insertion of an edge, it checks whether
     *        the source vertex exists already in the file. Otherwise it's ignored.
     * @return true if the update has been performed, false otherwise as there was not anymore space in the file
     */
    bool update(Context& context, const Update& update, bool has_source_vertex);

    /**
     * Rollback the given update
     */
    void rollback(Context& context, const Update& update, transaction::Undo* next);

    /**
     * Check whether the given key (vertex, edge) exists in the segment and is visible by the current transaction.
     * Assume an optimistic lock has been taken to the context.m_segment.
     * @param context the current context, with the tree traversal memstore -> leaf -> segment
     * @param key the search key
     * @param is_unlocked if true, the search key must be a vertex and its state must be unlocked, to avoid phantom writes.
     */
    bool has_item_optimistic(Context& context, const Key& key, bool is_unlocked) const;

    /**
     * Retrieve the weight associated to the given edge
     */
    double get_weight_optimistic(Context& context, const Key& key) const;

    /**
     * Remove all versions from the sparse file
     */
    void clear_versions();

    /**
     * Retrieve the start/end pointers of the content and versions area
     */
    uint64_t* get_lhs_content_start();
    const uint64_t* get_lhs_content_start() const;
    uint64_t* get_lhs_content_end();
    const uint64_t* get_lhs_content_end() const;
    uint64_t* get_lhs_versions_start();
    const uint64_t* get_lhs_versions_start() const;
    uint64_t* get_lhs_versions_end();
    const uint64_t* get_lhs_versions_end() const;
    uint64_t* get_rhs_content_start();
    const uint64_t* get_rhs_content_start() const;
    uint64_t* get_rhs_content_end();
    const uint64_t* get_rhs_content_end() const;
    uint64_t* get_rhs_versions_start();
    const uint64_t* get_rhs_versions_start() const;
    uint64_t* get_rhs_versions_end();
    const uint64_t* get_rhs_versions_end() const;
    uint64_t* get_content_start(bool is_lhs);
    const uint64_t* get_content_start(bool is_lhs) const;
    uint64_t* get_content_end(bool is_lhs);
    const uint64_t* get_content_end(bool is_lhs) const;
    uint64_t* get_versions_start(bool is_lhs);
    const uint64_t* get_versions_start(bool is_lhs) const;
    uint64_t* get_versions_end(bool is_lhs);
    const uint64_t* get_versions_end(bool is_lhs) const;

    /**
     * Retrieve the amount of free space, in qwords, in the segment
     */
    uint64_t free_space() const;

    /**
     * Retrieve the amount of used space, in qwords, in the segment
     */
    uint64_t used_space() const;

    /**
     * Check whether the segment is empty
     */
    bool is_empty() const;
    bool is_empty(bool is_lhs) const;
    bool is_lhs_empty() const;
    bool is_rhs_empty() const;

    /**
     * Check whether the segment contains any version stored
     */
    bool is_dirty() const;
    bool is_dirty(bool is_lhs) const;

    /**
     * Retrieve the number of qwords each sparse segment contains
     */
    static uint64_t max_num_qwords();

    // Retrieve the vertex/edge/version from the given pointer
    static Vertex* get_vertex(uint64_t* ptr);
    static const Vertex* get_vertex(const uint64_t* ptr);
    static Edge* get_edge(uint64_t* ptr);
    static const Edge* get_edge(const uint64_t* ptr);
    static Version* get_version(uint64_t* ptr);
    static const Version* get_version(const uint64_t* ptr);

    // Dump the segment to stdout, for debugging purposes
    void dump() const;

    // Dump the segment to the given output stream
    void dump_and_validate(std::ostream& out, Context& context, bool* integrity_check) const;


};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
uint64_t* SparseFile::get_lhs_content_start() {
    return reinterpret_cast<uint64_t*>(this + 1);
}

inline
const uint64_t* SparseFile::get_lhs_content_start() const {
    return reinterpret_cast<const uint64_t*>(this + 1);
}

inline
uint64_t* SparseFile::get_lhs_content_end() {
    return get_lhs_versions_start();
}

inline
const uint64_t* SparseFile::get_lhs_content_end() const {
    return get_lhs_versions_start();
}

inline
uint64_t* SparseFile::get_lhs_versions_start() {
    return get_lhs_content_start() + m_versions1_start;
}

inline
const uint64_t* SparseFile::get_lhs_versions_start() const {
    return get_lhs_content_start() + m_versions1_start;
}

inline
uint64_t* SparseFile::get_lhs_versions_end() {
    return get_lhs_content_start() + m_empty1_start;
}

inline
const uint64_t* SparseFile::get_lhs_versions_end() const {
    return get_lhs_content_start() + m_empty1_start;
}

inline
uint64_t* SparseFile::get_rhs_content_start() {
    return get_lhs_content_start() + m_versions2_start;
}

inline
const uint64_t* SparseFile::get_rhs_content_start() const {
    return get_lhs_content_start() + m_versions2_start;
}

inline
uint64_t* SparseFile::get_rhs_content_end() {
    return get_lhs_content_start() + max_num_qwords();
}

inline
const uint64_t* SparseFile::get_rhs_content_end() const {
    return get_lhs_content_start() + max_num_qwords();
}

inline
uint64_t* SparseFile::get_rhs_versions_start() {
    return get_lhs_content_start() + m_empty2_start;
}

inline
const uint64_t* SparseFile::get_rhs_versions_start() const {
    return get_lhs_content_start() + m_empty2_start;
}

inline
uint64_t* SparseFile::get_rhs_versions_end(){
    return get_rhs_content_start();
}

inline
const uint64_t* SparseFile::get_rhs_versions_end() const {
    return get_rhs_content_start();
}

inline
uint64_t* SparseFile::get_content_start(bool is_lhs){
    return is_lhs ? get_lhs_content_start() : get_rhs_content_start();
}

inline
const uint64_t* SparseFile::get_content_start(bool is_lhs) const {
    return is_lhs ? get_lhs_content_start() : get_rhs_content_start();
}

inline
uint64_t* SparseFile::get_content_end(bool is_lhs){
    return is_lhs ? get_lhs_content_end() : get_rhs_content_end();
}

inline
const uint64_t* SparseFile::get_content_end(bool is_lhs) const {
    return is_lhs ? get_lhs_content_end() : get_rhs_content_end();
}

inline
uint64_t* SparseFile::get_versions_start(bool is_lhs) {
    return is_lhs ? get_lhs_versions_start() : get_rhs_versions_start();
}

inline
const uint64_t* SparseFile::get_versions_start(bool is_lhs) const {
    return is_lhs ? get_lhs_versions_start() : get_rhs_versions_start();
}

inline
uint64_t* SparseFile::get_versions_end(bool is_lhs) {
    return is_lhs ? get_lhs_versions_end() : get_rhs_versions_end();
}

inline
const uint64_t* SparseFile::get_versions_end(bool is_lhs) const {
    return is_lhs ? get_lhs_versions_end() : get_rhs_versions_end();
}

inline
uint64_t SparseFile::max_num_qwords(){
    return context::StaticConfiguration::memstore_segment_size -1;
}

inline
uint64_t SparseFile::free_space() const {
    return m_empty2_start - m_empty1_start;
}

inline
uint64_t SparseFile::used_space() const {
    assert(free_space() <= max_num_qwords());
    return max_num_qwords() - free_space();
}

inline
bool SparseFile::is_empty() const {
    return used_space() == 0;
}

inline
bool SparseFile::is_empty(bool is_lhs) const {
    if(is_lhs) {
        return is_lhs_empty();
    } else {
        return is_rhs_empty();
    }
}

inline
bool SparseFile::is_lhs_empty() const {
    return m_empty1_start == 0;
}

inline
bool SparseFile::is_rhs_empty() const {
    return m_empty2_start == max_num_qwords();
}

inline
bool SparseFile::is_dirty() const {
    return is_dirty(true) || is_dirty(false);
}

inline
bool SparseFile::is_dirty(bool is_lhs) const {
    if(is_lhs){
        return m_versions1_start < m_empty1_start;
    } else {
        return m_empty2_start < m_versions2_start;
    }
}

inline
Vertex* SparseFile::get_vertex(uint64_t* ptr){
    return reinterpret_cast<Vertex*>(ptr);
}

inline
const Vertex* SparseFile::get_vertex(const uint64_t* ptr){
    return reinterpret_cast<const Vertex*>(ptr);
}

inline
Edge* SparseFile::get_edge(uint64_t* ptr){
    return reinterpret_cast<Edge*>(ptr);
}

inline
const Edge* SparseFile::get_edge(const uint64_t* ptr) {
    return reinterpret_cast<const Edge*>(ptr);
}

inline
Version* SparseFile::get_version(uint64_t* ptr){
    return reinterpret_cast<Version*>(ptr);
}

inline
const Version* SparseFile::get_version(const uint64_t* ptr){
    return reinterpret_cast<const Version*>(ptr);
}

} // namespace

