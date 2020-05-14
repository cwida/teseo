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
#include <utility>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/key.hpp"

namespace teseo::aux { class PartialResult; } // forward decl.
namespace teseo::rebalance { class ScratchPad; } // forward decl.
namespace teseo::transaction { class Undo; } // forward decl.

namespace teseo::memstore {

// Forward declarations
class Context;
class Edge;
class RemoveVertex;
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

    // Actual implementation of remove_vertex, for either the lhs or rhs of the file
    bool do_remove_vertex(RemoveVertex& instance, bool is_lhs);

    // Helper method for #do_remove_vertex, copy the versions stored in the scratch pad back to the file
    void copy_scratchpad(RemoveVertex& instance, bool is_lhs, int64_t scratchpad_pos, int64_t bookmark);

    // Actual implementation of the method, specialised for the lhs or rhs of the file
    void unlock_removed_vertex(RemoveVertex& instance, bool is_lhs);

    // Check whether there exists any edge in the current segment, with the given vertex as source, being visible by the current transaction
    bool is_source_visible(Context& context, const Vertex* vertex, const uint64_t* versions, uint64_t versions_sz, uint64_t vertex_backptr) const;

    // Actual implementation of the method #load, specialised for the lhs or the rhs section of the file
    void load(rebalance::ScratchPad& scratchpad, bool is_lhs);

    // Overwrite the file attemping to save `target_budget' qwords from the buffer
    void fill(rebalance::ScratchPad& buffer, bool is_lhs, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget);

    // Helper method to save the elements & the versions from the buffer into the file
    void save_elements(rebalance::ScratchPad& buffer, uint64_t pos_src_first_vertex, uint64_t pos_src_start, uint64_t pos_src_end, uint64_t* destination);
    void save_versions(rebalance::ScratchPad& buffer, uint64_t pos_src_start, uint64_t pos_src_end, uint64_t v_backptr_start, uint64_t* destination);

    // Retrieve the number of edges attached to the given vertex
    template<bool is_optimistic>
    std::pair</* continue ? */ bool, /* degree */ uint64_t> get_degree(Context& context, bool is_lhs, const Key& key, bool& has_found_vertex) const;

    // Scan implementation
    template<bool is_optimistic, typename Callback>
    bool scan_impl(Context& context, bool is_lhs, Key& next, Callback&& callback) const;

    // Process the partial results for the aux vector
    template<bool check_end_interval>
    bool aux_partial_result_impl(Context& context, bool is_lhs, const Key& next, aux::PartialResult* partial_result) const;

    // Remove non accessible undos from the file
    void prune_versions(bool is_lhs);

    // Remove non accessible elements from the file
    std::pair</* c_shift */ int64_t, /* v_shift */ int64_t> prune_elements(bool is_lhs);

    // Helper, shift an element in the file by the given amount
    void shift_element_by(void* element, int64_t amount);

    // Helper, shift a version in the file by the given amount
    void shift_version_by(void* version, int64_t amount);

    // Helper, dump either the lhs or the rhs to the output stream
    void dump_section(std::ostream& out, bool is_lhs, const Key& fence_key_low, const Key& fence_key_high, bool* integrity_check) const;

    // Helper, dump the given element to the output stream
    static void dump_element(std::ostream& out, uint64_t position, const Vertex* vertex, const Edge* edge, const Version* version, bool* integrity_check);

    // Helper, check the given element is in the interval set by the fence keys
    static void dump_validate_key(std::ostream& out, const Vertex* vertex, const  Edge* edge, const Key& fence_key_low, const Key& fence_key_high, bool* integrity_check);

    // Helper, dump the content of the section after a fill, for debugging purposes
    void dump_after_save(bool is_lhs) const;

    // Actual implementation of the #validate() method
    void do_validate(Context& context) const;
    void do_validate_impl(Context& context, bool is_lhs, const Key& fence_key_low, const Key& fence_key_high) const;

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
    Key get_pivot(Context& context) const;
    template<bool is_optimistic>
    Key get_pivot_impl(Context& context) const;

    /**
     * Retrieve the degree (number of edges attached) of the given vertex.
     * The method works for both optimistic and locked reader.
     */
    uint64_t get_degree(Context& context, const Key& key, bool& out_has_found_vertex) const;

    /**
     * Retrieve all elements in the segment such that are equal or greater than `key'.
     * The expected signature of the callback is bool fn(uint64_t source, uint64_t destination, double weight);
     * @return true if the scan should propagate to the next segment
     */
    template<typename Callback>
    bool scan(Context& context, Key& next, Callback&& callback);

    /**
     * Attempt to perform the given update
     * @param context the memstore context
     * @param update the update to perform
     * @param has_source_edge, if the update involves the insertion of an edge, it checks whether
     *        the source vertex exists already in the file. Otherwise it's ignored.
     * @return a pair:
     *        - the first element is true if the update has been performed, false otherwise as there was not anymore space in the file
     *        - the second element is the amount of space altered
     */
    bool update(Context& context, const Update& update, bool has_source_vertex);

    /**
     * Rollback the given update
     */
    void rollback(Context& context, const Update& update, transaction::Undo* next);

    /**
     * Remove the vertex and all of its attached outgoing edges
     * @return true if the operation was completed, false if there was not anymore space in the file
     */
    bool remove_vertex(RemoveVertex& instance);

    /**
     * Unlock the real & dummy vertices in the file
     */
    void unlock_removed_vertex(RemoveVertex& instance);

    /**
     * Load all elements stored in the file into the given buffer
     */
    void load(rebalance::ScratchPad& buffer);

    /**
     * Save `budget' qwords from the buffer into the file
     */
    void save(rebalance::ScratchPad& buffer, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget_achieved);

    /**
     * Remove inaccessible undo records in the history and compact the file
     */
    void prune();

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
     * Process the intermediate to create the aux vector
     */
    bool aux_partial_result(Context& context, const Key& next, bool check_end_interval, aux::PartialResult* partial_result) const;

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
     * The the total number of elements, including dummy vertices, in the file
     */
    uint64_t cardinality() const;

    /**
     * Retrieve the amount of free space, in qwords, in the file
     */
    uint64_t free_space() const;

    /**
     * Retrieve the amount of used space, in qwords, in the file
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

    // Validate the content of the file, for debugging purposes
    void validate(Context& context) const; // trampoline to do_validate() when NDEBUG is not defined
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/


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
void SparseFile::validate(Context& context) const {
#if !defined(NDEBUG)
    do_validate(context);
#endif
}

} // namespace

