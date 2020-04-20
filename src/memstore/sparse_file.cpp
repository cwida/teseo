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
#include <teseo/memstore/data_item.hpp>
#include "teseo/memstore/sparse_file.hpp"

#include <cassert>
#include <cinttypes>
#include <cstring>

#include "teseo/memstore/context.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/debug.hpp"

using namespace std;

#define DEBUG

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

SparseFile::SparseFile() {
    reset();
}

void SparseFile::reset(){
    m_versions1_start = 0;
    m_empty1_start = 0;
    m_empty2_start = max_num_qwords();
    m_versions2_start = max_num_qwords();
}

void SparseFile::clear_versions(){
    clear_versions0<true>();
    m_empty1_start = m_versions1_start;
    clear_versions0<false>();
    m_empty2_start = m_versions2_start;
}

template<bool is_lhs>
void SparseFile::clear_versions0(){
    if(is_dirty(is_lhs)){
        Version* v_pos = (Version*) get_versions_start(is_lhs);
        Version* v_end = (Version*) get_versions_end(is_lhs);
        while(v_pos != v_end){
            transaction::Undo::clear(v_pos->get_undo());
            v_pos->reset();
            v_pos++;
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

Key SparseFile::get_minimum() const {
    return get_minimum(/* is lhs ? */ !is_lhs_empty());
}

Key SparseFile::get_minimum(bool is_lhs) const {
    if(is_empty(is_lhs)) return KEY_MIN;

    const uint64_t* __restrict content = get_content_start(is_lhs);

    const Vertex* vertex = get_vertex(content);
    if(vertex->m_first){ // first vertex entry in the edge list
        return Key { vertex->m_vertex_id };
    } else { // as this is not the first vertex in the chain, then it must contain some edges
        assert(vertex->m_count > 0);
        const Edge* edge = get_edge(content + OFFSET_ELEMENT);
        return Key { vertex->m_vertex_id, edge->m_destination };
    }
}

Key SparseFile::get_pivot() const {
    if(is_rhs_empty()) return KEY_MAX;

    const uint64_t* __restrict content = get_rhs_content_start();
    const Vertex* vertex = get_vertex(content);
    if(vertex->m_first){ // first vertex entry in the edge list
        return Key { vertex->m_vertex_id };
    } else { // as this is not the first vertex in the chain, then it must contain some edges
        assert(vertex->m_count > 0);
        const Edge* edge = get_edge(content + OFFSET_ELEMENT);
        return Key { vertex->m_vertex_id, edge->m_destination };
    }
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/

bool SparseFile::update(Context& context, const Update& update, bool has_source_vertex){
    // FIXME: asserts at the segment level
//    assert(segment_id < get_num_segments_per_chunk() && "Invalid segment_id");
//    assert(Key(update.m_source, update.m_destination) >= gate->m_fence_low_key);
//    assert(Key(update.m_source, update.m_destination) < gate->m_fence_high_key);


    bool is_lhs = update.key() < get_pivot();

    if(update.is_vertex()){
        return update_vertex(context, update, is_lhs);
    } else {
        return update_edge(context, update, is_lhs, has_source_vertex);
    }
}

bool SparseFile::update_vertex(Context& context, const Update& update, bool is_lhs) {
    profiler::ScopedTimer profiler { profiler::SF_UPDATE_VERTEX };

    COUT_DEBUG("context: " << *context << ", update: " << update << ", is_lhs: " << is_lhs);
    const uint64_t vertex_id = update.source();

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // first, find the position in the content area where to insert the new vertex
    uint64_t v_backptr = 0;
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    bool c_found = false;
    bool stop = false;
    while(c_index < c_length && !stop){
        Vertex* vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < vertex_id){
            c_index += OFFSET_ELEMENT + vertex->m_count * OFFSET_ELEMENT; // skip the edges altogether
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
        Version* version = get_version(v_start + v_index);
        uint64_t backptr = version->get_backptr();
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            v_found = c_found && (backptr == v_backptr);
            stop = backptr >= v_backptr;
        }
    }


    // FIXME: move the exception messages to the interface
//    // three, consistency checks
//    assert((!c_found || get_vertex(c_start + c_index)->m_first == 1) && "This is not the first vertex in the chain");
//    if(v_found){
//        SegmentVersion* version = get_version(v_start + v_index);
//        if(!transaction->can_write(get_undo(version))){
//            RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the vertex ID " << I2E(vertex_id) << " is currently locked by another transaction. Restart this transaction to alter this object");
//        } else if( is_insert(update) && is_insert(version) ){
//            RAISE_EXCEPTION(LogicalError, "The vertex ID " << I2E(vertex_id) << " already exists");
//        } else if( is_remove(update) && is_remove(version) ){
//            RAISE_EXCEPTION(LogicalError, "The vertex ID " << I2E(vertex_id) << " does not exist");
//        }
//    } else if(c_found && is_insert(update)){ // the static vertex exists
//        RAISE_EXCEPTION(LogicalError, "The vertex ID " << I2E(vertex_id) << " already exists");
//    } else if(!c_found && is_remove(update)){ // the static vertex doesn't exist
//        RAISE_EXCEPTION(LogicalError, "The vertex ID " << I2E(vertex_id) << " does not exist");
//    }

    // three, consistency checks
    assert((!c_found || get_vertex(c_start + c_index)->m_first == 1) && "This is not the first vertex in the chain");
    if(v_found){
        Version* version = get_version(v_start + v_index);
        if(!context.m_transaction->can_write(version->get_undo())){
            throw Error{ Key(vertex_id), Error::EdgeLocked };
        } else if( update.is_insert() && version->is_insert() ){
            throw Error{ Key(vertex_id), Error::VertexAlreadyExists };
        } else if( update.is_remove() && version->is_remove() ){
            throw Error{ Key(vertex_id), Error::VertexDoesNotExist };
        }
    } else if(c_found && update.is_insert()){ // the static vertex exists
        throw Error{ Key(vertex_id), Error::VertexAlreadyExists };
    } else if(!c_found && update.is_remove()){ // the static vertex doesn't exist
        throw Error{ Key(vertex_id), Error::VertexDoesNotExist };
    }


    // four, check we have enough space to add the necessary entries
    int64_t c_shift = (!c_found) * OFFSET_ELEMENT;
    int64_t v_shift = (!v_found) * OFFSET_VERSION + c_shift;
    if((int64_t) free_space() < v_shift) return false;

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

                m_empty1_start += v_shift;

            } else { // shift backwards
                v_index -= OFFSET_VERSION; // because we're shifting backwards

                for(int64_t i = 0; i <= v_index; i++){
                    v_start[i - 1] = v_start[i];
                }

                m_empty2_start -= v_shift;
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
                v_index -= OFFSET_VERSION; // we're shifting backwards

                // again, the versions first
                for(int64_t i = 0; i <= v_index; i++){
                    v_start[i - v_shift] = v_start[i];
                    // do not change the back pointer
                }
                for(int64_t i = v_index +1; i < v_length; i++){
                    v_start[i - c_shift] = v_start[i];
                    get_version(v_start + i - c_shift)->m_backptr++;
                }
                v_index -= c_shift;

                // now the content
                memmove(c_start - c_shift, c_start, c_index * sizeof(uint64_t));
                c_index -= OFFSET_ELEMENT; // c_index is the position of the previous node
            }

            Vertex* vertex = get_vertex(c_start + c_index);
            vertex->m_vertex_id = vertex_id;
            vertex->m_first = 1;
            vertex->m_lock = 0;
            vertex->m_count = 0;
        }

        // update the pointers in the segment metadata
        if(is_lhs){
            m_versions1_start += c_shift;
            m_empty1_start += v_shift;
        } else {
            m_empty2_start -= v_shift;
            m_versions2_start -= c_shift;
        }

        get_version(v_start + v_index)->reset();
    } else {
        get_version(v_start + v_index)->prune_on_write();
    }

    // fifth, update the record's version with this change
    Version* version = get_version(v_start + v_index);
    version->set_type(update);
    version->set_backptr(v_backptr);
    version->set_undo(context.m_transaction->mark_last_undo(version->get_undo()));
//    set_undo(version, transaction->add_undo(this, get_undo(version), update));
//    flip_undo(version); // insert -> remove, remove -> insert

    // done
    return true;
}


bool SparseFile::update_edge(memstore::Context& context, const Update& update, bool is_lhs, bool has_source_vertex)  {
    profiler::ScopedTimer profiler { profiler::SF_UPDATE_EDGE };
    COUT_DEBUG("context: " << *context << ", update: " << update << ", is_lhs: " << is_lhs << ", has_source_vertex: " << has_source_vertex);

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // first, find the position in the content area where to insert the new vertex
    uint64_t v_backptr = 0;
    uint64_t v_backptr_csve = 0;
    int64_t c_index_vertex = 0;
    int64_t c_index_edge = 0;
    int64_t c_length = c_end - c_start;
    bool vertex_found = false;
    bool edge_found = false;
    bool stop = false;
    while(c_index_vertex < c_length && !stop){
        Vertex* vertex = get_vertex(c_start + c_index_vertex);
        if(vertex->m_vertex_id < update.source()){
            c_index_vertex += OFFSET_ELEMENT + vertex->m_count * OFFSET_ELEMENT; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else if(vertex->m_vertex_id == update.source()){
            vertex_found = true;
            v_backptr_csve = v_backptr;

            c_index_edge = c_index_vertex + OFFSET_ELEMENT;
            v_backptr++;

            int64_t e_length = c_index_edge + vertex->m_count * OFFSET_ELEMENT;
            while(c_index_edge < e_length && !stop){
                Edge* edge = get_edge(c_start + c_index_edge);
                if(edge->m_destination < update.destination()){
                    c_index_edge += OFFSET_ELEMENT;
                    v_backptr++;
                } else { // edge->m_destination >= update.m_destination
                    edge_found = edge->m_destination == update.destination();
                    stop = true;
                }
            }

            stop = true;
        } else { // vertex->m_vertex_id > update.m_source
            c_index_edge = c_index_vertex;
            stop = true;
        }
    }

    // in case of a deletion, we always need to find the record in the content area
    if(!edge_found && update.is_remove()){ throw Error{ update.key(), Error::EdgeDoesNotExist }; };

    // in case we didn't find the source vertex attached
    if(c_index_edge < c_index_vertex){
        assert(vertex_found == false);
        c_index_edge = c_index_vertex;
    }

    // second, find the position in the versions area
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    bool version_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        Version* version = get_version(v_start + v_index);
        uint64_t backptr = version->get_backptr();
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            version_found = edge_found && backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    // we are not sure whether the source vertex exists
    if(!has_source_vertex) {
        if(vertex_found){
            if( !is_source_visible(context, get_vertex(c_start + c_index_vertex), v_start, v_length, v_backptr_csve) ){
                throw NotSureIfItHasSourceVertex{};
            }
        } else if (c_index_edge > 0){
            // at this point the source vertex s can exist only iff it is stored in the previous segment. But if the edge
            // to insert is going to be set at a position greater than zero, it means that there is some other item already
            // preceding s in this segment
            throw Error{ Key(update.source()), Error::VertexDoesNotExist };
        }
    }

    // third, consistency checks
    // FIXME the error message in the client
//    if(version_found){
//        Version* version = get_version(v_start + v_index);
//
//        if(!transaction->can_write(get_undo(version))){
//            RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the edge " << I2E(update.m_source) << " -> " <<
//                    I2E(update.m_destination) << " is currently locked by another transaction. Restart this transaction to alter this object");
//        } else if( is_insert(update) && is_insert(version) ){
//            RAISE_EXCEPTION(LogicalError, "The edge " << I2E(update.m_source) << " -> " << I2E(update.m_destination) << " already exists");
//        } else if( is_remove(update) && is_remove(version) ){
//            RAISE_EXCEPTION(LogicalError, "The edge " << I2E(update.m_source) << " -> " << I2E(update.m_destination) << " does not exist");
//        }
//    } else if(edge_found && is_insert(update)) {
//        RAISE_EXCEPTION(LogicalError, "The edge " << I2E(update.m_source) << " -> " << I2E(update.m_destination) << " already exists");
//    } else if(vertex_found && is_insert(update) && get_vertex(c_start + c_index_vertex)->m_lock == 1){
//        RAISE_EXCEPTION(TransactionConflict, "Conflict detected, phantom write detected for the vertex " << I2E(update.m_source));
//    }

    // third, consistency checks
    if(version_found){
        Version* version = get_version(v_start + v_index);

        if(!context.m_transaction->can_write(version->get_undo())){
            throw Error { update.key(), Error::EdgeLocked };
        } else if( update.is_insert() && version->is_insert() ){
            throw Error { update.key(), Error::EdgeAlreadyExists };
        } else if( update.is_remove() && version->is_remove() ){
            throw Error { update.key(), Error::EdgeDoesNotExist };
        }
    } else if(edge_found && update.is_insert()) {
        throw Error { update.key(), Error::EdgeAlreadyExists };
    } else if(vertex_found && update.is_insert() && get_vertex(c_start + c_index_vertex)->m_lock == 1){
        throw Error { update.source(), Error::VertexPhantomWrite };
    }

    // fourth, check we have enough space to add the necessary entries
    int64_t c_shift = (!vertex_found) * OFFSET_ELEMENT + (!edge_found) * OFFSET_ELEMENT;
    int64_t v_shift = (!version_found) * OFFSET_VERSION + c_shift;
    if((int64_t) free_space() < v_shift) return false;

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
            } else { // shift backwards
                v_index -= OFFSET_VERSION; // because we're shifting backwards

                for(int64_t i = 0; i <= v_index; i++){
                    v_start[i - 1] = v_start[i];
                }
            }

        } else {
            assert(update.is_insert() && "With a remove, the edge in the content area always already exists");

            // we need to shift both the content and the versions
            int16_t backptr_shift = /* for the edge */ 1 + /* for the dummy vertex */ (vertex_found == false);

            if(is_lhs){
                // let's start with the versions
                for(int64_t i = v_length -1; i >= v_index; i--){
                    v_start[i + v_shift] = v_start[i];
                    get_version(v_start + i + v_shift)->m_backptr += backptr_shift;
                }

                // now the content
                int64_t shift_length = (v_start - c_start) + v_index - c_index_edge;
                memmove(c_start + c_index_edge + c_shift, c_start + c_index_edge, shift_length * sizeof(uint64_t));

                v_index += c_shift;
            } else { // right hand side
                v_index -= OFFSET_VERSION; // we're shifting backwards

                // again, the versions first
                for(int64_t i = 0; i <= v_index; i++){
                    v_start[i - v_shift] = v_start[i];
                    // do not change the back pointer
                }
                for(int64_t i = v_index +1; i < v_length; i++){
                    v_start[i - c_shift] = v_start[i];
                    get_version(v_start + i - c_shift)->m_backptr += backptr_shift;
                }
                v_index -= c_shift;

                // now the content
                memmove(c_start - c_shift, c_start, c_index_edge * sizeof(uint64_t));
                c_index_vertex -= OFFSET_ELEMENT; // we shifted it back by the amount to store an edge
                c_index_edge -= OFFSET_ELEMENT; // move back to the position of the previous item
            }

            // update the source vertex attached to this edge
            if(!vertex_found){
                c_index_vertex = c_index_edge;
                c_index_edge = c_index_vertex + OFFSET_ELEMENT;
                v_backptr++; // skip the dummy vertex

                Vertex* vertex = get_vertex(c_start + c_index_vertex);
                vertex->m_vertex_id = update.source();
                vertex->m_first = 0;
                vertex->m_lock = 0;
                vertex->m_count = 1; // the edge just inserted
            } else {
                Vertex* vertex = get_vertex(c_start + c_index_vertex);
                vertex->m_count++;
            }
        }

        // update the pointers in the segment metadata
        if(is_lhs){
            m_versions1_start += c_shift;
            m_empty1_start += v_shift;
        } else {
            m_empty2_start -= v_shift;
            m_versions2_start -= c_shift;
        }

        get_version(v_start + v_index)->reset();
    } else {
        get_version(v_start + v_index)->prune_on_write();
    }

    // sixth, update the record's version with this change
    Version* version = get_version(v_start + v_index);
    version->set_type(update);
    version->set_backptr(v_backptr);
    transaction::Undo* undo = context.m_transaction->mark_last_undo(version->get_undo());
    version->set_undo(undo);

    // seventh, update the content part of the record
    Edge* edge = get_edge(c_start + c_index_edge);
    edge->m_destination = update.destination();
    reinterpret_cast<Update*>(undo->payload())->set_weight(edge->m_weight);
    edge->m_weight = update.weight();

    // done
    return true;
}

bool SparseFile::is_source_visible(Context& context, const Vertex* vertex, const uint64_t* v_start, uint64_t v_length, uint64_t v_backptr) const {
    profiler::ScopedTimer profiler { profiler::SF_IS_SOURCE_VISIBLE };

    uint64_t v_index = 0;
    while(v_index < v_length && get_version(v_start + v_index)->get_backptr() < v_backptr){ v_index += OFFSET_VERSION; }

    if(vertex->m_first == 1){
        if(v_index == v_length || get_version(v_start + v_index)->get_backptr() > v_backptr){
            return true; // we have an unversioned first vertex
        } else {
            Update update = Update::read_delta(context, vertex, nullptr, get_version(v_start + v_index));
            return update.is_insert();
        }
    } else { // we need to deal with a dummy vertex
        v_backptr++;

        assert(vertex->m_count > 0 && "Dummy vertices must have at least one edge attached");
        uint64_t i = 0;
        uint64_t num_edges = vertex->m_count;
        const Edge* edge = reinterpret_cast<const Edge*>(vertex +1);

        while(i < num_edges){
            if(v_index == v_length) // there are no more versions left
                return true;
            const Version* version = get_version(v_start + v_index);
            if(version->get_backptr() > v_backptr)  // this edge does not have a version attached
                return true;

            Update update = Update::read_delta(context, vertex, edge, version);
            if(update.is_insert())
                return true;

            // next iteration
            i++;
            edge++;
            v_index += OFFSET_VERSION;
            v_backptr++;
        }

        // no edges are visible
        return false;
    }
}


} // namespace


