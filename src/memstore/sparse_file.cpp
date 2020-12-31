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
#include "teseo/memstore/sparse_file.hpp"

#include <cassert>
#include <cinttypes>
#include <cstring>
#include <iomanip>
#include <iostream>

#include "teseo/aux/partial_result.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/direct_pointer.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/remove_vertex.hpp"
#include "teseo/memstore/vertex_table.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

// [DEBUG] Set this macro to validate the content of the file after a #prune operation invoked by a background thread (merger)
//#define VALIDATE_PRUNE

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Debug macros                                                            *
 *                                                                           *
 *****************************************************************************/
// redef the macro DEBUG_PRUNE( x ) as ( x )
#if !defined(VALIDATE_PRUNE)
#define DEBUG_PRUNE( stuff )
#elif defined(VALIDATE_PRUNE) && defined(NDEBUG)
#warning "[Teseo] Setting VALIDATE_PRUNE ignored because the macro NDEBUG is defined"
#define DEBUG_PRUNE( stuff )
#else // validate_prune is defined, ndebug is not defined
#define DEBUG_PRUNE( stuff ) stuff
#endif

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
    update_pivot();
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
uint64_t* SparseFile::get_lhs_content_start() {
    if(context::StaticConfiguration::memstore_duplicate_pivot){
        return reinterpret_cast<uint64_t*>(this + 1) +2; // the first two qwords are the cached pivot
    } else {
        return reinterpret_cast<uint64_t*>(this + 1);
    }
}

const uint64_t* SparseFile::get_lhs_content_start() const {
    if(context::StaticConfiguration::memstore_duplicate_pivot){
        return reinterpret_cast<const uint64_t*>(this + 1) +2; // the first two qwords are the cached pivot
    } else {
        return reinterpret_cast<const uint64_t*>(this + 1);
    }
}

uint64_t* SparseFile::get_lhs_content_end() {
    return get_lhs_versions_start();
}

const uint64_t* SparseFile::get_lhs_content_end() const {
    return get_lhs_versions_start();
}

uint64_t* SparseFile::get_lhs_versions_start() {
    return get_lhs_content_start() + m_versions1_start;
}

const uint64_t* SparseFile::get_lhs_versions_start() const {
    return get_lhs_content_start() + m_versions1_start;
}

uint64_t* SparseFile::get_lhs_versions_end() {
    return get_lhs_content_start() + m_empty1_start;
}

const uint64_t* SparseFile::get_lhs_versions_end() const {
    return get_lhs_content_start() + m_empty1_start;
}

uint64_t* SparseFile::get_rhs_content_start() {
    return get_lhs_content_start() + m_versions2_start;
}

const uint64_t* SparseFile::get_rhs_content_start() const {
    return get_lhs_content_start() + m_versions2_start;
}

uint64_t* SparseFile::get_rhs_content_end() {
    return get_lhs_content_start() + max_num_qwords();
}

const uint64_t* SparseFile::get_rhs_content_end() const {
    return get_lhs_content_start() + max_num_qwords();
}

uint64_t* SparseFile::get_rhs_versions_start() {
    return get_lhs_content_start() + m_empty2_start;
}

const uint64_t* SparseFile::get_rhs_versions_start() const {
    return get_lhs_content_start() + m_empty2_start;
}

uint64_t* SparseFile::get_rhs_versions_end(){
    return get_rhs_content_start();
}

const uint64_t* SparseFile::get_rhs_versions_end() const {
    return get_rhs_content_start();
}

uint64_t* SparseFile::get_content_start(bool is_lhs){
    return is_lhs ? get_lhs_content_start() : get_rhs_content_start();
}

const uint64_t* SparseFile::get_content_start(bool is_lhs) const {
    return is_lhs ? get_lhs_content_start() : get_rhs_content_start();
}

uint64_t* SparseFile::get_content_end(bool is_lhs){
    return is_lhs ? get_lhs_content_end() : get_rhs_content_end();
}

const uint64_t* SparseFile::get_content_end(bool is_lhs) const {
    return is_lhs ? get_lhs_content_end() : get_rhs_content_end();
}

uint64_t* SparseFile::get_versions_start(bool is_lhs) {
    return is_lhs ? get_lhs_versions_start() : get_rhs_versions_start();
}

const uint64_t* SparseFile::get_versions_start(bool is_lhs) const {
    return is_lhs ? get_lhs_versions_start() : get_rhs_versions_start();
}

uint64_t* SparseFile::get_versions_end(bool is_lhs) {
    return is_lhs ? get_lhs_versions_end() : get_rhs_versions_end();
}

const uint64_t* SparseFile::get_versions_end(bool is_lhs) const {
    return is_lhs ? get_lhs_versions_end() : get_rhs_versions_end();
}

double* SparseFile::get_lhs_weights(const Context& context) {
    return reinterpret_cast<double*>(get_lhs_content_start() + Leaf::data_size_qwords(context.m_leaf->num_segments()));
}

const double* SparseFile::get_lhs_weights(const Context& context) const {
    return reinterpret_cast<const double*>(get_lhs_content_start() + Leaf::data_size_qwords(context.m_leaf->num_segments()));
}

double* SparseFile::get_rhs_weights(const Context& context) {
    return get_lhs_weights(context) + static_cast<uint64_t>(m_versions2_start);
}

const double* SparseFile::get_rhs_weights(const Context& context) const {
    return get_lhs_weights(context) + static_cast<uint64_t>(m_versions2_start);
}

double* SparseFile::get_weights(const Context& context, bool is_lhs) {
    return is_lhs ? get_lhs_weights(context) : get_rhs_weights(context);
}

const double* SparseFile::get_weights(const Context& context, bool is_lhs) const {
    return is_lhs ? get_lhs_weights(context) : get_rhs_weights(context);
}

Vertex* SparseFile::get_vertex(uint64_t* ptr){
    return reinterpret_cast<Vertex*>(ptr);
}

const Vertex* SparseFile::get_vertex(const uint64_t* ptr){
    return reinterpret_cast<const Vertex*>(ptr);
}

Edge* SparseFile::get_edge(uint64_t* ptr){
    return reinterpret_cast<Edge*>(ptr);
}

const Edge* SparseFile::get_edge(const uint64_t* ptr) {
    return reinterpret_cast<const Edge*>(ptr);
}

Version* SparseFile::get_version(uint64_t* ptr){
    return reinterpret_cast<Version*>(ptr);
}

const Version* SparseFile::get_version(const uint64_t* ptr){
    return reinterpret_cast<const Version*>(ptr);
}

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
        const Edge* edge = get_edge(content + OFFSET_VERTEX);
        return Key { vertex->m_vertex_id, edge->m_destination };
    }
}

Key SparseFile::fetch_pivot(Context& context) const {
    if(context.has_version()){ // optimistic latch
        return fetch_pivot_impl<true>(context);
    } else { // locked
        return fetch_pivot_impl<false>(context);
    }
}

template<bool is_optimistic>
Key SparseFile::fetch_pivot_impl(Context& context) const {
    if(is_rhs_empty()){
        if(is_optimistic) context.validate_version();
        return KEY_MAX;
    }

    const uint64_t* __restrict content = get_rhs_content_start();
    if(is_optimistic) context.validate_version(); // again, before jumping to any pointer, check it's valid
    const Vertex* vertex = get_vertex(content);
    if(vertex->m_first){ // first vertex entry in the edge list
        uint64_t vertex_id = vertex->m_vertex_id;
        if(is_optimistic) context.validate_version();
        return Key { vertex_id };
    } else { // as this is not the first vertex in the chain, then it must contain some edges
        assert(vertex->m_count > 0);
        const Edge* edge = get_edge(content + OFFSET_VERTEX);
        uint64_t source = vertex->m_vertex_id;
        uint64_t destination = edge->m_destination;
        if(is_optimistic) context.validate_version();
        return Key { source, destination };
    }
}

void SparseFile::update_pivot() {
    if(context::StaticConfiguration::memstore_duplicate_pivot){
        Context context { nullptr }; // ignore
        Key pivot = fetch_pivot_impl</* optimistic? */ false>(context);
        reinterpret_cast<Key*>(this +1)[0] = pivot;
    }
}

Key SparseFile::get_pivot(Context& context) const {
    if(context::StaticConfiguration::memstore_duplicate_pivot){
        Key pivot = reinterpret_cast<const Key*>(this +1)[0];
        context.validate_version_if_present();
        return pivot;
    } else {
        return fetch_pivot(context);
    }
}

uint64_t SparseFile::cardinality() const {
    uint64_t count = 0;

    for(int i = 1; i >= 0; i--){
        const bool is_lhs = i;
        const uint64_t* __restrict c_start = get_content_start(is_lhs);
        const uint64_t* __restrict c_end = get_content_end(is_lhs);
        uint64_t c_index = 0;
        uint64_t c_length = c_end - c_start;

        while(c_index < c_length){
            // fetch a vertex
            const Vertex* vertex = get_vertex(c_start + c_index);
            count += /* vertex */ 1 + /* attached edges */ vertex->m_count;

            // next vertex
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
        }

    }

    return count;
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/

bool SparseFile::update(Context& context, const Update& update, bool has_source_vertex){
    bool is_lhs = update.key() < get_pivot(context);

    if(update.is_vertex()){
        return update_vertex(context, update, is_lhs);
    } else {
        return update_edge(context, update, is_lhs, has_source_vertex);
    }
}

bool SparseFile::update_vertex(Context& context, const Update& update, bool is_lhs) {
    profiler::ScopedTimer profiler { profiler::SF_UPDATE_VERTEX };

    COUT_DEBUG("context: " << context << ", update: " << update << ", is_lhs: " << is_lhs);
    const uint64_t vertex_id = update.source();

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);
    double* __restrict weights = get_weights(context, is_lhs);

    // first, find the position in the content area where to insert the new vertex
    uint64_t v_backptr = 0;
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    bool c_found = false;
    bool stop = false;
    while(c_index < c_length && !stop){
        Vertex* vertex = get_vertex(c_start + c_index);
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
        Version* version = get_version(v_start + v_index);
        uint64_t backptr = version->get_backptr();
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            v_found = c_found && (backptr == v_backptr);
            stop = backptr >= v_backptr;
        }
    }

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
    int64_t c_shift = (!c_found) * OFFSET_VERTEX;
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
                memmove(/* to */ c_start + c_index + c_shift, /* from */ c_start + c_index, shift_length * sizeof(uint64_t)); // vertex, edges, versions
                memmove(/* to */ weights + c_index + c_shift, /* from */ weights + c_index, (v_start - c_start - c_index) * sizeof(uint64_t)); // weights

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
                memmove(c_start - c_shift, c_start, c_index * sizeof(uint64_t)); // vertex & edges
                memmove(weights - c_shift, weights, c_index * sizeof(uint64_t)); // weights
                c_index -= OFFSET_VERTEX;
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

    // sixth, update the cached pivot
    if(/* is rhs ? */ !is_lhs && !c_found && /* first element ? */ c_index == 0) {
        update_pivot();
    }

    // done
    return true;
}


bool SparseFile::update_edge(memstore::Context& context, const Update& update, bool is_lhs, bool has_source_vertex)  {
    profiler::ScopedTimer profiler { profiler::SF_UPDATE_EDGE };
    COUT_DEBUG("context: " << context << ", update: " << update << ", is_lhs: " << is_lhs << ", has_source_vertex: " << has_source_vertex);

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);
    double* __restrict weights = get_weights(context, is_lhs);

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
            c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else if(vertex->m_vertex_id == update.source()){
            vertex_found = true;
            v_backptr_csve = v_backptr;

            c_index_edge = c_index_vertex + OFFSET_VERTEX;
            v_backptr++;

            int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
            while(c_index_edge < e_length && !stop){
                Edge* edge = get_edge(c_start + c_index_edge);
                if(edge->m_destination < update.destination()){
                    c_index_edge += OFFSET_EDGE;
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
            // at this point the source vertex can exist only iff it is stored in the previous segment. But if the edge
            // to insert is going to be set at a position greater than zero, it means that there is some other item already
            // preceding s in this segment
            throw Error{ Key(update.source()), Error::VertexDoesNotExist };
        } else { // c_index_edge == 0
            throw NotSureIfItHasSourceVertex{};
        }
    }

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
    int64_t c_shift = (!vertex_found) * OFFSET_VERTEX + (!edge_found) * OFFSET_EDGE;
    int64_t v_shift = (!version_found) * OFFSET_VERSION + c_shift;
    if((int64_t) free_space() < v_shift) return false;

    // fifth, insert the record into the sparse array
    // similar to #update_vertex()
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
                memmove(c_start + c_index_edge + c_shift, c_start + c_index_edge, shift_length * sizeof(uint64_t)); // vertex & edges
                memmove(weights + c_index_edge + c_shift, weights + c_index_edge, ((v_start - c_start) - c_index_edge) * sizeof(double)); // weights

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
                memmove(c_start - c_shift, c_start, c_index_edge * sizeof(uint64_t)); // vertices & edges
                memmove(weights - c_shift, weights, c_index_edge * sizeof(double)); // weights
                c_index_vertex -= OFFSET_EDGE; // we shifted it back by the amount to store an edge
                c_index_edge -= OFFSET_EDGE; // move back to the position of the previous item
            }

            // update the source vertex attached to this edge
            if(!vertex_found){
                c_index_vertex = c_index_edge;
                c_index_edge = c_index_vertex + OFFSET_VERTEX;
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
    reinterpret_cast<Update*>(undo->payload())->set_weight(edge->get_weight(context));
    edge->set_weight(context, update.weight());

    // eight, update the pivot
    if(/* rhs only */ !is_lhs && !edge_found && c_index_edge == OFFSET_VERTEX /* the first element could be a dummy vertex */){
        update_pivot();
    }

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

bool SparseFile::remove_vertex(RemoveVertex& instance){
    Key pivot = get_pivot(instance.context());

    if(instance.m_key < pivot){ // visit the lhs
        bool success = do_remove_vertex(instance, /* lhs ? */ true);
        if(!success) return false;
    }

    if(!instance.done()){ // visit the rhs
        return do_remove_vertex(instance, /* lhs ? */ false);
    }

    return true; // -> success
}

bool SparseFile::do_remove_vertex(RemoveVertex& instance, bool is_lhs){
    COUT_DEBUG("context: " << instance.context() << ", is_lhs: " << boolalpha << is_lhs);
    const uint64_t vertex_id = instance.vertex_id();

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // first, find the position in the content area
    Vertex* vertex { nullptr };
    uint64_t v_backptr = 0;
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    bool c_found = false;
    bool stop = false;
    while(c_index < c_length && !stop){
        vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id <vertex_id){
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else {
            c_found = vertex->m_vertex_id == vertex_id;
            stop = vertex->m_vertex_id >= vertex_id;
        }
    }

    if(!c_found){
        instance.set_done();
        return true;
    }

    // second, find the position in the versions area
    Version* v_src { nullptr };
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    bool v_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        v_src = get_version(v_start + v_index);
        uint64_t backptr = v_src->get_backptr();
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            v_found = c_found && (backptr == v_backptr);
            stop = backptr >= v_backptr;
        }
    }
    int64_t v_bookmark = v_index;

    // three, consistency checks
    if(vertex->m_first == 1){
        if(v_found && !instance.context().m_transaction->can_write(v_src->get_undo())){
            throw Error( Key {vertex_id}, Error::VertexLocked);
        } else if (vertex->m_lock == 0 && v_found && v_src->is_remove()) {
            throw Error( Key {vertex_id}, Error::VertexDoesNotExist );
        }
    }

    // fourth, remove the vertex
    int64_t budget = free_space();
    Version* v_scratchpad = reinterpret_cast<Version*>(instance.scratchpad());
    uint64_t scratchpad_pos = 0;
    if(vertex->m_first == 1 && vertex->m_lock == 0){
        Version* v_dest = v_scratchpad + scratchpad_pos;
        if(!v_found){
            if(budget < (int64_t) OFFSET_VERSION) {
                return false; /* no space left */
            }
            v_dest->reset();
            budget -= OFFSET_VERSION;
        } else {
            v_scratchpad[scratchpad_pos] = *v_src;
            v_index++;
        }

        Update update { /* vertex ? */ true, /* insert ? */ false, Key { vertex_id } };
        v_dest->set_type(update);
        v_dest->set_backptr(v_backptr);

        transaction::Undo* undo = instance.context().m_transaction->add_undo(instance.context().m_tree, update);
        reinterpret_cast<Update*>(undo->payload())->flip(); // insert -> remove, remove -> insert
        undo->set_active(v_found ? v_src->get_undo() : nullptr);
        v_dest->set_undo(undo);

        assert(instance.m_key.destination() == 0 && "dest == 0 -> the key is a vertex, the first vertex starts from 1");
        instance.m_key.set(vertex_id, 1);
        scratchpad_pos++;
        instance.m_num_items_removed++;

    }
    instance.m_unlock_required = true;
    vertex->m_lock = 1;
    v_backptr++;

    // fifth, remove the edges
    c_index += OFFSET_VERTEX;
    int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
    bool has_conflict = false;
    bool no_space_left = false;
    while(c_index < e_length){
        bool ignore_edge = false;
        Edge* edge = get_edge(c_start + c_index);

        Version* v_src = nullptr;
        Version* v_dest = v_scratchpad + scratchpad_pos;
        if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
            v_src = get_version(v_start + v_index);
            if(!instance.context().m_transaction->can_write(v_src->get_undo())){ has_conflict = true; break; }
            ignore_edge = v_src->is_remove();
            *v_dest = *v_src;
            v_index++; // next iteration
        } else {
            if(budget < (int64_t) OFFSET_VERSION){ no_space_left = true; break; }
            v_dest->reset();
            budget -= OFFSET_VERSION;
        }

        if(!ignore_edge){
            Update update { /* vertex ? */ false, /* insert ? */ false, Key { vertex_id, edge->m_destination }, edge->get_weight(instance.context()) };
            v_dest->set_type(update);
            v_dest->set_backptr(v_backptr);

            transaction::Undo* undo = instance.context().m_transaction->add_undo(instance.context().m_tree, update);
            reinterpret_cast<Update*>(undo->payload())->flip(); // insert -> remove, remove -> insert
            undo->set_active(v_src != nullptr ? v_src->get_undo() : nullptr);
            v_dest->set_undo(undo);

            instance.record_removed_edge(edge);
        }

        instance.m_key.set(vertex_id, edge->m_destination +1);
        c_index += OFFSET_EDGE;
        scratchpad_pos++;
        v_backptr++;
    }

    // 6: copy the remaining versions into the scratchpad
    uint64_t copy_sz = (v_length - v_index) * OFFSET_VERSION;
    memcpy(instance.scratchpad() + scratchpad_pos, v_start + v_index, copy_sz * sizeof(uint64_t));
    scratchpad_pos += copy_sz;

    // 7: copy the versions from the scratchpad back to the sparse file
    copy_scratchpad(instance, is_lhs, scratchpad_pos, v_bookmark);

    // 8: if there has been a conflict, report it!
    if(has_conflict == true){
        Edge* edge = get_edge(c_start + c_index);
        throw Error { Key {vertex_id, edge->m_destination}, Error::EdgeLocked };
    }

    // 9: do we need more space to remove the edges?
    if(no_space_left == true){
        return false;
    }

    // 10: we're done
    assert(c_index == e_length && "We didn't visit all edges");
    if(vertex->m_first == 1 && e_length < c_length){
        vertex->m_lock = 0; // we can immediately release the lock
        instance.m_unlock_required = false;
    }

    return true; // success
}

void SparseFile::copy_scratchpad(RemoveVertex& instance, bool is_lhs, int64_t scratchpad_pos, int64_t bookmark) {
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);
    int64_t v_length = v_end - v_start;
    assert(v_length >= bookmark && "Underflow");
    int64_t v_add = scratchpad_pos - (v_length - bookmark);

    uint64_t copy_sz = scratchpad_pos * OFFSET_VERSION;

    if(is_lhs){
        memcpy(v_start + bookmark, instance.scratchpad(), copy_sz * sizeof(uint64_t));
        m_empty1_start += v_add;
    } else {
        memmove(v_start - v_add, v_start, bookmark * OFFSET_VERSION * sizeof(uint64_t));
        memcpy(v_start - v_add + bookmark, instance.scratchpad(), copy_sz * sizeof(uint64_t));
        m_empty2_start -= v_add;
    }
}

void SparseFile::unlock_removed_vertex(RemoveVertex& instance){
    Key pivot = get_pivot(instance.context());
    if(instance.m_key >= pivot){ // in this case we proceed right to left
        unlock_removed_vertex(instance, /* lhs ? */ false);
    }

    if(!instance.done()){
        unlock_removed_vertex(instance, /* lhs ? */ true);
    }
}

void SparseFile::unlock_removed_vertex(RemoveVertex& instance, bool is_lhs){
    const uint64_t vertex_id = instance.vertex_id();
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;

    // first, find the position in the content area
    Vertex* vertex { nullptr };
    uint64_t v_backptr = 0;

    bool vertex_found = false;
    bool stop = false;
    bool done = false;
    while(c_index < c_length && !stop){
        vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < vertex_id){
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
            done = true;
        } else {
            vertex_found = vertex->m_vertex_id == vertex_id;
            stop = vertex->m_vertex_id >= vertex_id;
        }
    }

    if(vertex_found){
       assert(vertex->m_vertex_id == vertex_id);
       vertex->m_lock = 0;
       done = vertex->m_first == 1;
    }

    if(done){ instance.set_done(); }
}

/*****************************************************************************
 *                                                                           *
 *   Point lookups                                                           *
 *                                                                           *
 *****************************************************************************/

bool SparseFile::has_item_optimistic(Context& context, const Key& key, bool is_unlocked) const {
    profiler::ScopedTimer profiler { profiler::SF_HAS_ITEM_OPTIMISTIC };
    const bool is_lhs = key < get_pivot(context);
    const bool is_key_vertex = key.destination() == 0; // the min vertex has ID 1, so if the destination is 0, this must be a vertex

    //COUT_DEBUG("context: " << context << ", is_lhs: " << is_lhs << ", key: " << key << ", is_unlocked: " << is_unlocked);

    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);
    context.validate_version(); // check these pointers still make sense

    // search in the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    uint64_t v_backptr = 0;
    const Vertex* vertex = nullptr;
    const Edge* edge = nullptr;
    bool vertex_found = false;
    bool edge_found = false;
    bool stop = false;

    while(c_index < c_length && !stop){
        vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < key.source()){
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            v_backptr += 1 + vertex->m_count;
        } else if (vertex->m_vertex_id == key.source()){
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
                    if(edge->m_destination < key.destination()){
                        c_index += OFFSET_EDGE;
                        v_backptr++;
                    } else { // edge->m_destination >= update.m_destination
                        edge_found = edge->m_destination == key.destination();
                        stop = true;
                    }
                }

                stop = true;
            }
        } else {
            stop = true;
        }
    }

    if(is_unlocked && vertex_found && vertex->m_lock == 1){
        assert(is_key_vertex == true && "Otherwise why asking whether the vertex is unlocked?");
        throw Error { key.source(), Error::VertexPhantomWrite };
    }

    if(!vertex_found || (is_key_vertex && vertex->m_first == 0) || (!is_key_vertex && !edge_found)) return false;

    // okay, at this point it exists a record with the given vertex/edge, but we need to check whether
    // the transaction can actually read id
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    const Version* version = nullptr;
    bool version_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        version = get_version(v_start + v_index);
        uint64_t backptr = version->get_backptr();
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            version_found = backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    if(!version_found) return true; // there is not a version around for this record

    Update stored_content = Update::read_delta(context, vertex, edge, version);
    return stored_content.is_insert();
}


double SparseFile::get_weight_optimistic(Context& context, const Key& key) const {
    profiler::ScopedTimer profiler { profiler::SF_GET_WEIGHT_OPTIMISTIC };
    const bool is_lhs = key < get_pivot(context);
    //COUT_DEBUG("context: " << context << ", is_lhs: " << is_lhs << ", key: " << key);

    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);
    context.validate_version(); // check these pointers still make sense

    // search in the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    uint64_t v_backptr = 0;
    const Vertex* vertex = nullptr;
    const Edge* edge = nullptr;
    bool edge_found = false;
    bool stop = false;

    while(c_index < c_length && !stop){
        vertex = get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < key.source()){
            c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            v_backptr += 1 + vertex->m_count;
        } else if (vertex->m_vertex_id == key.source()){
            // find the edge
            c_index += OFFSET_VERTEX;
            v_backptr++; // skip the vertex

            int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
            while(c_index < e_length && !stop){
                edge = get_edge(c_start + c_index);
                if(edge->m_destination < key.destination()){
                    c_index += OFFSET_EDGE;
                    v_backptr++;
                } else { // edge->m_destination >= update.m_destination
                    edge_found = edge->m_destination == key.destination();
                    stop = true;
                }
            }

            stop = true;
        } else {
            stop = true;
        }
    }

    if(!edge_found) { throw Error{ key, Error::EdgeDoesNotExist }; }
    assert(vertex != nullptr && edge != nullptr); // because edge_found == true

    // okay, at this point it exists a record with the given vertex/edge, but we need to check whether
    // the transaction can actually read id
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    const Version* version = nullptr;
    bool version_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        version = get_version(v_start + v_index);
        uint64_t backptr = version->get_backptr();
        if(backptr < v_backptr){
            v_index += OFFSET_VERSION;
        } else {
            version_found = backptr == v_backptr;
            stop = backptr >= v_backptr;
        }
    }

    if(!version_found) { // there is not a version around for this record
        double value =  edge->get_weight(context);
        context.validate_version();
        return value;
    } else {
        Update stored_content = Update::read_delta(context, vertex, edge, version);
        if(stored_content.is_insert()){
            return stored_content.weight();
        } else {
            throw Error{ key, Error::EdgeDoesNotExist };
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *   Degree                                                                  *
 *                                                                           *
 *****************************************************************************/
template<bool is_optimistic>
pair<bool, uint64_t> SparseFile::get_degree(Context& context, bool is_lhs, const Key& key, bool& has_found_vertex) const {
    bool has_next = false;
    uint64_t edge_count = 0;
    const uint64_t vertex_id = key.source();
    // if the degree of a vertex spans over multiple segments and a rebalance occurred in the meanwhile, there is the
    // possibility we may re-read edges we have already visited before the rebalance occurred. In this case, simply
    // skip those edges
    const uint64_t min_destination = key.destination();

    // pointers to the static & delta portions of the segment
    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);
    if(is_optimistic) context.validate_version(); // check these pointers are valid

    // find the vertex in the segment
    uint64_t v_backptr = 0;
    int64_t c_index_vertex = 0;
    int64_t c_length = c_end - c_start;
    bool c_found = false;
    bool stop = false;
    while(c_index_vertex < c_length && !stop){
        const Vertex* vertex = get_vertex(c_start + c_index_vertex);
        if(vertex->m_vertex_id < vertex_id){
            c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else {
            c_found = vertex->m_vertex_id == vertex_id;
            stop = true;
        }
    }

    if(c_found){ // vertex found?
        const Vertex* vertex = get_vertex(c_start + c_index_vertex);
        int64_t e_length = c_index_vertex + OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
        if(is_optimistic && e_length > c_length) { context.validate_version(); } // overflow

        const bool is_dirty = v_start != v_end;
        if(is_dirty){
            // move the cursor v_index to v_backptr
            int64_t v_index = 0;
            int64_t v_length = v_end - v_start;

            while(v_index < v_length && get_version(v_start + v_index)->get_backptr() < v_backptr) v_index++;

            // check whether we can "see" this vertex
            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                if(vertex->m_first == 1 && !has_found_vertex){
                    const Version* version = get_version(v_start + v_index);
                    Update update = Update::read_delta(context, vertex, nullptr, version);
                    if(update.is_insert()){
                        has_found_vertex = true;
                    } else {
                        throw Error{ Key { vertex_id }, Error::Type::VertexDoesNotExist };
                    }
                }

                // next iteration
                v_index ++;

            } else if (vertex->m_first == 1){ // this vertex does not have a history => it is visible to all transactions
                has_found_vertex = true;
            }

            v_backptr++;

            // skip the edges that are greater than min_destination. We have already counted them, but these could
            // reappear again due to a rebalance
            int64_t c_index_edge = c_index_vertex + OFFSET_VERTEX;
            while(c_index_edge < e_length && get_edge(c_start + c_index_edge)->m_destination < min_destination){
                c_index_edge += OFFSET_EDGE;
                v_backptr++;
            }
            while(v_index < v_length && get_version(v_start + v_index)->get_backptr() < v_backptr) v_index++;
            uint64_t v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : numeric_limits<uint64_t>::max();

            // iterate over the remaining edges of the vertex
            while(c_index_edge < e_length){
                const Edge* edge = get_edge(c_start + c_index_edge);
                const Version* version = nullptr;

                // check whether the edge has a version chain
                if(v_backptr == v_next){
                    version = get_version(v_start + v_index);

                    // next iteration
                    v_index ++;
                    if(v_index < v_length){
                        v_next = get_version(v_start + v_index)->get_backptr();
                    } else {
                        v_next = numeric_limits<uint64_t>::max();
                    }
                }

                Update update = Update::read_delta(context, vertex, edge, version);
                edge_count += update.is_insert();

                // next iteration
                c_index_edge += OFFSET_EDGE;
                v_backptr++;
            }
        } else { // there are no versions around
            has_found_vertex = true;
            edge_count = vertex->m_count;

            // skip the edges we have already visited
            if(min_destination > 0){
                int64_t c_index_edge = c_index_vertex + OFFSET_VERTEX;
                while(c_index_edge < e_length && get_edge(c_start + c_index_edge)->m_destination < min_destination){
                    c_index_edge += OFFSET_EDGE;
                    edge_count --;
                }
            }
        }

        has_next = (e_length == c_length);
    } else if (!has_found_vertex){
        if(is_optimistic) context.validate_version();
        throw Error{ Key { vertex_id }, Error::Type::VertexDoesNotExist };
    }

    if(is_optimistic) context.validate_version(); // check what we've computed so far is still correct
    return make_pair(has_next, edge_count);
}

uint64_t SparseFile::get_degree(Context& context, const Key& key, bool& out_has_found_vertex) const {
    const bool is_optimistic = context.has_version();
    profiler::ScopedTimer profiler { !is_optimistic ? profiler::SF_GET_DEGREE : profiler::SF_GET_DEGREE_OPTIMISTIC };

    Key pivot = get_pivot(context);
    auto result = make_pair<bool, uint64_t>(true, 0);

    if(key < pivot){ // visit the lhs
            result = is_optimistic ?
                    get_degree</* is optimistic ? */ true>(context, /* lhs ? */ true, key, out_has_found_vertex) :
                    get_degree</* is optimistic ? */ false>(context, /* lhs ? */ true, key, out_has_found_vertex) ;
    }

    if(result.first){ // visit the rhs
        auto result_rhs = is_optimistic ?
                get_degree</* is optimistic ? */ true>(context, /* lhs ? */ false, key, out_has_found_vertex) :
                get_degree</* is optimistic ? */ false>(context, /* lhs ? */ false, key, out_has_found_vertex) ;
        result.first = result_rhs.first;
        result.second += result_rhs.second;
    }

    return result.second;
}


/*****************************************************************************
 *                                                                           *
 *   Auxiliary view                                                          *
 *                                                                           *
 *****************************************************************************/
template<bool check_end_interval>
bool SparseFile::aux_partial_result_impl(Context& context, bool is_lhs, const Key& next, aux::PartialResult* partial_result) const {
    bool read_next = true;
    const uint64_t vertex_id = next.source();
    const uint64_t min_destination = next.destination();
    const Key last_key = partial_result->key_to();

    // pointers to the static & delta portions of the segment
    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // find the starting point in the segment
    uint64_t v_backptr = 0;
    int64_t c_index_vertex = 0;
    int64_t c_length = c_end - c_start;
    uint64_t first_vertex_skip_edges = 0; // number of edges to skip in the first vertex, because they do not belong to the interval
    bool starting_point_found = false;
    while(c_index_vertex < c_length && !starting_point_found){
        const Vertex* vertex = get_vertex(c_start + c_index_vertex);
        if(vertex->m_vertex_id < vertex_id){
            c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else {
            if(vertex_id == vertex->m_vertex_id && min_destination > 0){
                int64_t c_index_edge = c_index_vertex + OFFSET_VERTEX;
                int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
                // do not alter v_backptr here, it must point to the first vertex in the sequence

                // find the starting edge
                while(c_index_edge < e_length && !starting_point_found){
                    const Edge* edge = get_edge(c_start + c_index_edge);
                    if(edge->m_destination < min_destination){
                        c_index_edge += OFFSET_EDGE;
                    } else {
                        if(check_end_interval){  read_next = last_key > Key{ vertex->m_vertex_id, edge->m_destination }; }
                        first_vertex_skip_edges = (c_index_edge - (c_index_vertex + OFFSET_VERTEX) ) / OFFSET_EDGE;
                        starting_point_found = true;
                    }
                }

                if(!starting_point_found){
                    v_backptr += 1 + vertex->m_count;
                }
            } else {
                starting_point_found = true;
                if(check_end_interval){ read_next = last_key > Key { vertex->m_vertex_id }; }
                first_vertex_skip_edges = 0;
            }
        }
    }

    if(starting_point_found && read_next){ // start processing the segment
        const bool is_dirty = v_start != v_end;

        if(is_dirty){ // we need to check whether the transaction can read the data items in the segment
            // starting version
            int64_t v_index = 0;
            int64_t v_length = v_end - v_start;
            while(v_index < v_length && get_version(v_start + v_index)->get_backptr() < v_backptr) v_index++;
            uint64_t v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : std::numeric_limits<uint64_t>::max();

            do {
                const Vertex* vertex = get_vertex(c_start + c_index_vertex);

                // retrieve the version (if present)
                const Version* version = nullptr;
                if(v_backptr == v_next){
                    version = get_version(v_start + v_index);

                    // next iteration
                    v_index ++;
                    v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : std::numeric_limits<uint64_t>::max();
                }
                v_backptr++;

                bool process_edges = (version == nullptr) || Update::read_delta(context, vertex, nullptr, version).is_insert();
                if(check_end_interval && last_key.source() < vertex->m_vertex_id){ // we're done
                    process_edges = false;
                    read_next = false;
                }

                if(process_edges){
                    int64_t c_index_edge = c_index_vertex + OFFSET_VERTEX;
                    int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
                    c_index_edge += first_vertex_skip_edges * OFFSET_EDGE;
                    uint64_t edge_count = 0;
                    v_backptr += first_vertex_skip_edges;

                    while(read_next && c_index_edge < e_length){
                        const Edge* edge = get_edge(c_start + c_index_edge);

                        if(check_end_interval && last_key.source() == vertex->m_vertex_id) {
                            read_next = edge->m_destination < last_key.destination();
                        }

                        if(read_next){

                            // retrieve the version (if present)
                            const Version* version = nullptr;
                            if(v_backptr == v_next){
                                version = get_version(v_start + v_index);

                                // next iteration
                                v_index ++;
                                v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : std::numeric_limits<uint64_t>::max();
                            }

                            if(version != nullptr){
                                Update update = Update::read_delta(context, vertex, edge, version);
                                assert(update.is_edge() && "Expected an edge");
                                assert(update.source() == vertex->m_vertex_id && "source mismatch");
                                assert(update.destination() == edge->m_destination && "destination mismatch");
                                edge_count += update.is_insert();
                            } else {
                                edge_count ++;
                            }

                            c_index_edge += OFFSET_EDGE;
                        }
                        v_backptr++;
                    }

                    if(vertex->m_first || edge_count > 0) {
                        partial_result->incr_degree(vertex->m_vertex_id, edge_count);
                    }
                } else {
                    v_backptr += vertex->m_count; // skip the edges

                    while(v_index < v_length && get_version(v_start + v_index)->get_backptr() < v_backptr) v_index++;
                    v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : std::numeric_limits<uint64_t>::max();
                }

                // next iteration
                first_vertex_skip_edges = 0;
                c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            } while (read_next && c_index_vertex < c_length);


        } else { // the segment does not have any versions

            do {
                const Vertex* vertex = get_vertex(c_start + c_index_vertex);
                uint64_t edge_count = vertex->m_count - first_vertex_skip_edges;

                if(check_end_interval){
                    if(last_key.source() < vertex->m_vertex_id){
                        edge_count = 0;
                        read_next = false;
                    } else if (last_key.source() == vertex->m_vertex_id){
                        int64_t c_index_edge = c_index_vertex + OFFSET_VERTEX;
                        int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
                        c_index_edge += first_vertex_skip_edges * OFFSET_EDGE;

                        while(read_next && c_index_edge < e_length){
                            const Edge* edge = get_edge(c_start + c_index_edge);
                            read_next = edge->m_destination < last_key.destination();
                            if(read_next) { c_index_edge += OFFSET_EDGE; }
                        }

                        if(!read_next){
                            edge_count -= (e_length - c_index_edge) / OFFSET_EDGE;
                        }
                    }
                } // check_end_interval

                if(vertex->m_first || edge_count > 0) {
                    partial_result->incr_degree(vertex->m_vertex_id, edge_count);
                }

                // next iteration
                first_vertex_skip_edges = 0;
                c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            } while (read_next && c_index_vertex < c_length);
        }
    }

    return read_next;
}

bool SparseFile::aux_partial_result(Context& context, const Key& next, bool check_end_interval, aux::PartialResult* partial_result) const {
    assert(!context.has_version() && "Assuming a read lock has been acquired to the segment");
    profiler::ScopedTimer profiler { profiler::SF_AUX_PARTIAL_RESULT };

    bool read_next = true;
    Key pivot = get_pivot(context);

    if(next < pivot){ // visit the lhs
        bool lhs_check_end_interval = check_end_interval && partial_result->key_to() < pivot;
        read_next = lhs_check_end_interval ?
            aux_partial_result_impl</* check end interval ? */ true>(context, /* lhs ? */ true, next, partial_result) :
            aux_partial_result_impl</* check end interval ? */ false>(context, /* lhs ? */ true, next, partial_result) ;
    }

    if(read_next){ // visit the rhs
        bool rhs_check_end_interval = check_end_interval;
        read_next = rhs_check_end_interval ?
            aux_partial_result_impl</* check end interval ? */ true>(context, /* lhs ? */ false, next, partial_result) :
            aux_partial_result_impl</* check end interval ? */ false>(context, /* lhs ? */ false, next, partial_result) ;
    }

    return read_next;
}

/*****************************************************************************
 *                                                                           *
 *   Rollback                                                                *
 *                                                                           *
 *****************************************************************************/

void SparseFile::rollback(Context& context, const Update& update, transaction::Undo* next){
    profiler::ScopedTimer profiler { profiler::SF_ROLLBACK };
    const bool is_lhs = update.key() < get_pivot(context);

    COUT_DEBUG("context: " << context << ", update: " << update << ", is_lhs: " << is_lhs << ", next: " << next);

    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);
    double* __restrict weights = get_weights(context, is_lhs);

    // we need to find the vertex/edge in the content section and its version in the versions area
    // let's start with the content area
    int64_t c_index_vertex = 0;
    int64_t c_index_edge = -1;
    int64_t c_length = c_end - c_start;
    uint64_t v_backptr = 0;
    Vertex* vertex = nullptr;
    Edge* edge = nullptr;
    [[maybe_unused]] bool vertex_found = false; // only used to assert
    [[maybe_unused]] bool edge_found = false; // only used to assert
    bool stop = false;

    while(c_index_vertex < c_length && !stop){
        vertex = get_vertex(c_start + c_index_vertex);

        if(vertex->m_vertex_id < update.source()){
            c_index_vertex += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
            v_backptr += 1 + vertex->m_count;
        } else if (vertex->m_vertex_id == update.source()){
            vertex_found = true;
            if(update.is_vertex()){
                stop = true; // done
            } else {
                v_backptr++; // skip the vertex

                // find the edge
                c_index_edge = c_index_vertex + OFFSET_VERTEX;
                int64_t e_length = c_index_edge + vertex->m_count * OFFSET_EDGE;
                while(c_index_edge < e_length && !stop){
                    edge = get_edge(c_start + c_index_edge);
                    if(edge->m_destination < update.destination()){
                        c_index_edge += OFFSET_EDGE;
                        v_backptr++;
                    } else { // edge->m_destination >= update.m_destination
                        edge_found = edge->m_destination == update.destination();
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
    assert((!update.is_edge() || edge_found == true) && "Edge not found in the content area?");

    // find the version in the versions area
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    [[maybe_unused]] bool version_found = false; // only used to assert
    stop = false;
    while(v_index < v_length && !stop){
        Version* version = get_version(v_start + v_index);
        uint64_t backptr = version->get_backptr();
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
        bool remove_vertex = update.is_remove() && ( update.is_vertex() || ( /* is_edge && */ vertex->m_first == 0 && vertex->m_count == 1));
        bool remove_edge = update.is_remove() && update.is_edge();
        if(remove_edge && !remove_vertex){ vertex->m_count -= 1; } // fix the vertex cardinality
        if(update.is_edge() && !remove_edge){ edge->set_weight( context, update.weight() ); } // fix the edge weight to what was before
        int v_backptr_shift = remove_vertex + remove_edge; // =2 if we need to remove both vertex & edge, 1 only one item, 0 otherwise
        int64_t c_index = remove_vertex? c_index_vertex : c_index_edge;
        int64_t c_shift = (remove_vertex) * OFFSET_VERTEX + (remove_edge) * OFFSET_EDGE;
        int64_t v_shift = c_shift + OFFSET_VERSION;

        static_assert(OFFSET_VERSION == 1, "Otherwise the code below is broken");

        if(is_lhs){ // left hand side
            if(c_shift > 0){
                // shift the content by c_shift
                assert(update.is_remove() && "Remove the record altogether only if did not exist before");
                int64_t c_shift_length = (v_start - c_start) + v_index - c_index;
                memmove(c_start + c_index, c_start + c_index + c_shift, c_shift_length * sizeof(uint64_t)); // vertices & edges
                memmove(weights + c_index, weights + c_index + c_shift, ((v_start - c_start) - c_index) * sizeof(double)); // weights
            }

            // shift the versions
            for(int64_t i = v_index +1; i < v_length; i++){
                v_start[i - v_shift] = v_start[i];
                Version* version = get_version(v_start + i - v_shift);
                assert(version->m_backptr >= v_backptr_shift && "Underflow");
                version->m_backptr -= v_backptr_shift;
            }

            m_versions1_start -= c_shift;
            m_empty1_start -= v_shift;

        } else { // right hand side
            if(c_shift > 0){ // shift the content
                memmove(c_start + c_shift, c_start, c_index * sizeof(uint64_t)); // vertices & edges
                memmove(weights + c_shift, weights, c_index * sizeof(double)); // weights

                // shift the versions
                for(int64_t i = v_length -1; i >= v_index +1; i--){
                    v_start[i + c_shift] = v_start[i];
                    Version* version = get_version(v_start + i + c_shift);
                    assert(version->m_backptr >= v_backptr_shift && "Underflow");
                    version->m_backptr -= v_backptr_shift;
                }
            } else { // only alter the backptr for the versions
                for(int64_t i = v_length -1; i >= v_index +1; i--){
                    Version* version = get_version(v_start + i);
                    assert(version->m_backptr >= v_backptr_shift && "Underflow");
                    version->m_backptr -= v_backptr_shift;
                }
            }

            for(int64_t i = v_index -1; i >= 0; i--){
                v_start[i + v_shift] = v_start[i];
                // do not alter the backptr
            }

            m_versions2_start += c_shift;
            m_empty2_start += v_shift;

            if(c_shift > 0 && c_index == 0){
                update_pivot();
            }
        }

        if(remove_vertex && update.is_vertex()){
            context.m_tree->vertex_table()->remove(update.source());
        }

    } else { // keep the record alive as other versions exist
        Version* version = get_version(v_start + v_index);
        version->set_type(update.is_insert());
        version->unset_undo(next);

        // restore the weight if this is an edge
        assert((!update.is_edge() || edge_found == true) && "Edge not found in the content area?");
        if(update.is_edge()){
            Edge* edge = get_edge(c_start + c_index_edge);
            assert(edge->m_destination == update.destination() && "Key mismatch");
            edge->set_weight( context, update.weight() );
        }
    }

    // and that's it...
}

/*****************************************************************************
 *                                                                           *
 *   Load                                                                    *
 *                                                                           *
 *****************************************************************************/
void SparseFile::load(Context& context, rebalance::ScratchPad& buffer){
    profiler::ScopedTimer profiler { profiler::SF_LOAD };

    load(context, buffer, /* lhs ? */ true);
    load(context, buffer, /* lhs ? */ false);
}

void SparseFile::load(Context& context, rebalance::ScratchPad& scratchpad, bool is_lhs){
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // iterate over the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    uint64_t v_backptr = 0;
    Vertex* vertex = nullptr;
    Edge* edge = nullptr;
    Version* version = nullptr;

    while(c_index < c_length){
        // Fetch a vertex
        vertex = get_vertex(c_start + c_index);
        edge = nullptr;
        version = nullptr;

        if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
            version = get_version(v_start + v_index);
            vertex->validate(version);
            v_index += OFFSET_VERSION;
        }

        if(vertex->m_first == 1 || !scratchpad.has_last_vertex()){
            scratchpad.load_vertex(vertex, version);
        } else { // compact the duplicate vertex entries
            assert(vertex->m_count > 0 && "Dummy vertex with zero edges attached");
            scratchpad.get_last_vertex()->m_count += vertex->m_count;
        }

        c_index += OFFSET_VERTEX;
        v_backptr++;

        // Fetch its edges
        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
        while(c_index < e_length){
            edge = get_edge(c_start + c_index);
            version = nullptr;

            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                version = get_version(v_start + v_index);
                edge->validate(vertex, version);
                v_index += OFFSET_VERSION;
            }

            scratchpad.load_edge(edge->m_destination, edge->get_weight(context), version);

            // next iteration
            c_index += OFFSET_EDGE;
            v_backptr++;
        } // end while, fetch edges
    } // end while, fetch vertices
}


/*****************************************************************************
 *                                                                           *
 *   Save                                                                    *
 *                                                                           *
 *****************************************************************************/

void SparseFile::save(Context& context, rebalance::ScratchPad& scratchpad, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget_achieved) {
    profiler::ScopedTimer profiler { profiler::SF_SAVE };

    COUT_DEBUG("[before] target_budget: " << target_budget << " qwords, pos_next_vertex: " << pos_next_vertex << ", pos_next_element: " << pos_next_element);

    // fill the lhs
    int64_t target_budget_lhs = min<int64_t>(target_budget, target_budget / 2 + (context::StaticConfiguration::test_mode ? 1ull : OFFSET_VERTEX * 3)); // put a few elements more in the lhs than in the rhs
    int64_t achieved_budget_lhs = 0;
    fill(context, scratchpad, /* lhs ? */ true, pos_next_vertex, pos_next_element, target_budget_lhs, &achieved_budget_lhs);

    // fill the rhs
    int64_t target_budget_rhs = max<int64_t>(0ll, target_budget - achieved_budget_lhs);
    int64_t achieved_budget_rhs = 0;
    fill(context, scratchpad, /* lhs ? */ false, pos_next_vertex, pos_next_element, target_budget_rhs, &achieved_budget_rhs);

    update_pivot();

    int64_t budget_achieved = achieved_budget_lhs + achieved_budget_rhs;
    *out_budget_achieved = budget_achieved;

    COUT_DEBUG("[after] target_budget: " << target_budget << " qwords, achieved: " << budget_achieved << " qwords (lhs: " << achieved_budget_lhs << " qwords, rhs: " << achieved_budget_rhs << " qwords), pos_next_vertex: " << pos_next_vertex << ", pos_next_element: " << pos_next_element);
}

void SparseFile::fill(Context& context, rebalance::ScratchPad& scratchpad, bool is_lhs, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget){
    profiler::ScopedTimer profiler { profiler::SF_SAVE_FILL };
    COUT_DEBUG("[before] is_lhs: " << boolalpha << is_lhs << ", pos_next_vertex: " << pos_next_vertex << ", pos_next_element: " << pos_next_element << ", target_budget: " << target_budget << " qwords");

    // decide in advance how many elements to copy from the scratchpad
    int64_t num_elements = 0; // number of elements to write
    int64_t num_versions = 0; // number of versions to store
    std::tie(num_elements, num_versions) = get_num_elts_to_store(context, scratchpad, is_lhs, pos_next_vertex, pos_next_element, target_budget, out_budget);

    // update the segment boundaries
    uint64_t real_space_needed = *out_budget + /* in case we need to write a dummy vertex */ (num_elements > 0 && pos_next_vertex < pos_next_element) * OFFSET_VERTEX;
    assert(*out_budget > 0 || real_space_needed == 0); // if space_consumed == 0 => then we didn't write anything
    if(is_lhs){
        m_versions1_start = real_space_needed - num_versions * OFFSET_VERSION;
        m_empty1_start = real_space_needed;
    } else {
        const uint64_t upper_capacity = max_num_qwords();
        m_empty2_start = upper_capacity - real_space_needed;
        m_versions2_start = static_cast<int64_t>(m_empty2_start) + num_versions * OFFSET_VERSION;

        // check we didn't overflow the segment
        assert(m_versions1_start <= m_empty1_start);
        assert(m_empty2_start <= m_versions2_start);
        assert(m_empty1_start <= m_empty2_start);
    }

    // finally copy the elts from the scratchpad to the segment
    save_elements(context, scratchpad, is_lhs, pos_next_vertex, pos_next_element, num_elements);

#if defined(DEBUG)
    COUT_DEBUG("[after] is_lhs: " << boolalpha << is_lhs << ", pos_next_vertex: " << pos_next_vertex << ", pos_next_element: " << pos_next_element << ", target budget: " << target_budget << " qwords, achieved: " << *out_budget << " qwords");
    dump_after_save(is_lhs);
#endif
}

std::pair</* elts */ int64_t, /* versions */ int64_t> SparseFile::get_num_elts_to_store(Context& context, const rebalance::ScratchPad& scratchpad, bool is_lhs, int64_t pos_next_vertex, int64_t pos_next_element, int64_t target_budget, int64_t* out_budget){
    *out_budget = 0;
    int64_t num_elts = 0;
    int64_t num_versions = 0;
    int64_t space_consumed = 0;
    while(space_consumed < target_budget && pos_next_element < (int64_t) scratchpad.size()){
        Vertex* vertex = scratchpad.get_vertex(pos_next_vertex);
        const bool is_dummy_vertex = pos_next_vertex < pos_next_element;
        const int64_t num_edges = static_cast<int64_t>(vertex->m_count) - (pos_next_element - pos_next_vertex) + is_dummy_vertex; // +1 if this is a dummy vertex

        if(!is_dummy_vertex){ // determine the amount of space_required to write this vertex
            bool has_undo = scratchpad.has_version(pos_next_vertex);
            int64_t space_required = OFFSET_VERTEX + has_undo * OFFSET_VERSION;

            if(!is_lhs && num_edges > 0 && space_consumed + space_required >= target_budget) break; // stop here if we cannot at least write one of its edges

            space_consumed += space_required;
            num_elts++;
            num_versions += has_undo;

            //COUT_DEBUG("[" << pos_next_vertex << "] vertex: " << scratchpad.get_vertex(pos_next_vertex)->m_vertex_id << ", budget: " << space_required);
        }

        if(!is_dummy_vertex) { pos_next_element++; } // next element

        // fetch the edges
        int64_t i = 0;
        while(i < num_edges && space_consumed < target_budget){
            assert(pos_next_element < (int64_t) scratchpad.size() && "Counted more edges than what loaded");

            bool has_undo = scratchpad.has_version(pos_next_element); //m_versions[m_write_cursor].m_version != 0;
            space_consumed += OFFSET_EDGE + (has_undo) * OFFSET_VERSION;
            //COUT_DEBUG("[" << pos_next_element << "] edge: " << scratchpad.get_edge(pos_next_element)->m_destination << ", budget: " << (OFFSET_EDGE + (has_undo) * OFFSET_VERSION));
            num_elts++;
            num_versions += has_undo;
            pos_next_element++;
            i++; // next edge
        }

        /**
         * corner case: if we have just written a non first vertex, we need to write at least one edge
         */
         if(i == 0 && i < num_edges && (is_dummy_vertex || is_lhs)){
             assert(pos_next_element < (int64_t) scratchpad.size() && "Counted more edges than what loaded");

             bool has_undo = scratchpad.has_version(pos_next_element); //m_versions[m_write_cursor].m_version != 0;
             space_consumed += OFFSET_EDGE + (has_undo) * OFFSET_VERSION;
             //COUT_DEBUG("[" << pos_next_element << "] edge: " << scratchpad.get_edge(pos_next_element)->m_destination << ", budget: " << (OFFSET_EDGE + (has_undo) * OFFSET_VERSION));
             num_elts++;
             num_versions += has_undo;
             pos_next_element++;
             i++; // next edge
         }

        if(i == num_edges){ // next vertex
            pos_next_vertex = pos_next_element;
        }
    }

    *out_budget = space_consumed;
    return make_pair(num_elts, num_versions);
}

void SparseFile::save_elements(Context& context, const rebalance::ScratchPad& scratchpad, bool is_lhs, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t num_elts){
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // iterate over the content section
    int64_t c_index = 0;
    [[maybe_unused]] int64_t c_length = c_end - c_start; // only to assert
    int64_t v_index = 0;
    [[maybe_unused]] int64_t v_length = v_end - v_start; // only to assert
    uint64_t v_backptr = 0;

    int64_t i = 0;
    while(i < num_elts){
        // Fetch a vertex
        Vertex* sp_vertex = scratchpad.get_vertex(pos_next_vertex);
        assert(c_index < c_length && "Vertex, overflow");
        Vertex* sf_vertex = get_vertex(c_start + c_index);
        *sf_vertex = *sp_vertex; // copy the content

        const bool is_dummy_vertex = pos_next_vertex < pos_next_element;
        const uint64_t num_edges = static_cast<int64_t>(sp_vertex->m_count) - (pos_next_element - pos_next_vertex) + is_dummy_vertex; // +1 if it's a dummy vertex
        if(is_dummy_vertex){
            sf_vertex->m_first = 0;
        } else {
            // Save the version
            if(scratchpad.has_version(pos_next_vertex)){
                assert(v_index < v_length && "Version, overflow");
                Version* sf_version = get_version(v_start + v_index);
                *sf_version = *(scratchpad.get_version(pos_next_vertex));
                sf_version->m_backptr = v_backptr;

                v_index++;
            }

            // Update the vertex table
            if(sf_vertex->m_first == 1){ // this may still be a dummy vertex, just the very first in the scratchpad
                DirectPointer pointer { context, /* vertex */ (uint64_t) c_index, /* edge */ 0, /* backptr */ v_backptr };
                if(! (context.m_tree->vertex_table()->update(sf_vertex->m_vertex_id, pointer)) ){ // update the pointer
                    context.m_segment->request_rebuild_vertex_table(); // the vertex does not exist in the vertex table
                }
            }

            i++; // consume an element from the buffer
            assert(pos_next_vertex == pos_next_element && "This is how we discriminate a dummy vertex from a first vertex");
            pos_next_element++;
        }
        sf_vertex->m_count = 0; // reset the number of edges stored
        c_index += OFFSET_VERTEX;
        v_backptr++;

        // Fetches its edges
        uint64_t j = 0;
        while(j < num_edges && i < num_elts){
            assert(c_index < c_length && "Edge, overflow");

            auto sp_edge = scratchpad.get_edge(pos_next_element);
            Edge* sf_edge = get_edge(c_start + c_index);
            sf_edge->m_destination = sp_edge->m_destination;
            sf_edge->set_weight(context, sp_edge->m_weight);

            // Save the version
            if(scratchpad.has_version(pos_next_element)){
                assert(v_index < v_length && "Version, overflow");
                Version* sf_version = get_version(v_start + v_index);
                *sf_version = *(scratchpad.get_version(pos_next_element));
                sf_version->m_backptr = v_backptr;

                v_index++;
            }

            c_index += OFFSET_EDGE;
            pos_next_element++;
            v_backptr++;
            j++; i++;
        }

        // set the number of edges in this segment for this vertex
        sf_vertex->m_count = j;

        if(sf_vertex->m_count == num_edges){ // move to the next vertex
            pos_next_vertex = pos_next_element;
        }
    }

    // check we copied all elements
    assert(c_index == c_length && "Not all elements were copied?");
    assert(v_index == v_length && "Not all versions were copied?");
}

/*****************************************************************************
 *                                                                           *
 *   Prune                                                                   *
 *                                                                           *
 *****************************************************************************/
void SparseFile::prune(const Context& context){
    profiler::ScopedTimer profiler { profiler::SF_PRUNE };
    int64_t c_shift = 0, v_shift = 0;

    // LHS
    DEBUG_PRUNE( PruneHistory history_lhs = prune_validate_init(context, /* is_lhs ? */ true) );
    prune_versions(/* is lhs ? */ true);
    DEBUG_PRUNE( prune_validate_unset_versions(context, /* is_lhs ? */ true, history_lhs) );
    tie(c_shift, v_shift) = prune_elements(context, /* is lhs ? */ true);
    DEBUG_PRUNE ( prune_validate_check(context, /* is_lhs ? */ true, history_lhs, c_shift, v_shift) );

    if(c_shift > 0){
        uint64_t* v_start = get_lhs_versions_start();
        uint64_t* v_end = get_lhs_versions_end();
        uint64_t v_length = v_end - v_start;

        // bug: memmove doesn't work here, but I don't know the reason?
        //memmove(v_start - c_shift, v_start, OFFSET_VERSION * v_length);
        for(uint64_t i = 0; i < v_length; i++){
            v_start[i - c_shift] = v_start[i];
        }
    }
    m_versions1_start -= c_shift;
    m_empty1_start -= c_shift + v_shift;
    DEBUG_PRUNE( prune_validate_check(context, /* is_lhs ? */ true, history_lhs) );

    // RHS
    DEBUG_PRUNE( PruneHistory history_rhs = prune_validate_init(context, /* is_lhs ? */ false) );
    prune_versions(/* is lhs ? */ false);
    DEBUG_PRUNE( prune_validate_unset_versions(context, /* is_lhs ? */ false, history_rhs) );
    tie(c_shift, v_shift) = prune_elements(context, /* is lhs ? */ false);
    DEBUG_PRUNE( prune_validate_check(context, /* is_lhs ? */ false, history_rhs, c_shift, v_shift) );

    if(c_shift > 0){
        uint64_t* c_start = get_rhs_content_start();
        uint64_t* c_end = get_rhs_content_end();
        uint64_t c_length = c_end - c_start;
        double* weights = get_rhs_weights(context);
        //memmove(c_start + c_shift, c_start, (c_length - c_shift) * sizeof(uint64_t));
        //memmove(weights + c_shift, weights, (c_length - c_shift) * sizeof(uint64_t));
        for(int64_t i = c_length - c_shift -1; i >= 0; i--){
            c_start[i + c_shift] = c_start[i];
            weights[i + c_shift] = weights[i];
        }
        assert(v_shift > 0 && "If we removed some element, we must also have removed some version");
    }
    if(v_shift > 0){
        uint64_t* v_start = get_rhs_versions_start();
        uint64_t* v_end = get_rhs_versions_end();
        uint64_t v_length = v_end - v_start;
        //memmove(v_start + c_shift + v_shift, v_start, (v_length - v_shift) * sizeof(uint64_t));
        for(int64_t i = v_length - v_shift -1; i >= 0; i--){
            v_start[i + c_shift + v_shift] = v_start[i];
        }

    }
    m_versions2_start += c_shift;
    m_empty2_start += c_shift + v_shift;
    DEBUG_PRUNE( prune_validate_check(context, /* is_lhs ? */ false, history_rhs) );

    update_pivot();
}


void SparseFile::prune_versions(bool is_lhs) {
    profiler::ScopedTimer profiler { profiler::SF_PRUNE_VERSIONS };

    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

    for(int64_t v_index = 0, v_length = v_end - v_start; v_index < v_length; v_index++ ){
        Version* version = get_version(v_start + v_index);
        version->prune();
    }
}

pair<int64_t, int64_t> SparseFile::prune_elements(const Context& context, bool is_lhs) {
    profiler::ScopedTimer profiler { profiler::SF_PRUNE_ELEMENTS };

    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);
    double* __restrict weights = get_weights(context, is_lhs);

    // iterate over the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    uint64_t v_backptr = 0;

    int64_t c_shift = 0; // current shift of the elements (vertices, edges)
    int64_t v_shift = 0; // current shift of the versions
    int64_t v_backptr_shift = 0; // current shift of the version backptr == number of elts removed so far

    while(c_index < c_length){
        // Move the next vertex back by c_shift positions
        if(c_shift != 0){
            static_assert(sizeof(Vertex) == 2 * sizeof(uint64_t)); // copy two words
            c_start[c_index - c_shift] = c_start[c_index]; // first word
            c_start[c_index - c_shift + 1] = c_start[c_index +1]; // second word
        }
        // Get the vertex just moved
        Vertex* vertex = get_vertex(c_start + c_index - c_shift);
        assert((vertex->m_first == 1 || c_index == 0) && "Dummy vertices can only be present at the start of the data section");
        bool vertex_exists = true; // if we removed the vertex, do not try to read or alter its fields

        // Does this vertex have a version?
        if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){ // => yes
            Version* version = get_version(v_start + v_index);
            version->m_backptr -= v_backptr_shift; // update the backptr, taking into account the elts removed so far
            if(v_shift != 0){  // move the version backwards?
                static_assert(sizeof(Version) == 1 * sizeof(uint64_t)); // 1 qword
                v_start[v_index - v_shift] = v_start[v_index];
            }

            if(version->get_undo() == nullptr){ // Was this version pruned?
                v_shift++; // okay, then overwrite the next versions by one position more

                if(version->is_remove()){ // was the vertex completely removed?
                    vertex_exists = false; // hereafter, do not try to read or alter its fields
                    c_shift += OFFSET_VERTEX; // shift all the next elts by 2 positions (OFFSET_VERTEX)
                    v_backptr_shift++; // shift the next versions' backptr by one more position
                }
            }

            v_index += OFFSET_VERSION; // next index in the versions area
        }

        // Iterate over the edges
        c_index += OFFSET_VERTEX; // starting interval for the edges
        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE; // end interval for the edges
        v_backptr++; // next version position (source)
        while(c_index < e_length){
            // Move the next edge back of `c_shift' positions
            if(c_shift != 0){
                static_assert(sizeof(Edge) == 1 * sizeof(uint64_t)); // copy only one word
                c_start[c_index - c_shift] = c_start[c_index]; // edge's destination
                weights[c_index - c_shift] = weights[c_index]; // weight
            }

            // Does this edge have a version ?
            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){ // => yes
                Version* version = get_version(v_start + v_index);
                version->m_backptr -= v_backptr_shift;  // update the backptr, taking into account the elts removed so far
                if(v_shift != 0){  // move the version backwards?
                    static_assert(sizeof(Version) == 1 * sizeof(uint64_t)); // 1 qword
                    v_start[v_index - v_shift] = v_start[v_index];
                }

                if(version->get_undo() == nullptr){ // Was this version pruned?
                    v_shift++; // okay, then overwrite the next versions by one position more

                    if(version->is_remove()){ // was the edge completely removed?
                        c_shift += OFFSET_EDGE; // shift all the next elts by 1 position more (OFFSET_EDGE)
                        v_backptr_shift ++; // shift the next versions' backptr by one more position

                        if(vertex_exists){ // do not alter the vertex fields
                            assert(vertex->m_count > 0 && "Underflow");
                            vertex->m_count--;
                        }
                    }
                }

                v_index += OFFSET_VERSION; // next index in the versions area
            }

            // next iteration
            c_index += OFFSET_EDGE;
            v_backptr++;
        } // end while, fetch edges

        // Remove a dummy vertex ?
        if(vertex_exists && vertex->m_first == 0 && vertex->m_count == 0){
            c_shift += OFFSET_VERTEX;
            // ... there is no need to check v_shift, dummy vertices do not have a version.
            v_backptr_shift++;
        }
    }  // end while, fetch vertices

    return make_pair(c_shift, v_shift);
}

auto SparseFile::prune_validate_init(const Context& context, bool is_lhs) -> PruneHistory {
    PruneHistory history;

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);

     // iterate over the content section
     int64_t c_index = 0;
     int64_t c_length = c_end - c_start;
     int64_t v_index = 0;
     int64_t v_length = v_end - v_start;
     uint64_t v_backptr = 0;
     Vertex* vertex = nullptr;
     Edge* edge = nullptr;
     Version* version = nullptr;

     while(c_index < c_length){
         // Fetch a vertex
         vertex = get_vertex(c_start + c_index);
         edge = nullptr;
         version = nullptr;

         if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
             version = get_version(v_start + v_index);
             v_index += OFFSET_VERSION;
         }

         Update e_vertex { /* is vertex ? */ true,
                          /* is insert ? */ version != nullptr && version->is_remove() ? false : true,
                          /* key */ Key {vertex->m_vertex_id, 0},
                          /* weight */ 0
         };
         history.push_back(PruneHistoryEntry{ e_vertex, version != nullptr ? VERSION_PRESENT : VERSION_NOT_PRESENT });

         c_index += OFFSET_VERTEX;
         v_backptr++;

         // Fetch its edges
         int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
         while(c_index < e_length){
             edge = get_edge(c_start + c_index);
             version = nullptr;

             if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                 version = get_version(v_start + v_index);
                 v_index += OFFSET_VERSION;
             }

             Update e_edge { /* is vertex ? */ false,
                           /* is insert ? */ version != nullptr && version->is_remove() ? false : true,
                           /* key */ Key {vertex->m_vertex_id, edge->m_destination},
                           /* weight */ edge->get_weight(context)
             };
             history.push_back(PruneHistoryEntry{ e_edge, version != nullptr ? VERSION_PRESENT : VERSION_NOT_PRESENT });

             // next iteration
             c_index += OFFSET_EDGE;
             v_backptr++;
         } // end while, fetch edges
     } // end while, fetch vertices

     return history;
}

void SparseFile::prune_validate_unset_versions(const Context& context, bool is_lhs, PruneHistory& history){
    if(is_empty(is_lhs)) return; // nop

    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict v_start = get_versions_start(is_lhs);
    uint64_t* __restrict v_end = get_versions_end(is_lhs);
    int64_t v_length = v_end - v_start;

    for(int64_t v_index = 0, v_length = v_end - v_start; v_index < v_length; v_index++ ){
        Version* version = get_version(v_start + v_index);
        if(version->get_undo() == nullptr){ // this version has been pruned
            uint64_t c_index = version->get_backptr();
            assert(c_index < history.size() && "overflow");
            assert(history[c_index].m_version == VERSION_PRESENT && "This item did not have a version");
            history[c_index].m_version = VERSION_REMOVED;
        }
    }

    // If the first vertex is dummy, check whether its edges have been removed
    Vertex* vertex = get_vertex(c_start);
    if(vertex->m_first == 0){ // this is a dummy vertex
        int64_t c_index = OFFSET_VERTEX;
        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
        int64_t v_index = 0;
        uint64_t v_backptr = 1; // ignore the first dummy vertex
        bool remove_dummy_vertex = true;

        while(c_index < e_length && remove_dummy_vertex){
            Version* version = nullptr;

            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                version = get_version(v_start + v_index);
                v_index += OFFSET_VERSION;
            }

            remove_dummy_vertex &= version != nullptr && version->get_undo() == nullptr && version->is_remove();

            // next iteration
            c_index += OFFSET_EDGE;
            v_backptr++;
        }

        if(remove_dummy_vertex){
            history[0].m_version = VERSION_REMOVED;

            // Reset the element as a deletion
            assert(history[0].m_element.is_insert());
            history[0].m_element.flip();
            assert(history[0].m_element.is_remove());
        }
    }
}

void SparseFile::prune_validate_check(const Context& context, bool is_lhs, PruneHistory& history, int64_t c_shift, int64_t v_shift){
    // pointers to the static & delta portions of the segment
    uint64_t* c_start = get_content_start(is_lhs);
    uint64_t* c_end = get_content_end(is_lhs);
    uint64_t c_length = c_end - c_start - c_shift;
    uint64_t* v_start = get_versions_start(is_lhs);
    uint64_t* v_end = get_versions_end(is_lhs);
    uint64_t v_length = v_end - v_start - v_shift;
    double* weights = get_weights(context, is_lhs);

    prune_validate_check(context, history, c_start, c_length, weights, v_start, v_length);
}

void SparseFile::prune_validate_check(const Context& context, PruneHistory& history, uint64_t* c_start, int64_t c_length, double* weights, uint64_t* v_start, int64_t v_length){
#if !defined(NDEBUG) // otherwise all assertions are nops
    int64_t c_index = 0;
    int64_t v_index = 0;
    uint64_t v_backptr = 0;
    uint64_t h_index = 0;
    Vertex* vertex = nullptr;
    Edge* edge = nullptr;
    Version* version = nullptr;

    while(c_index < c_length){
        // Fetch a vertex
        vertex = get_vertex(c_start + c_index);
        edge = nullptr;
        version = nullptr;

        if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
            version = get_version(v_start + v_index);
            v_index += OFFSET_VERSION;
        }

        // Validate the vertex in the history
        // Skip removed elements from the history
        while(h_index < history.size() && history[h_index].m_element.key().source() < vertex->m_vertex_id){
            assert(history[h_index].m_version == VERSION_REMOVED);
            assert(history[h_index].m_element.is_remove());
            h_index++;
        }
        assert(h_index < history.size() && "History depleted, but expected to find the vertex");
        assert(history[h_index].m_element.is_vertex() && "Expected to find a vertex");
        assert(history[h_index].m_element.key().source() == vertex->m_vertex_id && "Key mismatch, the entry does not match the vertex fetched");
        assert((history[h_index].m_version != VERSION_REMOVED || history[h_index].m_element.is_insert()) && "This vertex should have been removed from the data section");
        assert(((history[h_index].m_version == VERSION_PRESENT) == (version != nullptr)) && "Expected to find a version");
        assert((version == nullptr || version->get_undo() != nullptr) && "Empty versions should have been removed from the version area");
        h_index++; // next item

        c_index += OFFSET_VERTEX;
        v_backptr++;

        // Fetch its edges
        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
        while(c_index < e_length){
            edge = get_edge(c_start + c_index);
            version = nullptr;

            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                version = get_version(v_start + v_index);
                v_index += OFFSET_VERSION;
            }

            // Validate the vertex in the history
            // Skip removed elements from the history
            while(h_index < history.size() &&
                    (history[h_index].m_element.key().source() < vertex->m_vertex_id ||
                    (history[h_index].m_element.key().source() == vertex->m_vertex_id && history[h_index].m_element.key().destination() < edge->m_destination) )){
                assert(history[h_index].m_element.key().source() <= vertex->m_vertex_id);
                assert(history[h_index].m_version == VERSION_REMOVED);
                assert(history[h_index].m_element.is_remove());
                h_index++;
            }
            assert(h_index < history.size() && "History depleted, but expected to find the edge");
            assert(history[h_index].m_element.is_edge() && "Expected to find an edge");
            assert(history[h_index].m_element.key().source() == vertex->m_vertex_id && "Key mismatch, the entry does not match the edge's source");
            assert(history[h_index].m_element.key().destination() == edge->m_destination  && "Key mismatch, the entry does not match the edge's destination");
            assert((history[h_index].m_version != VERSION_REMOVED || history[h_index].m_element.is_insert()) && "This edge should have been removed from the data section");
            assert(((history[h_index].m_version == VERSION_PRESENT) == (version != nullptr)) && "Expected to find a version");
            assert((version == nullptr || version->get_undo() != nullptr) && "Empty versions should have been removed from the version area");
            h_index ++;

            // next iteration
            c_index += OFFSET_EDGE;
            v_backptr++;
        } // end while, fetch edges
    } // end while, fetch vertices


    assert(v_index == v_length && "We did not fetch all versions");

    // check we visited all items from the history
    while(h_index < history.size()){
        // if these assertions fire, some items were not loaded in the data section
        assert(history[h_index].m_version == VERSION_REMOVED);
        assert(history[h_index].m_element.is_remove());
        h_index++;
    }
#endif
}

/*****************************************************************************
 *                                                                           *
 *   Rebuild vertex table                                                    *
 *                                                                           *
 *****************************************************************************/
void SparseFile::rebuild_vertex_table(Context& context) {
    profiler::ScopedTimer profiler { profiler::SF_REBUILD_VERTEX_TABLE };

    do_rebuild_vertex_table(context, /* lhs ? */ true);
    do_rebuild_vertex_table(context, /* lhs ? */ false);
}

void SparseFile::do_rebuild_vertex_table(Context& context, bool is_lhs){
    VertexTable* vt = context.m_tree->vertex_table();

    uint64_t* __restrict c_start = get_content_start(is_lhs);
    uint64_t* __restrict c_end = get_content_end(is_lhs);
    uint64_t c_index = 0;
    uint64_t c_length = c_end - c_start;
    uint64_t v_backptr = 0;

    while(c_index < c_length){
        Vertex* vertex = get_vertex(c_start + c_index);
        if(vertex->m_first == 1){
            DirectPointer dptr { context, c_index, 0, v_backptr };
            vt->upsert(vertex->m_vertex_id, dptr);
        }

        c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE; // skip the edges altogether
        v_backptr += /* the current vertex */ 1 + /* its attached edges */ vertex->m_count;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
static void print_tabs(std::ostream& out, int tabs){
    auto flags = out.flags();
    out << setw(tabs * 2) << setfill(' ') << ' ';
    out.setf(flags);
}

void SparseFile::dump(Context& context) const {
    dump(context.m_leaf);
}

void SparseFile::dump(const Leaf* leaf) const {
    cout << "[SparseFile] " << ((void*) this) << ", " <<
            "versions1: " << m_versions1_start << ", empty1: " << m_empty1_start << ", " <<
            "empty2: " << m_empty2_start << ", versions2: " << m_versions2_start << ", " <<
            "free space: " << free_space() << " qwords, " <<
            "used space: " << used_space() << " qwords";
    if(context::StaticConfiguration::memstore_duplicate_pivot){
        cout << ", pivot (cached): " << reinterpret_cast<const Key*>(this +1)[0];
    }
    cout << "\n";

    cout << "Left hand side: \n";
    dump_section(cout, /* lhs ? */ true, KEY_MIN, KEY_MAX, leaf, nullptr);

    cout << "Right hand side: \n";
    dump_section(cout, /* lhs ? */ false, KEY_MIN, KEY_MAX, leaf, nullptr);
}

void SparseFile::dump_and_validate(std::ostream& out, Context& context, bool* integrity_check) const {
    // Fence keys
    Key lfkey = Segment::get_lfkey(context);
    Key pivot = get_pivot(context);
    Key hfkey = Segment::get_hfkey(context);

    print_tabs(out, 2);
    out << "Sparse File @ " << ((void*) this) << ", " <<
            "versions1: " << m_versions1_start << ", empty1: " << m_empty1_start << ", " <<
            "empty2: " << m_empty2_start << ", versions2: " << m_versions2_start << ", " <<
            "free space: " << free_space() << " qwords, " <<
            "used space: " << used_space() << " qwords, " <<
            "pivot: " << pivot;
            ;
    if(is_empty()){ out << ", empty"; }
    out << "\n";

    if(!is_empty()){
        print_tabs(out, 2);
        out << "Left hand side: ";
        if(is_lhs_empty()){
            out << "empty\n";
        } else {
            out << "\n";
            dump_section(out, /* lhs ? */ true, lfkey, pivot, context.m_leaf, integrity_check);
        }

        print_tabs(out, 2);
        out << "Right hand side: ";
        if(is_rhs_empty()){
            out << "empty\n";
        } else {
            out << "\n";
            dump_section(out, /* lhs ? */ false, pivot, hfkey, context.m_leaf, integrity_check);
        }
    }
}

void SparseFile::dump_section(std::ostream& out, bool is_lhs, const Key& fence_key_low, const Key& fence_key_high, const Leaf* leaf, bool* integrity_check) const {
    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);

    // iterate over the content section
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    uint64_t v_backptr = 0;
    const Vertex* vertex = nullptr;
    const Edge* edge = nullptr;
    const Version* version = nullptr;

    while(c_index < c_length){
        // Fetch a vertex
        vertex = get_vertex(c_start + c_index);
        edge = nullptr;
        version = nullptr;

        if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
            version = get_version(v_start + v_index);
            v_index += OFFSET_VERSION;
        }

        dump_element(out, v_backptr, vertex, edge, version, leaf, integrity_check);
        dump_validate_key(out, vertex, edge, fence_key_low, fence_key_high, integrity_check);

        c_index += OFFSET_VERTEX;
        v_backptr++;

        // Fetch its edges
        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
        while(c_index < e_length){
            edge = get_edge(c_start + c_index);
            version = nullptr;

            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                version = get_version(v_start + v_index);
                v_index += OFFSET_VERSION;
            }

            dump_element(out, v_backptr, vertex, edge, version, leaf, integrity_check);
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

void SparseFile::dump_element(std::ostream& out, uint64_t position, const Vertex* vertex, const Edge* edge, const Version* version, const Leaf* leaf, bool* integrity_check) {
    print_tabs(out, 3);
    out << "[" << position << "] ";
    if(edge == nullptr){
        out << vertex->to_string(version) << "\n";
    } else {
        out << edge->to_string(vertex, version, leaf) << "\n";
    }

    if(version != nullptr){
        if(version->get_backptr() != position){
            out << "--> ERROR, the back pointer (" << version->to_string() << ") does not match the position of the record (" << position << ")\n";
            if(integrity_check) *integrity_check = false;
        }

        Segment::dump_unfold_undo(out, version->get_undo()); // do not insert a "\n" above
    }
}

void SparseFile::dump_validate_key(std::ostream& out, const Vertex* vertex, const Edge* edge, const Key& fence_key_low, const Key& fence_key_high, bool* integrity_check) {
    Key key { vertex->m_vertex_id, edge != nullptr ? edge->m_destination : 0 };

    if(key < fence_key_low && (edge != nullptr || vertex->m_first == 1)){
        out << "--> ERROR, the key above is lesser than the low fence key: " << fence_key_low << "\n";
        if(integrity_check != nullptr) *integrity_check = false;
    } else if (key >= fence_key_high){
        out << "--> ERROR, the key above is greater or equal than the high fence key: " << fence_key_high << "\n";
        if(integrity_check != nullptr) *integrity_check = false;
    }
}


void SparseFile::dump_after_save(bool is_lhs) const{
    lock_guard<mutex> lock(util::g_debugging_mutex);
    cout << "[" << DEBUG_WHOAMI << "]" << endl;
    cout << "segment: " << (void*) this << ", " <<
            "versions1: " << m_versions1_start << ", empty1: " << m_empty1_start << ", " <<
            "empty2: " << m_empty2_start << ", version2: " << m_versions1_start << ", ";
    if(!is_lhs) {
        // if this were the LHS, we still need to update empty2 and versions2. Invoking #get_segment_free_space
        // could have raised an assertion as these fields would have been inconsistent
        cout << "free space: " << free_space() << " qwords, used space: " << used_space() << ", ";
    }
    cout << (is_lhs ? "lhs" : "rhs") << endl;

    // Iterate over the elements in the file
    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);

    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    uint64_t v_backptr = 0;

    while(c_index < c_length){
        const Vertex* vertex = get_vertex(c_start + c_index);
        const Version* version = nullptr;

        if(v_index < v_length){
            auto candidate = get_version(v_start + v_index);
            if(candidate->get_backptr() == v_backptr){
                version = candidate;
                v_index += OFFSET_VERSION;
            }
        }

        cout << "[" << v_backptr << "] " << vertex->to_string(version) << endl;


        c_index += OFFSET_VERTEX;
        v_backptr++;

        uint64_t e_pos = 0;
        uint64_t e_len = vertex->m_count;
        while(c_index < c_length && e_pos < e_len){
            const Edge* edge = get_edge(c_start + c_index);
            version = nullptr;

            if(v_index < v_length){
                auto candidate = get_version(v_start + v_index);
                if(candidate->get_backptr() == v_backptr){
                    version = candidate;
                    v_index += OFFSET_VERSION;
                }
            }

            cout << "[" << v_backptr << "] " << edge->to_string(vertex, version) << endl;

            e_pos ++;
            c_index += OFFSET_EDGE;
            v_backptr++;
        }
    }
}


/*****************************************************************************
 *                                                                           *
 *   Validate                                                                *
 *                                                                           *
 *****************************************************************************/
void SparseFile::do_validate(Context& context) const {
    profiler::ScopedTimer profiler { profiler::SF_VALIDATE };

    // Fence keys
    Key lfkey = Segment::get_lfkey(context);
    Key pivot = get_pivot(context);
    Key hfkey = Segment::get_hfkey(context);

    assert(get_pivot(context) == fetch_pivot(context) && "Pivot mismatch");
    assert((lfkey < pivot || (pivot == KEY_MAX && is_lhs_empty())) && "The pivot must be greater or equal than the min fence key");
    assert((pivot <= hfkey || (pivot == KEY_MAX && is_rhs_empty())) && "The pivot must be smaller than the max fence key");

    do_validate_impl(context, /* lhs ? */ true, lfkey, pivot);
    do_validate_impl(context, /* lhs ? */ false, pivot, hfkey);
}

void SparseFile::do_validate_impl(Context& context, bool is_lhs, const Key& fence_key_low, const Key& fence_key_high) const {
    Key key = fence_key_low;

    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);

    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    uint64_t v_backptr = 0;
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    while(c_index < c_length){
        const Vertex* vertex = get_vertex(c_start + c_index);
        assert((vertex->m_first == 1 || vertex->m_count > 0) && "Dummy vertices must contain edges attached");
        assert(((Key(vertex->m_vertex_id) > key) || (c_index == 0 && Key(vertex->m_vertex_id) == key) || (vertex->m_first == 0 && vertex->m_vertex_id == key.source())) && "Order not respected");

        if(v_index < v_length && get_version(v_start + v_index)->m_backptr == v_backptr){
            const Version* version = get_version(v_start + v_index);
            vertex->validate(version);
            v_index += OFFSET_VERSION;
        }

        key = vertex->m_vertex_id;
        c_index += OFFSET_VERTEX;
        v_backptr++;

        int64_t e_length = c_index + vertex->m_count * OFFSET_EDGE;
        while(c_index < e_length){
            const Edge* edge = get_edge(c_start + c_index);
            Key next { vertex->m_vertex_id, edge->m_destination };
            assert((next > key || (next.destination() == 0 && key.destination() == 0 && next.source() == key.source())) && "Order not respected");

            if(v_index < v_length && get_version(v_start + v_index)->m_backptr == v_backptr){
                const Version* version = get_version(v_start + v_index);
                edge->validate(vertex, version);
                v_index += OFFSET_VERSION;
            }

            key = next;
            c_index += OFFSET_EDGE;
            v_backptr++;
        }
    }

    assert(v_index == v_length && "Not all version have been inspected");
    assert((is_lhs && m_empty1_start == (c_length + v_length)) ||
            (!is_lhs && ((int64_t) (max_num_qwords() - m_empty2_start) == (c_length + v_length))));
}

void SparseFile::do_validate_vertex_table(Context& context, bool is_lhs, bool is_prune) const {
    constexpr uint64_t num_nodes = context::StaticConfiguration::numa_num_nodes;
    VertexTable* vt = context.m_tree->vertex_table();

    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    int64_t backptr = 0;
    while(c_index < c_length){
        const Vertex* vertex = get_vertex(c_start + c_index);
        for(uint64_t node = 0; node < num_nodes; node ++){
            DirectPointer dptr = vt->get(vertex->m_vertex_id, node);
            if(vertex->m_first){
                if(is_prune || /* is prune = false */ dptr.leaf() != nullptr){
                    assert(dptr.leaf() == context.m_leaf);
                    assert(dptr.segment() == context.m_segment);
                    assert(dptr.get_segment_version() == context.m_segment->get_version() +1); // as #prune did not left the segment yet
                    assert(dptr.has_filepos() == true);
                    uint64_t pos_vertex {0}, pos_edge {0}, pos_backptr {0};
                    dptr.get_filepos(&pos_vertex, &pos_edge, &pos_backptr);
                    assert((int64_t) pos_vertex == c_index);
                    assert(pos_edge == 0);
                    assert((int64_t) pos_backptr == backptr);
                } else { // invalid pointer
                    assert(dptr.leaf() == nullptr);
                    assert(dptr.segment() == nullptr);
                    assert(dptr.has_filepos() == false);
                }
            } else { // dummy vertex
                // if lhs == true, then it must point to a different (previous) segment
                assert(is_lhs == false || /* lhs == true -> */ dptr.segment() != context.m_segment);
            }
        }

        // next vertex
        c_index += OFFSET_VERTEX + vertex->m_count * OFFSET_EDGE;
        backptr += 1 + vertex->m_count;
    }
}

void SparseFile::validate_scratchpad(Context& context, rebalance::ScratchPad& scratchpad, int64_t& pos_next_vertex, int64_t& pos_next_element, const Update* update, bool* out_update_processed){
#if !defined(NDEBUG)
    for(int left_and_right = 1; left_and_right >= 0; left_and_right--){
        const bool is_lhs = (bool) left_and_right;
        uint64_t* __restrict c_start = get_content_start(is_lhs);
        uint64_t* __restrict c_end = get_content_end(is_lhs);
        uint64_t* __restrict v_start = get_versions_start(is_lhs);
        uint64_t* __restrict v_end = get_versions_end(is_lhs);

        // iterate over the content section
        int64_t c_index = 0;
        int64_t c_length = c_end - c_start;
        int64_t v_index = 0;
        int64_t v_length = v_end - v_start;
        uint64_t v_backptr = 0;

        while(c_index < c_length){
            // Fetch a sf_vertex
            Vertex* sf_vertex = get_vertex(c_start + c_index);
            Version* sf_version = nullptr;
            Vertex* sp_vertex = pos_next_vertex < static_cast<int64_t>(scratchpad.size()) ? scratchpad.get_vertex(pos_next_vertex) : nullptr;

            // Validate the version
            if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                sf_version = get_version(v_start + v_index);
                sf_vertex->validate(sf_version);
                v_index += OFFSET_VERSION;
            }

            // Validate the vertex
            if(update != nullptr && update->is_insert() && update->is_vertex() && update->source() == sf_vertex->m_vertex_id){ // this is the update
                assert((sp_vertex == nullptr || (pos_next_vertex == pos_next_element)) && "There are edges not processed in the scratchpad");
                assert(sf_version != nullptr && "(Vertex, insert) Because this is an update, it must have a version");
                assert(sf_version->is_insert() == update->is_insert() && "(Vertex, insert) The operation in the version does not match the operation in the update");

                // The vertex may or may not be present in the scratchpad. If it is present, it must be flagged as a deletion
                if(sp_vertex != nullptr && sp_vertex->m_vertex_id == sf_vertex->m_vertex_id){
                    Version* sp_version = scratchpad.get_version(pos_next_vertex);
                    assert(sp_version != nullptr && "The vertex in the scratchpad should have a version (deletion)");
                    assert(sp_version->is_remove() && "The last version in the scratchpad should be an vertex deletion");
                    if(pos_next_element == pos_next_vertex) pos_next_element++;
                } else {
                    assert((sp_vertex == nullptr || update->source() < sp_vertex->m_vertex_id) && "Sorted order not respected, expected to be placed afterwards");
                }

                // Because we just inserted this vertex, it cannot have any edges attached. If it has, they must be all deletions from older transactions.
                uint64_t e_index = c_index + OFFSET_VERTEX;
                uint64_t e_length = e_index + sf_vertex->m_count;
                int64_t ev_index = v_index ;
                uint64_t ev_backptr = v_backptr +1;
                while(e_index < e_length){
                    assert(ev_index < v_length && "This edge is expected to be a deletion, but it doesn't have a version attached");
                    Version* e_version = get_version(v_start + ev_index);
                    assert(e_version->get_backptr() == ev_backptr && "The version does not refer to the examined edge");
                    assert(e_version->is_remove() && "Because this vertex has just been inserted, all its present edges must be deletions from older transactions");

                    e_index += OFFSET_EDGE;
                    ev_index++;
                    ev_backptr++;
                }

                if(out_update_processed != nullptr) { *out_update_processed = true; }
                update = nullptr;
            } else if (sf_vertex->m_first == 0 && sf_vertex->m_count == 1 && update != nullptr && update->is_insert() && update->is_edge() && update->source() == sf_vertex->m_vertex_id && (sp_vertex == nullptr || sp_vertex->m_vertex_id > sf_vertex->m_vertex_id)){
                // corner case: this is a dummy vertex, inserted as a consequence of the edge just inserted. This vertex is not present in the scratchpad.
                c_index += OFFSET_VERTEX; // skip the dummy vertex
                v_backptr ++; // skip the vertex

                assert(c_index < c_length && "Expected to find the newly inserted edge");
                Edge* sf_edge = get_edge(c_start + c_index);
                assert(sf_edge->m_destination == update->destination() && "This should be the edge just inserted");

                assert(v_index < v_length && "Expected a version for the newly inserted edge");
                assert(get_version(v_start + v_index)->get_backptr() == v_backptr && "Expected a version for the newly inserted edge");
                sf_version = get_version(v_start + v_index);
                assert(sf_version != nullptr && sf_version->is_insert() && "The version should refer to the newly inserted edge");
                sf_edge->validate(sf_vertex, sf_version);
                v_index += OFFSET_VERSION;
                c_index += OFFSET_EDGE;
                v_backptr++; // skip the edge

                if(out_update_processed != nullptr) { *out_update_processed = true; }
                update = nullptr;

                continue; // next item
            } else {
                if(update != nullptr && update->is_remove() && update->is_vertex() && update->source() == sf_vertex->m_vertex_id){
                    assert(sf_version != nullptr && "(Vertex, remove) Because this is an update, it must have a version");
                    assert(sf_version->is_remove() && "(Vertex, remove) The operation in the version does not match the operation in the update");

                    if(out_update_processed != nullptr) { *out_update_processed = true; }
                    update = nullptr;
                }

                assert((sp_vertex != nullptr && sp_vertex->m_vertex_id == sf_vertex->m_vertex_id) && "The two vertices must match");
                if(pos_next_element == pos_next_vertex) pos_next_element++;
            }

            if(sp_vertex != nullptr && pos_next_element - pos_next_vertex > static_cast<int64_t>(sp_vertex->m_count)){ // next vertex from the scratchpad
                pos_next_vertex = pos_next_element;
            }
            c_index += OFFSET_VERTEX;
            v_backptr++;

            // Fetch its edges
            int64_t e_length = c_index + sf_vertex->m_count * OFFSET_EDGE;
            while(c_index < e_length){
                Edge* sf_edge = get_edge(c_start + c_index);
                sf_version = nullptr;
                rebalance::WeightedEdge* sp_edge = (pos_next_vertex < pos_next_element /* otherwise it's a vertex */ && pos_next_element < static_cast<int64_t>(scratchpad.size())) ? scratchpad.get_edge(pos_next_element) : nullptr;

                // Validate the version
                if(v_index < v_length && get_version(v_start + v_index)->get_backptr() == v_backptr){
                    sf_version = get_version(v_start + v_index);
                    sf_edge->validate(sf_vertex, sf_version);
                    v_index += OFFSET_VERSION;
                }

                // Validate the edge
                if(update != nullptr && update->is_insert() && update->is_edge() && update->source() == sf_vertex->m_vertex_id && update->destination() == sf_edge->m_destination){ // this is the update
                    assert(sf_version != nullptr && "Because this edge has been just inserted");
                    assert(sf_version->is_insert() && "Because the update is an insert");

                    // The edge may or may not be present in the scratchpad. If it is present, it must be flagged as a deletion
                    if(sp_edge != nullptr && sp_vertex->m_vertex_id == sf_vertex->m_vertex_id && sp_edge->m_destination == update->destination()){
                        Version* sp_version = scratchpad.get_version(pos_next_element);
                        assert(sp_version != nullptr && "The edge in the scratchpad should have a version (deletion)");
                        assert(sp_version->is_remove() && "The last version in the scratchpad should be an edge deletion");
                        pos_next_element++;
                    } else {
                        assert((sp_edge == nullptr || update->destination() < sp_edge->m_destination) && "Sorted order not respected");
                    }

                    if(out_update_processed != nullptr) { *out_update_processed = true; }
                    update = nullptr;
                } else {
                    assert(sp_edge != nullptr && "This edge is not in the scratchpad");
                    assert(sf_edge->m_destination == sp_edge->m_destination && "The edges do not match");

                    if(update != nullptr && update->is_remove() && update->is_edge() && update->source() == sf_vertex->m_vertex_id && update->destination() == sf_edge->m_destination){
                        assert(sf_version != nullptr && "(Edge, remove) Because this is an update, it must have a version");
                        assert(sf_version->is_remove() && "(Edge, remove) The operation in the version does not match the operation in the update");

                        if(out_update_processed != nullptr) { *out_update_processed = true; }
                        update = nullptr;
                    }

                    pos_next_element++;
                }

                if(sp_vertex != nullptr && pos_next_element - pos_next_vertex > static_cast<int64_t>(sp_vertex->m_count)){
                    pos_next_vertex = pos_next_element;
                }

                // next iteration
                c_index += OFFSET_EDGE;
                v_backptr++;
            } // end while, fetch edges
        }
    }
#endif
}

} // namespace


