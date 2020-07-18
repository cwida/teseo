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
#include <limits>


#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/cursor_state.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/memstore/vertex_table.hpp"
#include "teseo/profiler/direct_access.hpp"
#include "teseo/util/interface.hpp"

//#define DEBUG
//#include "teseo/util/debug.hpp"

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Memstore                                                                *
 *                                                                           *
*****************************************************************************/
template<typename Callback>
void Memstore::scan(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, Callback&& callback) const {
    scan(transaction, source, destination, nullptr, callback);
}

template<typename Callback>
void Memstore::scan(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, CursorState* cs, Callback&& callback) const {
    PROFILE_DIRECT_ACCESS ( memstore_invocations );
    Context context { const_cast<Memstore*>(this), transaction };
    Key key { source, destination };
    bool done = false;
    DirectPointer directptr;
    context::ScopedEpoch epoch; // protect from the GC

    // entry pointer
    bool acquire_latch = true;
    if(cs != nullptr && cs->is_valid()) { // cursor state
        PROFILE_DIRECT_ACCESS( memstore_cs_present );
        if(cs->key() == key){ // match
            PROFILE_DIRECT_ACCESS( memstore_cs_key_match );
            acquire_latch = false;
            directptr = cs->position();
        } else if (cs->position().leaf()->check_fence_keys(cs->position().get_segment_id(), key) == FenceKeysDirection::OK){ // fence keys ok
            PROFILE_DIRECT_ACCESS( memstore_cs_fkeys_match );
            acquire_latch = false;

//            if(destination == 0){ // -> it's a vertex
//                DirectPointer ptr = vertex_table()->get(source, context::thread_context()->numa_node());
//                if(ptr.leaf() == cs->position().leaf() && ptr.get_segment_id() == cs->position().get_segment_id() && ptr.get_segment_version() == ptr.segment()->get_version()){
//                    PROFILE_DIRECT_ACCESS( memstore_cs_dptr_match );
//                    directptr = ptr;
//                }
//            }
//
//            if(!directptr.has_filepos()){
//                PROFILE_DIRECT_ACCESS( memstore_cs_no_filepos );
                directptr = cs->position();
                directptr.unset_filepos();
//            }
        } else { // close the cursor & release the held latch
            PROFILE_DIRECT_ACCESS( memstore_cs_no_match );
            cs->close();
        }
    }
    if (directptr.leaf() == nullptr && destination == 0){ // vertex table
        PROFILE_DIRECT_ACCESS( memstore_vt_lookups );
        DirectPointer ptr = vertex_table()->get(source, context::thread_context()->numa_node());
        if(ptr.leaf() != nullptr && ptr.leaf()->check_fence_keys(ptr.get_segment_id(), key) == FenceKeysDirection::OK){
            PROFILE_DIRECT_ACCESS( memstore_vt_fkeys_match );
            directptr = ptr;
            if(ptr.get_segment_version() != ptr.segment()->get_version()){
                PROFILE_DIRECT_ACCESS( memstore_vt_invalid_filepos );
                directptr.unset_filepos();
            }
        }
    }

    // scan
    do {
        try {

            if(acquire_latch){ // first time
                context.reader_direct_access(key, directptr);
            } else {
                directptr.restore_context(&context);
                acquire_latch = true; // next time
            }

            done = ! Segment::scan(context, key, &directptr, cs, callback);
            directptr.unset(); // consumed

            while(!done){
                context.reader_next(key);
                done = ! Segment::scan(context, key, nullptr, cs, callback);
            }

            if(cs == nullptr || !cs->is_valid()){
                context.reader_exit();
            }

        } catch(Abort){
            /* nop, segment being rebalanced in the meanwhile, retry ...  */
            directptr.unset(); // consumed

            assert(context.m_segment == nullptr && "This exception was not raised while accessing the segment");
        } catch ( ... ) {
            if(cs != nullptr) cs->invalidate();
            if(context.m_segment != nullptr){ context.reader_exit(); } // release the lock on the segment

            throw;
        }
    } while (!done);


    if(key == KEY_MAX && cs != nullptr){
        cs->close();
    }
}

template<typename Callback>
void Memstore::scan_nolock(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, Callback&& callback) const {
    Context context { const_cast<Memstore*>(this), transaction };
    Key key { source, destination };
    bool done = false;

    do {
        context::ScopedEpoch epoch;

        try {
            context.optimistic_enter(key);
            done = ! Segment::scan(context, key, nullptr, nullptr, callback);

            while(!done){ // move to the next segment
                context.optimistic_next(key);
                done = ! Segment::scan(context, key, nullptr, nullptr, callback);
            }

            context.optimistic_reset();
        } catch ( Abort ) {
            /* retry ... */
            context.optimistic_reset();
        } catch ( ... ){
            throw;
        }

    } while(!done);
}

/*****************************************************************************
 *                                                                           *
 *   Segment                                                                 *
 *                                                                           *
 *****************************************************************************/
template<typename Callback>
bool Segment::scan(Context& context, Key& next, DirectPointer* state_load, CursorState* state_save, Callback&& callback){
    Segment* segment = context.m_segment;
    auto hfkey = Segment::get_hfkey(context);
    bool read_next = true; // move to the next segment ?

    if(segment->is_sparse()){
        read_next = sparse_file(context)->scan(context, next, state_load, state_save, callback);
    } else {
        assert(state_load == nullptr || state_load->has_filepos() == false); // dense files must have an invalid entry pointer
        if(state_save != nullptr){ state_save->invalidate(); }
        read_next = dense_file(context)->scan(context, next, callback);
    }

    // do not validate when read_next == false, we need to terminate the scan!
    if(read_next){
        context.validate_version_if_present(); // before setting the next key check our result is correct
        next = hfkey;
        read_next = (hfkey != KEY_MAX); // otherwise, we're done
    }

    return read_next;
}

/*****************************************************************************
 *                                                                           *
 *   Sparse file                                                             *
 *                                                                           *
 *****************************************************************************/
template<bool is_optimistic, typename Callback>
bool SparseFile::scan_impl(Context& context, bool is_lhs, Key& next, DirectPointer* state_load, CursorState* state_save, Callback&& callback) const {
    // if the degree of a vertex spans over multiple segments and a rebalance occurred in the meanwhile, there is the
    // possibility we may re-read edges we have already visited before the rebalance occurred. In this case, simply
    // skip those edges
    const uint64_t vertex_id = next.source();
    const uint64_t min_destination = next.destination();

    // pointers to the static & delta portions of the segment
    const uint64_t* __restrict c_start = get_content_start(is_lhs);
    const uint64_t* __restrict c_end = get_content_end(is_lhs);
    const uint64_t* __restrict v_start = get_versions_start(is_lhs);
    const uint64_t* __restrict v_end = get_versions_end(is_lhs);
    if(is_optimistic) context.validate_version(); // check these pointers are valid

    // find the starting point in the segment
    uint64_t v_backptr = 0; // it seems redundant, as it equal to current_position / OFFSET_ELEMENT
    int64_t c_index_vertex = 0;
    int64_t c_index_edge = 0;
    int64_t c_length = c_end - c_start;
    int64_t e_length = 0;
    const Vertex* vertex = nullptr;
    bool starting_point_found = false;
    if(state_load == nullptr || !state_load->has_filepos()){ // search the starting point in the segment
        while(c_index_vertex < c_length && !starting_point_found){
            vertex = get_vertex(c_start + c_index_vertex);
            if(vertex->m_vertex_id < vertex_id){
                c_index_vertex += OFFSET_ELEMENT + vertex->m_count * OFFSET_ELEMENT; // skip the edges altogether
                v_backptr += 1 + vertex->m_count;
            } else {
                if(vertex_id == vertex->m_vertex_id && min_destination > 0){
                    c_index_edge = c_index_vertex + OFFSET_ELEMENT;
                    e_length = c_index_edge + vertex->m_count * OFFSET_ELEMENT;
                    if(is_optimistic && e_length > c_length){ context.validate_version(); } // overflow
                    v_backptr++; // skip the vertex

                    // find the starting edge
                    while(c_index_edge < e_length && !starting_point_found){
                        const Edge* edge = get_edge(c_start + c_index_edge);
                        if(edge->m_destination < min_destination){
                            c_index_edge += OFFSET_ELEMENT;
                            v_backptr++;
                        } else {
                            starting_point_found = true;
                        }
                    }

                    // next iteration
                    if(!starting_point_found){
                        c_index_vertex = e_length;
                        c_index_edge = e_length = 0;
                    }
                } else {
                    starting_point_found = true;
                    c_index_edge = e_length = 0;
                }
            }
        }
    } else { // restore the cursor state
        assert(state_load != nullptr && "Direct pointer is null");
        assert(state_load->has_filepos() && "The pointer does not have a filepos");
        assert(is_optimistic == false && "The cursor state can be utilised only with regular (non optimistic) readers");

        starting_point_found = true;
        state_load->get_filepos((uint64_t*) &c_index_vertex, (uint64_t*) &c_index_edge, &v_backptr);
        state_load->unset_filepos(); // pointer consumed, avoid loading it in the RHS

        vertex = get_vertex(c_start + c_index_vertex);
#if !defined(NDEBUG)
        assert(vertex->m_vertex_id == next.source() && "Vertex (source) mismatch");
        if(next.destination() != 0){
            assert(c_index_edge > c_index_vertex);
            assert(c_index_edge < (int64_t) std::numeric_limits<uint16_t>::max() && "uint16_t::max() is the flag used to mark invalid edges");
            const Edge* edge = get_edge(c_start + c_index_edge);
            assert(edge->m_destination == next.destination() && "Destination mismatch");
            assert(is_dirty(is_lhs) == false || v_backptr == (uint64_t) c_index_edge / 2); // clean segments do not use the backptr
        } else {
            assert(is_dirty(is_lhs) == false || v_backptr == (uint64_t) c_index_vertex / 2); // clean segments do not use the backptr
        }
#endif
    }

    if(state_save != nullptr){
        state_save->invalidate();
    }


    bool read_next = true;
    if(starting_point_found){ // start processing the segment
        const bool is_dirty = v_start != v_end;
        assert(vertex != nullptr);
        uint64_t source = vertex->m_vertex_id;

        if(is_dirty){
            // starting version
            int64_t v_index = 0;
            int64_t v_length = v_end - v_start;
            while(v_index < v_length && get_version(v_start + v_index)->get_backptr() < v_backptr) v_index++;
            uint64_t v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : std::numeric_limits<uint64_t>::max();

            while(read_next && c_index_vertex < c_length){

                // process a vertex
                if(c_index_edge >= e_length){
                    vertex = get_vertex(c_start + c_index_vertex);
                    source = vertex->m_vertex_id;
                    bool is_first = vertex->m_first;
                    c_index_edge = c_index_vertex + OFFSET_ELEMENT;
                    e_length = c_index_edge + vertex->m_count * OFFSET_ELEMENT;
                    if(is_optimistic && e_length > c_length){ context.validate_version(); } // overflow

                    if(is_first){
                        // retrieve the version (if present)
                        const Version* version = nullptr;
                        if(v_backptr == v_next){
                            version = get_version(v_start + v_index);

                            // next iteration
                            v_index ++;
                            v_next = (v_index < v_length) ? get_version(v_start + v_index)->get_backptr() : std::numeric_limits<uint64_t>::max();
                        }

                        if(version != nullptr){
                            Update update = Update::read_delta(context, vertex, nullptr, version);
                            assert(update.is_vertex() && "Expected a vertex");
                            assert(update.source() == source && "Vertex mismatch");

                            if(update.is_insert()){
                                read_next = callback(source, 0, 0);
                            }
                        } else {
                            if(is_optimistic){ context.validate_version(); } // always before invoking the callback
                            read_next = callback(source, 0 , 0);
                        }

                        if(is_optimistic) { next = Key{ source }.successor(); };
                    }
                    v_backptr++;
                }

                // process an edge
                while(read_next && c_index_edge < e_length){
                    const Edge* edge = get_edge(c_start + c_index_edge);
                    uint64_t destination = edge->m_destination;

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
                        assert(update.source() == source && "source mismatch");
                        assert(update.destination() == destination && "destination mismatch");
                        if(update.is_insert()){
                            read_next = callback( source, destination, update.weight() );
                        }
                    } else {
                        double weight = edge->m_weight;
                        if(is_optimistic){ context.validate_version(); } // always before invoking the callback
                        read_next = callback(source, destination, weight);
                    }

                    // next iteration
                    if(is_optimistic) { next = Key{ source, destination }.successor(); }
                    c_index_edge += OFFSET_ELEMENT;
                    v_backptr++;
                }

                // next iteration
                c_index_vertex = c_index_edge;
            } // read_next
        } else {
            while(read_next && c_index_vertex < c_length){
                // process a vertex
                if(c_index_edge >= e_length){
                    vertex = get_vertex(c_start + c_index_vertex);
                    source = vertex->m_vertex_id;
                    const bool is_first = vertex->m_first;

                    if(is_first){
                        read_next = callback( source, 0, 0 );
                        if(is_optimistic) { next = Key{ source }.successor(); }
                    }

                    if(read_next){ // next item
                        c_index_edge = c_index_vertex + OFFSET_ELEMENT;
                        e_length = c_index_edge + vertex->m_count * OFFSET_ELEMENT;
                        if(is_optimistic){ context.validate_version(); }
                    } else { // the position to record for the cursor state
                        c_index_edge = std::numeric_limits<uint16_t>::max(); // avoid overflows when invoking state_save#set_filepos()
                    }
                }

                // process an edge
                while(read_next && c_index_edge < e_length){
                    const Edge* edge = get_edge(c_start + c_index_edge);
                    uint64_t destination = edge->m_destination;
                    double weight = edge->m_weight;
                    if(is_optimistic){ context.validate_version(); }

                    read_next = callback(source, destination, weight);

                    // We can set `next' before or after invoking the callback. Here it can help
                    // a little bit more with debugging as we can use the callback to catch errors.
                    if(is_optimistic) { next = Key{ source, destination }.successor(); }

                    // next iteration
                    if(read_next){ c_index_edge += OFFSET_ELEMENT; }
                }

                // next iteration
                if(read_next) { c_index_vertex = c_index_edge; }

            }  // read_next

            if(state_save != nullptr && !read_next && c_index_vertex < c_length){ // cursor state
                uint64_t source = vertex->m_vertex_id;
                uint64_t destination = 0;
                if(c_index_edge < e_length){
                    destination = get_edge(c_start + c_index_edge)->m_destination;
                }

                state_save->key() = Key { source, destination };
                state_save->position().set_context( context );
                state_save->position().set_filepos(c_index_vertex, c_index_edge, 0);
            }

        }
    } // starting_point_found

    return read_next;
}

template<typename Callback>
bool SparseFile::scan(Context& context, Key& next, DirectPointer* state_load, CursorState* state_save, Callback&& callback){
    const bool is_optimistic = context.has_version();

    bool read_next = true;
    Key pivot = get_pivot(context);

    if(next < pivot){ // visit the lhs
        read_next = is_optimistic ?
                scan_impl</* optimistic ? */ true>(context, /* lhs ? */ true, next, state_load, state_save, callback) :
                scan_impl</* optimistic ? */ false>(context, /* lhs ? */ true, next, state_load, state_save, callback);
    }


    if(read_next){ // visit the rhs
        read_next = is_optimistic ?
                scan_impl</* optimistic ? */ true>(context, /* lhs ? */ false, next, state_load, state_save, callback) :
                scan_impl</* optimistic ? */ false>(context, /* lhs ? */ false, next, state_load, state_save, callback);

    }

    return read_next;
}

/*****************************************************************************
 *                                                                           *
 *   Dense file                                                              *
 *                                                                           *
 *****************************************************************************/

template<typename Callback>
void DenseFile::scan_internal(Context& context, const Key& key, Callback&& cb) const {
    if(context.has_version()){ // optimistic
        Node* root = m_root;
        context.validate_version(); // check root is a valid pointer
        do_scan_node<true>(context, key, root, 0, cb);
    } else { // locked
        do_scan_node<false>(context, key, m_root, 0, cb);
    }
}

template<bool is_optimistic, typename Callback>
bool DenseFile::do_scan_node(Context& context, const Key& key, Node* node, int level, Callback&& cb) const {
    auto prefix_result = node->prefix_compare<is_optimistic>(context, this, key, level);
    if(is_optimistic) { context.validate_version(); }

    switch(prefix_result){
    case -1: {
        // counterintuitively, it means that the prefix of the node is lesser than the key
        // i.e. the key is bigger than any element in this node
        return true;
    } break;
    case 0: {
        bool keep_going = true;
        Node* child = node->get_child(key[level]);
        if(is_optimistic){ context.validate_version(); } // `child' is safe
        if(child != nullptr){
            if(is_leaf(child)){
                Leaf leaf = node2leaf(child);
                const DataItem* data_item = leaf2di(leaf);
                if(is_optimistic){ context.validate_version(); } // before jumping to any pointer, validate it is still valid
                Key key2 { data_item->m_update.key().source(), data_item->m_update.key().destination() };
                if(key <= key2){
                    keep_going = do_scan_leaf<is_optimistic>(context, leaf, cb);
                } else {
                    keep_going = true; // backtrack
                }
            } else {
                keep_going = do_scan_node<is_optimistic>(context, key, child, level +1, cb);
            }
        }
        if(keep_going){
            NodeList list = node->children_gt(key[level]);
            if(is_optimistic) { context.validate_version(); } // the pointers in the list are all safe
            uint64_t i = 0;
            while(keep_going && i < list.m_size){
                keep_going = do_scan_everything<is_optimistic>(context, list.m_nodes[i], cb);
                i++;
            }
        }

        return keep_going;
    } break;
    case +1: {
        // counterintuitively, it means that the prefix of the node is greater than the key
        // ask the parent to return the max for the sibling that precedes this node
        return do_scan_everything<is_optimistic>(context, node, cb);
    } break;
    default:
        assert(0 && "Invalid case");
        return true;
    } // end switch
}

template<bool is_optimistic, typename Callback>
bool DenseFile::do_scan_everything(Context& context, Node* node, Callback&& cb) const {
    if(is_leaf(node)){
        return do_scan_leaf<is_optimistic>(context, node2leaf(node), cb);
    } else {
        NodeList children = node->children();
        if(is_optimistic){ context.validate_version(); } // all pointers in `children' are safe
        uint64_t i = 0;
        bool keep_going = true;
        while(i < children.m_size && keep_going){
            keep_going = do_scan_everything<is_optimistic>(context, children.m_nodes[i], cb);
            i++;
        }
        return keep_going;
    }
}

template<bool is_optimistic, typename Callback>
bool DenseFile::do_scan_leaf(Context& context, Leaf leaf, Callback&& cb) const {
    const DataItem* di = leaf2di( leaf );
    if(is_optimistic) { context.validate_version(); } // before jumping to any pointer, validate it is still valid
    if(di->m_update.is_empty()){ // ignore this data item
        return true;
    } else {
        return cb(di);
    }
}

template<typename Callback>
bool DenseFile::scan(Context& context, memstore::Key& next, Callback&& callback) {
    bool read_next = true;
    const bool is_optimistic = context.has_version();

    auto visitor_cb = [&context, &read_next, &next, &callback, is_optimistic](const DataItem* const_data_item){
        // make a copy
        DataItem data_item = *const_data_item;
        if(is_optimistic) context.validate_version();

        if(data_item.m_update.is_empty()){
            next = data_item.m_update.key().successor();
            return true;
        }

        Update update = Update::read_delta(context, &data_item);
        if(update.is_insert()){
            if(update.is_vertex()){
                read_next = callback(update.source(), 0, 0);
            } else { // process an edge
                read_next = callback(update.source(), update.destination(), update.weight());
            }
        }

        next = update.key().successor();
        return read_next;
    };

    Key key_start { next.source(), next.destination() };
    scan_internal(context, key_start, visitor_cb);
    return read_next;
}

} // namespace
