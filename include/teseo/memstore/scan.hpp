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
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
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
    Context context { const_cast<Memstore*>(this), transaction };
    Key key { source, destination };
    bool done = false;

    do {
        context::ScopedEpoch epoch;

        try {
            context.reader_enter(key);
            done = ! Segment::scan(context, key, callback);

            while(!done){
                context.reader_next(key);
                done = ! Segment::scan(context, key, callback);
            }

            context.reader_exit();
        } catch ( Abort ) {
            /* nop, segment being rebalanced in the meanwhile, retry ...  */
        } catch ( ... ){
            if(context.m_segment != nullptr){ context.reader_exit(); } // release the lock on the segment
            throw;
        }

    } while(!done);
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
            done = ! Segment::scan(context, key, callback);

            while(!done){ // move to the next segment
                context.optimistic_next(key);
                done = ! Segment::scan(context, key, callback);
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
bool Segment::scan(Context& context, Key& next, Callback&& callback){
    Segment* segment = context.m_segment;
    auto hfkey = Segment::get_hfkey(context);
    bool read_next = true; // move to the next segment ?

    if(segment->is_sparse()){
        read_next = sparse_file(context)->scan(context, next, callback);
    } else {
        read_next = dense_file(context)->scan(context, next, callback);
    }

    if(context.has_version()) { context.validate_version(); } // before setting the next key check our result is correct
    next = hfkey;

    if(next == KEY_MAX){ // we're done
        read_next = false;
    }

    return read_next;
}

/*****************************************************************************
 *                                                                           *
 *   Sparse file                                                             *
 *                                                                           *
 *****************************************************************************/
template<bool is_optimistic, typename Callback>
bool SparseFile::scan_impl(Context& context, bool is_lhs, Key& next, Callback&& callback) const {
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
    uint64_t v_backptr = 0;
    int64_t c_index_vertex = 0;
    int64_t c_index_edge = 0;
    int64_t c_length = c_end - c_start;
    int64_t e_length = 0;
    const Vertex* vertex = nullptr;
    bool starting_point_found = false;
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

    bool read_next = true;
    if(starting_point_found){ // start processing the segment
        const bool is_dirty = v_start != v_end;
        uint64_t source = vertex != nullptr ? vertex->m_vertex_id : 0;

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
                    c_index_edge = c_index_vertex + OFFSET_ELEMENT;
                    e_length = c_index_edge + vertex->m_count * OFFSET_ELEMENT;
                    if(is_optimistic){ context.validate_version(); }

                    if(is_first){
                        if(is_optimistic) { next = Key{ source }.successor(); }
                        read_next = callback( source, 0, 0 );
                    }
                }

                // process an edge
                while(read_next && c_index_edge < e_length){
                    const Edge* edge = get_edge(c_start + c_index_edge);
                    uint64_t destination = edge->m_destination;
                    double weight = edge->m_weight;
                    if(is_optimistic){
                        context.validate_version();
                        next = Key{ source, destination }.successor();
                    }

                    read_next = callback( source, destination, weight);

                    // next iteration
                    c_index_edge += OFFSET_ELEMENT;
                }


                // next iteration
                c_index_vertex = c_index_edge;
            }
        } // read_next
    } // starting_point_found

    return read_next;
}

template<typename Callback>
bool SparseFile::scan(Context& context, Key& next, Callback&& callback){
    const bool is_optimistic = context.has_version();

    bool read_next = true;
    Key pivot = get_pivot(context);

    if(next < pivot){ // visit the lhs
        read_next = is_optimistic ?
                scan_impl</* optimistic ? */ true>(context, /* lhs ? */ true, next, callback) :
                scan_impl</* optimistic ? */ false>(context, /* lhs ? */ true, next, callback);
    }


    if(read_next){ // visit the rhs
        read_next = is_optimistic ?
                scan_impl</* optimistic ? */ true>(context, /* lhs ? */ false, next, callback) :
                scan_impl</* optimistic ? */ false>(context, /* lhs ? */ false, next, callback);

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
        context.validate_version(); // did we scan everything?
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
    uint64_t vertex_id = next.source();
    const bool is_optimistic = context.has_version();

    auto visitor_cb = [&context, &read_next, &next, &callback, vertex_id, is_optimistic](const DataItem* const_data_item){
        // make a copy
        DataItem data_item = *const_data_item;
        if(is_optimistic) context.validate_version();

        if(data_item.m_update.is_empty()){
            next = data_item.m_update.key().successor();
            return true;
        }

        auto current = data_item.m_update.key();
        if(current.source() != vertex_id){ // we're done
            return false;
        }


        Update update = Update::read_delta(context, &data_item);
        assert(update.source() == vertex_id);
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
