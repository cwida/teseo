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

#include "../memstore-old/remove_vertex.hpp"

#include <iomanip>
#include <iostream>
#include <string>

#include "util/miscellaneous.hpp"
#include "context.hpp"
#include "error.hpp"

#include "../memstore-old/gate.hpp"
#include "../memstore-old/sparse_array.hpp"

using namespace std;
using namespace teseo::internal::context;

namespace teseo::internal::memstore {

/**
 * The vertex ID 0 is reserved to avoid the confusion of the key <42, 0> in the Index, both referring
 * to the vertex 42 and the edge 42 -> 0
 */
static uint64_t I2E(uint64_t i){ assert(i >= 1); return i -1; }

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::lock_guard<std::mutex> lock(g_debugging_mutex); std::cout << "[RemoveVertex::" << __FUNCTION__ << "] [" << teseo::internal::util::get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/

RemoveVertex::RemoveVertex(SparseArray* instance, SparseArray::Transaction* transaction, uint64_t vertex_id, std::vector<uint64_t>* out_outgoing_edges) : m_instance(instance), m_transaction(transaction), m_vertex_id(vertex_id), m_outgoing_edges(out_outgoing_edges), m_owns_outgoing_edges(false){
    if(out_outgoing_edges == nullptr && !instance->is_directed()){
        m_outgoing_edges = new vector<uint64_t>();
        m_owns_outgoing_edges = true;
    }

    m_scratchpad = new uint64_t[ m_instance->get_num_qwords_per_segment() ];
    m_scratchpad_pos = 0;
}

RemoveVertex::~RemoveVertex(){
    if(m_owns_outgoing_edges){
        delete m_outgoing_edges; m_outgoing_edges = nullptr;
        m_owns_outgoing_edges = false;
    }

    delete m_scratchpad; m_scratchpad = nullptr;
}

uint64_t RemoveVertex::operator()(){
    COUT_DEBUG("vertex id: " << m_vertex_id);

    m_key = Key{m_vertex_id};
    if(m_outgoing_edges != nullptr) { m_outgoing_edges->clear(); }
    m_unlock_required = false;

    try {
        lock();
    } catch (...){
        if(m_unlock_required){ unlock(); }
        m_transaction->do_rollback(m_num_items_removed);
        throw;
    }

    // Did we remove the vertex?
    if(m_num_items_removed == 0){ // vertex not found
        assert(m_unlock_required == false && "If there vertex does not exist, it cannot be locked");
        RAISE_EXCEPTION(LogicalError, "The vertex " << I2E(m_vertex_id) << " does not exist");
    }

    if(m_unlock_required){ unlock(); }

    uint64_t num_edges_removed = m_num_items_removed -1;  // the vertex + outgoing edges

    if(num_edges_removed > 0 && !m_instance->is_directed()){ // remove incoming edges
        assert(m_outgoing_edges != nullptr && "It should have recorded all edges removed");
        assert(m_outgoing_edges->size() == num_edges_removed); // as above

        SparseArray::Update update;
        update.m_entry_type = SparseArray::Update::Edge;
        update.m_update_type = SparseArray::Update::Remove;
        update.m_source = 0; // going to be set in the loop
        update.m_destination = m_vertex_id;
        update.m_weight = 0; // it doesn't matter, ignored

        try {
            for(uint64_t i = 0; i < num_edges_removed; i++){
                update.m_source = m_outgoing_edges->at(i);
                m_instance->write(m_transaction, update);
                m_num_items_removed++;
            }
        } catch(...){
            m_transaction->do_rollback(m_num_items_removed);
            throw;
        }
    }

    return num_edges_removed;
}

/*****************************************************************************
 *                                                                           *
 *   Locking step                                                            *
 *                                                                           *
 *****************************************************************************/

void RemoveVertex::lock(){
    do {
        ScopedEpoch epoch;
        m_chunk = nullptr;
        m_gate = nullptr;
        m_rebalance = false;

        try { // Access the chunk, gate pair
            std::tie(m_chunk, m_gate) = m_instance->writer_on_entry(m_key);
            assert(m_chunk != nullptr && m_gate != nullptr);

            lock_gate();

            if(m_rebalance){
                m_instance->rebalance_chunk(m_chunk, m_gate); // can fire Abort{}
            } else {
                m_instance->writer_on_exit(m_chunk, m_gate);
            }

        } catch(RebalancingAbort){
            /* nop, don't release the gate */
        } catch (Abort){
            if(m_gate != nullptr){ m_instance->writer_on_exit(m_chunk, m_gate); }  // release the gate
        } catch(...){
            if(m_gate != nullptr){ m_instance->writer_on_exit(m_chunk, m_gate); }  // release the gate
            throw;
        }
    } while( m_key.get_source() == m_vertex_id );

    m_chunk = nullptr;
    m_gate = nullptr;
}

void RemoveVertex::lock_gate(){
    COUT_DEBUG("Gate: " << m_gate->id() << ", key: " << m_key);
    int64_t g2sid = m_gate->find(m_key);;

    do {
        m_rebalance = false; // reset the rebalance flag

        Key next_key = (g2sid +1 == m_gate->window_length()) ? m_gate->m_fence_high_key : m_gate->get_separator_key(g2sid +1);

        uint64_t segment_id = m_gate->id() * m_instance->get_num_segments_per_lock() + g2sid / 2;
        m_segment = m_instance->get_segment(m_chunk, segment_id);
        m_is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment

        lock_segment();

        if(m_rebalance){
            bool rebalance_done = m_instance->rebalance_gate(m_chunk, m_gate, segment_id);
            m_rebalance = !rebalance_done;
        } else {
            g2sid++; // next segment
            m_key = next_key;
        }

    } while(!m_rebalance && m_key.get_source() == m_vertex_id && m_key < m_gate->m_fence_high_key);

    m_segment = nullptr;
}

void RemoveVertex::lock_segment(){
    assert(m_chunk != nullptr && "Chunk not set");
    assert(m_segment != nullptr && "Segment not given");
    COUT_DEBUG("segment: " << m_instance->get_segment_id(m_chunk, m_segment) << ", is_lhs: " << (boolalpha) << m_is_lhs);

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = m_instance->get_segment_content_start(m_chunk, m_segment, m_is_lhs);
    uint64_t* __restrict c_end = m_instance->get_segment_content_end(m_chunk, m_segment, m_is_lhs);
    uint64_t* __restrict v_start = m_instance->get_segment_versions_start(m_chunk, m_segment, m_is_lhs);
    uint64_t* __restrict v_end = m_instance->get_segment_versions_end(m_chunk, m_segment, m_is_lhs);

    // first, find the position in the content area
    SparseArray::SegmentVertex* vertex { nullptr };
    uint64_t v_backptr = 0;
    int64_t c_index = 0;
    int64_t c_length = c_end - c_start;
    bool c_found = false;
    bool stop = false;
    while(c_index < c_length && !stop){
        vertex = SparseArray::get_vertex(c_start + c_index);
        if(vertex->m_vertex_id < m_vertex_id){
            c_index += SparseArray::OFFSET_VERTEX + vertex->m_count * SparseArray::OFFSET_EDGE; // skip the edges altogether
            v_backptr += 1 + vertex->m_count;
        } else {
            c_found = vertex->m_vertex_id == m_vertex_id;
            stop = vertex->m_vertex_id >= m_vertex_id;
        }
    }

    if(!c_found){ m_key.set(m_vertex_id +1); return; }

    // second, find the position in the versions area
    SparseArray::SegmentVersion* v_src { nullptr };
    int64_t v_index = 0;
    int64_t v_length = v_end - v_start;
    bool v_found = false;
    stop = false;
    while(v_index < v_length && !stop){
        v_src = SparseArray::get_version(v_start + v_index);
        uint64_t backptr = SparseArray::get_backptr(v_src);
        if(backptr < v_backptr){
            v_index += SparseArray::OFFSET_VERSION;
        } else {
            v_found = c_found && (backptr == v_backptr);
            stop = backptr >= v_backptr;
        }
    }
    int64_t v_bookmark = v_index;

    // three, consistency checks
    if(vertex->m_first == 1){
        if(v_found && !m_transaction->can_write(SparseArray::get_undo(v_src))){
            RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the vertex ID " << I2E(m_vertex_id) << " is currently locked by another transaction. Restart this transaction to alter this object");
        } else if (vertex->m_lock == 0 && v_found && SparseArray::is_remove(v_src)) {
            RAISE_EXCEPTION(LogicalError, "The vertex " << I2E(m_vertex_id) << " does not exist");
        }
    }

    // fourth, remove the vertex
    int64_t budget = m_instance->get_gate_free_space(m_chunk, m_gate);
    SparseArray::SegmentVersion* v_scratchpad = reinterpret_cast<SparseArray::SegmentVersion*>(m_scratchpad);
    m_scratchpad_pos = 0;
    if(vertex->m_first == 1 && vertex->m_lock == 0){
        SparseArray::SegmentVersion* v_dest = v_scratchpad + m_scratchpad_pos;
        if(!v_found){
            if(budget < (int64_t) SparseArray::OFFSET_VERSION) { m_rebalance = true; return; }
            SparseArray::reset_header(v_dest);
            budget -= SparseArray::OFFSET_VERSION;
        } else {
            v_scratchpad[m_scratchpad_pos] = *v_src;
            v_index++;
        }

        SparseArray::Update update;
        update.m_entry_type = SparseArray::Update::Vertex;
        update.m_update_type = SparseArray::Update::Remove;
        update.m_source = m_vertex_id;

        SparseArray::set_type(v_dest, update);
        SparseArray::set_backptr(v_dest, v_backptr);
        SparseArray::set_undo(v_dest, m_transaction->add_undo(m_instance, v_found ? SparseArray::get_undo(v_src) : nullptr, update));
        SparseArray::flip_undo(v_dest); // insert -> remove, remove -> insert

        m_scratchpad_pos++;
        m_num_items_removed++;
    }
    m_unlock_required = true;
    vertex->m_lock = 1;
    v_backptr++;

    // fifth, remove the edges
    c_index += SparseArray::OFFSET_VERTEX;
    int64_t e_length = c_index + vertex->m_count * SparseArray::OFFSET_EDGE;
    bool has_conflict = false;
    bool no_space_left = false;
    while(c_index < e_length){
        bool ignore_edge = false;
        SparseArray::SegmentEdge* edge = SparseArray::get_edge(c_start + c_index);

        SparseArray::SegmentVersion* v_dest = v_scratchpad + m_scratchpad_pos;
        if(v_index < v_length && SparseArray::get_backptr(v_start + v_index) == v_backptr){
            SparseArray::SegmentVersion* v_src = SparseArray::get_version(v_start + v_index);
            if(!m_transaction->can_write(SparseArray::get_undo(v_src))){ has_conflict = true; break; }
            ignore_edge = SparseArray::is_remove(v_src);
            *v_dest = *v_src;
            v_index++; // next iteration
        } else {
            if(budget < (int64_t) SparseArray::OFFSET_VERSION){ no_space_left = true; break; }
            SparseArray::reset_header(v_dest);
            budget -= SparseArray::OFFSET_VERSION;
        }

        if(!ignore_edge){
            SparseArray::Update update;
            update.m_entry_type = SparseArray::Update::Edge;
            update.m_update_type = SparseArray::Update::Remove;
            update.m_source = m_vertex_id;
            update.m_destination = edge->m_destination;
            update.m_weight = 0; // it doesn't matter, ignored

            SparseArray::set_type(v_dest, update);
            SparseArray::set_backptr(v_dest, v_backptr);
            SparseArray::set_undo(v_dest, m_transaction->add_undo(m_instance, SparseArray::get_undo(v_src), update));
            SparseArray::flip_undo(v_dest); // insert -> remove, remove -> insert

            record_edge_removed(edge);
        }

        m_key.set(m_vertex_id, edge->m_destination +1);
        c_index += SparseArray::OFFSET_EDGE;
        m_scratchpad_pos++;
        v_backptr++;
    }

    // 6: copy the remaining versions into the scratchpad
    uint64_t copy_sz = (v_length - v_index) * SparseArray::OFFSET_VERSION;
    memcpy(m_scratchpad + m_scratchpad_pos, v_start + v_index, copy_sz * sizeof(uint64_t));
    m_scratchpad_pos += copy_sz;

    // 7: copy the versions from the scratchpad back to the sparse array
    copy_scratchpad(v_bookmark);

    // 8: if there has been a conflict, report it!
    if(has_conflict == true){
        SparseArray::SegmentEdge* edge = SparseArray::get_edge(c_start + c_index);
        RAISE_EXCEPTION(TransactionConflict, "Conflict detected, the edge " << I2E(m_vertex_id) << " -> " << I2E(edge->m_destination) << " is currently "
                "locked by another transaction. Restart this transaction to alter this object");
    }

    // 9: do we need more space to remove the edges?
    if(no_space_left == true){
        m_rebalance = true;
        return;
    }

    // 10: we're done
    assert(c_index == e_length && "We didn't visit all edges");
    if(vertex->m_first == 1 && e_length < c_length){
        vertex->m_lock = 0; // we can immediately release the lock
        m_unlock_required = false;
    }
}

//void RemoveVertex::record_edge_removed(SparseArray::SegmentEdge* edge){
//    m_num_items_removed++;
//    if(m_outgoing_edges != nullptr){
//        m_outgoing_edges->push_back(edge->m_destination);
//    }
//}

void RemoveVertex::copy_scratchpad(int64_t bookmark){
    uint64_t* __restrict v_start = m_instance->get_segment_versions_start(m_chunk, m_segment, m_is_lhs);
    uint64_t* __restrict v_end = m_instance->get_segment_versions_end(m_chunk, m_segment, m_is_lhs);
    int64_t v_length = v_end - v_start;
    assert(v_length >= bookmark && "Underflow");
    int64_t v_add = m_scratchpad_pos - (v_length - bookmark);

    uint64_t copy_sz = m_scratchpad_pos * SparseArray::OFFSET_VERSION;

    if(m_is_lhs){
        memcpy(v_start + bookmark, m_scratchpad, copy_sz * sizeof(uint64_t));
        m_segment->m_empty1_start += v_add;
    } else {
        memmove(v_start - v_add, v_start, bookmark * SparseArray::OFFSET_VERSION * sizeof(uint64_t));
        memcpy(v_start - v_add + bookmark, m_scratchpad, copy_sz * sizeof(uint64_t));
        m_segment->m_empty2_start -= v_add;
    }

    m_gate->m_used_space += v_add;
    m_scratchpad_pos = 0;
}

/*****************************************************************************
 *                                                                           *
 *   Unlocking step                                                          *
 *                                                                           *
 *****************************************************************************/

void RemoveVertex::unlock(){
   if(m_key.get_source() != m_vertex_id){
       m_key.set(m_vertex_id, numeric_limits<uint64_t>::max());
   }

   bool done = false;
   do {
       ScopedEpoch epoch;
       m_chunk = nullptr;
       m_gate = nullptr;
       m_segment = nullptr;

       try {
           // access the gate
           std::tie(m_chunk, m_gate) = m_instance->writer_on_entry(m_key);
           assert(m_chunk != nullptr && m_gate != nullptr);

           // access the segment
           uint64_t g2sid = m_gate->find(m_key);
           int64_t segment0 = m_gate->id() * m_instance->get_num_segments_per_lock();
           int64_t segment_id = segment0 + g2sid / 2;
           m_is_lhs = g2sid % 2 == 0; // whether to use the lhs or rhs of the segment

           do {
               m_segment = m_instance->get_segment(m_chunk, segment_id);

               // pointers to the static & delta portions of the segment
               uint64_t* __restrict c_start = m_instance->get_segment_content_start(m_chunk, m_segment, m_is_lhs);
               uint64_t* __restrict c_end = m_instance->get_segment_content_end(m_chunk, m_segment, m_is_lhs);
               int64_t c_index = 0;
               int64_t c_length = c_end - c_start;

               // first, find the position in the content area
               SparseArray::SegmentVertex* vertex { nullptr };
               uint64_t v_backptr = 0;

               bool vertex_found = false;
               bool stop = false;
               while(c_index < c_length && !stop){
                   vertex = SparseArray::get_vertex(c_start + c_index);
                   if(vertex->m_vertex_id < m_vertex_id){
                       c_index += SparseArray::OFFSET_VERTEX + vertex->m_count * SparseArray::OFFSET_EDGE; // skip the edges altogether
                       v_backptr += 1 + vertex->m_count;
                       done = true;
                   } else {
                       vertex_found = vertex->m_vertex_id == m_vertex_id;
                       stop = vertex->m_vertex_id >= m_vertex_id;
                   }
               }

               if(vertex_found){
                  assert(vertex->m_vertex_id == m_vertex_id);
                  vertex->m_lock = 0;
                  done = (vertex->m_first == 1);

                  if(!done){ // bookmark in case of abort{}
                      assert(vertex->m_count > 0 && "Because all dummy edges must have a vertex");
                      m_key.set(m_vertex_id, SparseArray::get_edge(reinterpret_cast<uint64_t*>(vertex +1))->m_destination -1);
                      if(m_key.get_destination() == numeric_limits<uint64_t>::max()){ // underflow
                          m_key.set(m_vertex_id, 0);
                      }
                  }
               }

               m_segment = nullptr;
               if(m_is_lhs){ // go down one segment
                   m_is_lhs = false;
                   segment_id --;
               } else { // look to the lhs
                   m_is_lhs = true;
               }
           } while(segment_id >= segment0 && !done);

           m_instance->writer_on_exit(m_chunk, m_gate);

       } catch (Abort) {
           // try again
       }
   } while(!done);

}

} // namespace



