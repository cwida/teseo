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

#include "teseo/memstore/segment.hpp"

#include <cassert>
#include <chrono>
#include <iomanip>
#include <ostream>

#include "teseo/memstore/context.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/thread.hpp"

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

Segment::Segment() : m_fence_key( KEY_MAX ) {
    m_num_active_threads = 0;
    m_time_last_rebal = chrono::steady_clock::now();
    m_rebal_context = nullptr;
}

Segment::~Segment() {

}

/*****************************************************************************
 *                                                                           *
 *   Latching                                                                *
 *                                                                           *
 *****************************************************************************/

#if !defined(NDEBUG)
void Segment::lock(){
    m_latch.lock();

    util::barrier();
    assert(m_locked == false && "Spin lock already acquired");
    m_locked = true;
    m_owned_by = util::Thread::get_thread_id();
    util::barrier();
}

void Segment::unlock(){
    util::barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    util::barrier();

    m_latch.unlock();
}
#endif

void Segment::wake_next(){
    assert(m_locked && "To invoke this method the internal lock must be acquired first");
    if(m_queue.empty()) return;

    // FREE are "optimistic readers". These are readers that don't modify the version of the latch. There
    // is no point to wake them immediately if there are other readers or writers in the queue, because
    // they will abort immediately once another reader/writer accesses the latch. The idea is either to skip
    // them or wake all of them if there are no other entities in the queue
    if(m_queue[0].m_purpose == State::FREE){
        uint64_t sz = m_queue.size();
        uint64_t i = 0;
        do {
            m_queue.append(m_queue[0]);
            m_queue.pop();
            i++;
        } while(i < sz && m_queue[0].m_purpose == State::FREE);

        if(i == sz){ // are all the items optimistic readers?
            wake_all();
            return; // done
        }
    }

    switch(m_queue[0].m_purpose){
    case State::READ:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::READ);
        break;
    case State::WRITE:
        m_queue[0].m_promise->set_value();
        m_queue.pop();
        break;
    case State::REBAL:
        do {
            m_queue[0].m_promise->set_value();
            m_queue.pop();
        } while(!m_queue.empty() && m_queue[0].m_purpose == State::REBAL);
        break;
    default:
        assert(0 && "Invalid state");
    }
}

void Segment::wake_all(){
    assert((m_locked || m_state == State::REBAL) && "To invoke this method the internal lock must be acquired first");

    while(!m_queue.empty()){
        m_queue[0].m_promise->set_value(); // notify
        m_queue.pop();
    }
}

/*****************************************************************************
 *                                                                           *
 *   Fence keys                                                              *
 *                                                                           *
 *****************************************************************************/
Key Segment::get_lfkey(Context& context) {
    assert(context.m_segment != nullptr && "Context not set");
    return context.m_segment->m_fence_key;
}

Key Segment::get_hfkey(Context& context) {
    uint64_t sid = context.segment_id();
    if(sid == context.m_leaf->num_segments()){
        return context.m_leaf->get_hfkey();
    } else {
        return context.m_leaf->get_segment(sid)->m_fence_key;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/
void Segment::update(Context& context, const Update& update, bool has_source_vertex) {
    // first of all, ensure we hold a writer lock on this segment
    assert(m_state == State::WRITE);
    assert(m_writer_id == util::Thread::get_thread_id());
    assert(update.key() >= get_lfkey(context) && "This update does not respect the low fence key of this segment");
    assert(update.key() < get_hfkey(context) && "This update does not respect the high fence key of this segment");

    // perform the update
    if(is_sparse()){
        bool success = sparse_file(context)->update(context, update, has_source_vertex);
        if(!success){
            to_dense_file(context);
            dense_file(context)->update(context, update, has_source_vertex);
        }
    } else {
        assert(is_dense());
        dense_file(context)->update(context, update, has_source_vertex);
    }

    // FIXME: rebalance
}

void Segment::rollback(Context& context, const Update& update, transaction::Undo* next){
    if(is_sparse()){
        sparse_file(context)->rollback(context, update, next);
    } else {
        assert(is_dense());
        dense_file(context)->rollback(context, update, next);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Point look ups                                                          *
 *                                                                           *
 *****************************************************************************/

bool Segment::has_item_optimistic(Context& context, const Key& key, bool is_unlocked) const {
    if(is_sparse()){
        return sparse_file(context)->has_item_optimistic(context, key, is_unlocked);
    } else {
        assert(is_dense());
        return dense_file(context)->has_item_optimistic(context, key, is_unlocked);
    }
}

double Segment::get_weight_optimistic(Context& context, const Key& key) const {
    if(is_sparse()){
        return sparse_file(context)->get_weight_optimistic(context, key);
    } else {
        assert(is_dense());
        return dense_file(context)->get_weight_optimistic(context, key);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Sparse file                                                             *
 *                                                                           *
 *****************************************************************************/
void Segment::to_sparse_file(Context& context){
    if(!is_sparse()){ return; } // it's already a sparse segment

    dense_file(context)->~DenseFile();
    new (sparse_file(context)) SparseFile();

    m_latch.set_payload(0); /* 0 = sparse file, 1 = dense file */
}

SparseFile* Segment::sparse_file(Context& context) const {
    return context.sparse_file();
}

/*****************************************************************************
 *                                                                           *
 *   Dense file                                                              *
 *                                                                           *
 *****************************************************************************/
void Segment::load_to_file(SparseFile* sparse_file, bool is_lhs, void* output_file, void* output_txlocks){
    DenseFile::File* file = reinterpret_cast<DenseFile::File*>(output_file);
    DenseFile::TransactionLocks* transaction_locks = reinterpret_cast<DenseFile::TransactionLocks*>(output_txlocks);

    // pointers to the static & delta portions of the segment
    uint64_t* __restrict c_start = sparse_file->get_content_start(is_lhs);
    uint64_t* __restrict c_end = sparse_file->get_content_end(is_lhs);
    uint64_t* __restrict v_start = sparse_file->get_versions_start(is_lhs);
    uint64_t* __restrict v_end = sparse_file->get_versions_end(is_lhs);

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
         vertex = sparse_file->get_vertex(c_start + c_index);
         edge = nullptr;
         version = nullptr;

         if(v_index < v_length &&  sparse_file->get_version(v_start + v_index)->get_backptr() == v_backptr){
             version = sparse_file->get_version(v_start + v_index);
             v_index += OFFSET_VERSION;
         }

         if(vertex->m_first == 1){ // do not save dummy vertices
             DataItem* data_item = file->append();

             bool is_insert = true;
             if(version != nullptr){
                 data_item->m_version = *version;
                 is_insert = version->is_insert();
             }

             data_item->m_update = Update(/* is vertex ? */ true, is_insert, Key { vertex->m_vertex_id } );
         }

         if(vertex->m_lock == 1){
             transaction_locks->lock(vertex->m_vertex_id);
         }

         c_index += OFFSET_ELEMENT;
         v_backptr++;

         // Fetch its edges
         int64_t e_length = c_index + vertex->m_count * OFFSET_ELEMENT;
         while(c_index < e_length){
             edge = sparse_file->get_edge(c_start + c_index);
             version = nullptr;

             if(v_index < v_length && sparse_file->get_version(v_start + v_index)->get_backptr() == v_backptr){
                 version = sparse_file->get_version(v_start + v_index);
                 v_index += OFFSET_VERSION;
             }

             DataItem* data_item = file->append();
             bool is_insert = true;
             if(version != nullptr){
                 data_item->m_version = *version;
                 is_insert = version->is_insert();
             }

             data_item->m_update = Update(/* is vertex ? */ false, is_insert, Key { vertex->m_vertex_id, edge->m_destination}, edge->m_weight);

             // next iteration
             c_index += OFFSET_ELEMENT;
             v_backptr++;
         } // end while, fetch edges
     } // end while, fetch vertices
}

void Segment::to_dense_file(Context& context){
    assert(!is_dense() && "It's already a dense file");

    SparseFile* sf = sparse_file(context);
    DenseFile::File file;
    DenseFile::TransactionLocks transaction_locks;

    load_to_file(sf, /* true -> lhs */ true, &file, &transaction_locks);
    load_to_file(sf, /* false -> rhs */ false, &file, &transaction_locks);

    sf->~SparseFile();

    DenseFile* df = dense_file(context);
    new (df) DenseFile(move(file), move(transaction_locks));

    m_latch.set_payload(1); /* 0 = sparse file, 1 = dense file */
}

DenseFile* Segment::dense_file(Context& context) const {
    return context.dense_file();
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

void Segment::dump() {
    cout << "[Segment] " << (void*) this << ", ";
    cout << "state: " << m_state << ", ";
    cout << "num active threads: " << m_num_active_threads << ", ";
    cout << "low fence key: " << m_fence_key << ", ";
#if !defined(NDEBUG)
    cout << "locked: " << boolalpha << m_locked << ", ";
    cout << "writer_id: " << m_writer_id << ", ";
    cout << "rebalancer_id: " << m_rebal_context << ", ";
#endif
    cout << "rebal_context: " << m_rebal_context;
}

void Segment::dump_and_validate(std::ostream& out, Context& context, bool* integrity_check) {
    Segment* segment = context.m_segment;
    assert(segment != nullptr);

    print_tabs(out, 1);
    out << "+-- [SEGMENT #"  << context.segment_id() << "] " << ((void*) segment);
#if !defined(NDEBUG)
        out << ", locked: ";
        if(segment->m_locked){
            out << "yes, by thread id " << segment->m_owned_by;
        } else {
            out << "no";
        }
        out << ", writer_id: " << segment->m_writer_id << ", rebalancer_id: " << segment->m_rebalancer_id;
#endif
    out << ", fence keys = [" << Segment::get_lfkey(context) << ", " << Segment::get_hfkey(context) << ") \n";

    dump_file(out, context, integrity_check);
}

void Segment::dump_file(std::ostream& out, Context& context, bool* integrity_check) {
    Segment* segment = context.m_segment;
    if(segment->is_sparse()){
        segment->sparse_file(context)->dump_and_validate(out, context, integrity_check);
    } else {
        assert(segment->is_dense());
        // FIXME
    }
}

void Segment::dump_unfold_undo(std::ostream& out, const transaction::Undo* undo){
    uint64_t tx_max = numeric_limits<uint64_t>::max();
    uint64_t i = 0;

    while(undo != nullptr) {
        const transaction::TransactionImpl* tx = undo->transaction();
        uint64_t read_id = tx->ts_read();
        uint64_t write_id = tx->ts_write();

        print_tabs(out, 5);
        out << i << ". " << undo << ", ";

        if(read_id != write_id){
            out << "version locked by txn read_id: " << read_id << ", write_id: " << write_id;
        } else {
            out << "version (";
            if(tx_max == numeric_limits<uint64_t>::max()){
                out << "+inf";
            } else {
                out << tx_max;
            }
            out << ", " << read_id << "]";
        }

        Update* update = reinterpret_cast<Update*>(undo->payload());
        transaction::Undo* next = undo->next();
        out << ", update: {" << *update << "}, next: " << next << "\n";

        tx_max = read_id;
        undo = next;
    }
}

ostream& operator<<(ostream& out, const Segment::State& state){
    switch(state){
    case Segment::State::FREE:
        out << "FREE";
        break;
    case Segment::State::READ:
        out << "READ";
        break;
    case Segment::State::WRITE:
        out << "WRITE";
        break;
    case Segment::State::REBAL:
        out << "REBAL";
        break;
    default:
        out << "Unknown (" << ((int) state) << ")";
    }
    return out;
}

} // namespace
