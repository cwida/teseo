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

#include "teseo/aux/partial_result.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/remove_vertex.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/thread.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

Segment::Segment() : m_flags(0), m_fence_key( KEY_MAX ) {
    m_num_active_threads = 0;
    m_time_last_rebal = chrono::steady_clock::now();
    m_crawler = nullptr;
    m_used_space = 0;

    set_state(State::FREE);
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

    util::compiler_barrier();
    assert(m_locked == false && "Spin lock already acquired");
    m_locked = true;
    m_owned_by = util::Thread::get_thread_id();
    util::compiler_barrier();
}

void Segment::unlock(){
    util::compiler_barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    util::compiler_barrier();
    m_latch.unlock();
}

void Segment::invalidate(){
    util::compiler_barrier();
    assert(m_locked == true && "Spin lock already released");
    m_locked = false;
    m_owned_by = -1;
    util::compiler_barrier();
    m_latch.invalidate();
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
            auto item = m_queue[0]; // copy the item. Otherwise if the queue resizes, the ref won't be valid anymore
            m_queue.pop();
            m_queue.append(item);
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
    assert((m_locked || get_state() == State::REBAL) && "To invoke this method the internal lock must be acquired first");

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
Key Segment::get_lfkey(const Context& context) {
    assert(context.m_segment != nullptr && "Context not set");
    return context.m_segment->m_fence_key;
}

Key Segment::get_hfkey(const Context& context) {
    uint64_t next_segment_id = context.segment_id() +1;
    if(next_segment_id == context.m_leaf->num_segments()){
        return context.m_leaf->get_hfkey();
    } else {
        return context.m_leaf->get_segment(next_segment_id)->m_fence_key;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/
void Segment::update(Context& context, const Update& update, bool has_source_vertex) {
    profiler::ScopedTimer profiler { profiler::SEGMENT_UPDATE };

    COUT_DEBUG("update: " << update << ", has_source_vertex: " << has_source_vertex);

    // first of all, ensure we hold a writer lock on this segment
    Segment* segment = context.m_segment;
    assert(segment->get_state() == State::WRITE || segment->get_state() == State::REBAL);
    assert(segment->m_writer_id == util::Thread::get_thread_id());
    assert(update.key() >= get_lfkey(context) && "This update does not respect the low fence key of this segment");
    assert(update.key() < get_hfkey(context) && "This update does not respect the high fence key of this segment");

    // perform the update
    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        int64_t space_before = sf->used_space();
        sf->validate(context); // debug only, nop in opt build
        bool success = sf->update(context, update, has_source_vertex);
        sf->validate(context); // debug only, nop in opt build
        int64_t space_after = sf->used_space();
        segment->m_used_space += (space_after - space_before);

        if(!success){
            to_dense_file(context);
            segment->m_used_space += dense_file(context)->update(context, update, has_source_vertex);
        }
    } else {
        assert(segment->is_dense());
        segment->m_used_space += dense_file(context)->update(context, update, has_source_vertex);
    }

    request_async_rebalance(context);
}

void Segment::remove_vertex(RemoveVertex& instance){
    Context& context = instance.context();
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        int64_t space_before = sf->used_space();
        sf->validate(context); // debug only, nop in opt build
        bool success = sf->remove_vertex(instance);
        sf->validate(context); // debug only, nop in opt build
        int64_t space_after = sf->used_space();
        segment->m_used_space += (space_after - space_before);

        if(!success){
            to_dense_file(context);
            segment->m_used_space += dense_file(context)->remove_vertex(instance);
        }

    } else {
        assert(segment->is_dense());
        segment->m_used_space += dense_file(context)->remove_vertex(instance);
    }

    if(!instance.done()){
        instance.m_key = Segment::get_hfkey(context);
        if(instance.m_key.source() > instance.vertex_id()){
            instance.set_done();
        }
    }

    request_async_rebalance(context);
}

void Segment::unlock_vertex(RemoveVertex& instance){
    Context& context = instance.context();
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        sf->validate(context); // debug only, nop in opt build
        sf->unlock_removed_vertex(instance);
        sf->validate(context); // debug only, nop in opt build
    } else {
        assert(segment->is_dense());
        dense_file(context)->unlock_vertex(instance);
    }

    if(!instance.done()){
        instance.m_key = segment->m_fence_key.predecessor();
        if(instance.m_key.source() != instance.vertex_id()){
            instance.set_done();
        }
    }
}

void Segment::rollback(Context& context, const Update& update, transaction::Undo* next){
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        int64_t space_before = sf->used_space();
        sf->rollback(context, update, next);
        int64_t space_after = sf->used_space();
        segment->m_used_space += (space_after - space_before);
    } else {
        assert(segment->is_dense());
        segment->m_used_space += dense_file(context)->rollback(context, update, next);
    }
}


void Segment::request_async_rebalance(Context& context){
    Segment* segment = context.m_segment;
    if(segment->has_requested_rebalance()) return; // we already sent one

    if(segment->is_sparse()){
        SparseFile* sf = context.sparse_file();
        constexpr int64_t THRESHOLD = static_cast<int64_t>(context::StaticConfiguration::memstore_segment_size) - static_cast<int64_t>(4*OFFSET_ELEMENT + 2*OFFSET_VERSION);
        if( static_cast<int64_t>(sf->used_space()) < THRESHOLD ){
            return; // there is still space in the file
        }
    }

    COUT_DEBUG("Request rebalance, leaf: " << context.m_leaf << ", segment: " << context.segment_id());
    segment->set_flag(FLAG_REBAL_REQUESTED, 1);
    //context.m_tree->global_context()->async()->request(context);
    context.m_tree->global_context()->runtime()->schedule_rebalance(context, segment->m_fence_key);
}

/*****************************************************************************
 *                                                                           *
 *   Point look ups                                                          *
 *                                                                           *
 *****************************************************************************/

bool Segment::has_item_optimistic(Context& context, const Key& key, bool is_unlocked) {
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        return sparse_file(context)->has_item_optimistic(context, key, is_unlocked);
    } else {
        return dense_file(context)->has_item_optimistic(context, key, is_unlocked);
    }
}

double Segment::get_weight_optimistic(Context& context, const Key& key) {
    Segment* segment = context.m_segment;

    if(segment->is_sparse()){
        return sparse_file(context)->get_weight_optimistic(context, key);
    } else {
        return dense_file(context)->get_weight_optimistic(context, key);
    }
}

uint64_t Segment::get_degree(Context& context, Key& next){
    Segment* segment = context.m_segment;
    bool vertex_found = !(next.destination() == 0);
    auto lfkey = Segment::get_lfkey(context);
    auto hfkey = Segment::get_hfkey(context);
    uint64_t degree { 0 };

    if(segment->is_sparse()){
        degree = sparse_file(context)->get_degree(context, next, vertex_found);

        if(context.has_version()) { context.validate_version(); } // before setting the next key check our result is correct
        next = hfkey;
    } else {
        if(context.has_version()) context.validate_version(); // ensure lfkey and hfkey are correct
        Key dfnext = next;
        DenseFile* df = dense_file(context);
        bool done = false;
        do {
            try {
                df->get_degree(context, dfnext, vertex_found, degree);
                done = true;
            } catch( Abort ){
                // see if we can recover from this update
                context.optimistic_bump(lfkey);
                if(context.m_segment != segment ||  segment->m_fence_key != lfkey || !segment->is_dense() ) throw; // we failed
            }
        } while(!done);


        if(context.has_version()){ // for optimistic readers
            auto new_hfkey = Segment::get_hfkey(context);
            if(new_hfkey == hfkey){
                next = hfkey;
            } else {
                next = dfnext;
            }
        } else { // for locked readers
            next = hfkey;
        }
    }

    return degree;
}

/*****************************************************************************
 *                                                                           *
 *   Auxiliary vector                                                        *
 *                                                                           *
 *****************************************************************************/
bool Segment::aux_partial_result(Context& context, Key& next, aux::PartialResult* partial_result){
    assert(!context.has_version() && "A read lock for this operation is required");

    Segment* segment = context.m_segment;
    auto hfkey = Segment::get_hfkey(context);
    bool read_next = true; // move to the next segment ?

    if(segment->is_sparse()){
        bool check_end_interval = partial_result->key_to() < hfkey;
        read_next = sparse_file(context)->aux_partial_result(context, next, check_end_interval, partial_result);
    } else {
        read_next = dense_file(context)->aux_partial_result(context, next, partial_result);
    }

    next = hfkey;

    if(hfkey == KEY_MAX){ // we're done
        read_next = false;
    }
    return read_next;
}

/*****************************************************************************
 *                                                                           *
 *   Maintenance                                                             *
 *                                                                           *
 *****************************************************************************/
void Segment::load(Context& context, rebalance::ScratchPad& scratchpad){
    if(context.m_segment->is_sparse()){
        sparse_file(context)->load(scratchpad);
    } else {
        assert(context.m_segment->is_dense());
        dense_file(context)->load(scratchpad);
    }
}

void Segment::save(Context& context, rebalance::ScratchPad& scratchpad, int64_t& pos_next_vertex, int64_t& pos_next_element, int64_t target_budget, int64_t* out_budget_achieved) {
    to_sparse_file(context); // ensure the file is sparse
    SparseFile* sf = sparse_file(context);
    sf->save(scratchpad, pos_next_vertex, pos_next_element, target_budget, out_budget_achieved);
    context.m_segment->m_used_space = sf->used_space();
}

void Segment::clear_versions(Context& context){
    Segment* segment = context.m_segment;
    if(segment->is_sparse()){
        context.sparse_file()->clear_versions();
    } else {
        assert(segment->is_dense());
        context.dense_file()->clear_versions();
    }
}

void Segment::prune(Context& context){
    Segment* segment = context.m_segment;
    if(segment->is_sparse()){
        SparseFile* sf = sparse_file(context);
        sf->prune();
        segment->m_used_space = sf->used_space();
        segment->cancel_rebalance_request();
    }
    // dense file do not support prune, because they are going to be rebalanced soon
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

uint64_t Segment::cardinality(Context& context) {
    if(context.m_segment->is_sparse()){
        return sparse_file(context)->cardinality();
    } else {
        assert(context.m_segment->is_dense());
        return dense_file(context)->cardinality();
    }
}

uint64_t Segment::used_space(Context& context) {
    return context.m_segment->used_space();
}

bool Segment::is_unindexed(Context& context){
    return get_lfkey(context) == get_hfkey(context);
}

/*****************************************************************************
 *                                                                           *
 *   Sparse file                                                             *
 *                                                                           *
 *****************************************************************************/

void Segment::to_sparse_file(Context& context){
    if(context.m_segment->is_sparse()){ return; } // it's already a sparse segment
    profiler::ScopedTimer profiler { profiler::SEGMENT_TO_SPARSE };

    DenseFile* df = dense_file(context);
    df->clear();
    df->~DenseFile();
    new (sparse_file(context)) SparseFile();

    context.m_segment->set_flag(FLAG_FILE_TYPE, 0); /* 0 = sparse file, 1 = dense file */
}

SparseFile* Segment::sparse_file(Context& context) {
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
    profiler::ScopedTimer profiler { profiler::SEGMENT_TO_DENSE };
    assert(!context.m_segment->is_dense() && "It's already a dense file");

    SparseFile* sf = sparse_file(context);
    DenseFile::File file;
    DenseFile::TransactionLocks transaction_locks;

    load_to_file(sf, /* true -> lhs */ true, &file, &transaction_locks);
    load_to_file(sf, /* false -> rhs */ false, &file, &transaction_locks);

    sf->~SparseFile();

    DenseFile* df = dense_file(context);
    new (df) DenseFile(move(file), move(transaction_locks));

    context.m_segment->set_flag(FLAG_FILE_TYPE, 1); /* 0 = sparse file, 1 = dense file */
}

DenseFile* Segment::dense_file(Context& context) {
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
    if(is_sparse()){
        cout << "sparse, ";
    } else {
        cout << "dense, ";
    }
    cout << "state: " << get_state() << ", ";
    cout << "num active threads: " << m_num_active_threads << ", ";
    cout << "used space: " << m_used_space << " qwords, ";
    cout << "low fence key: " << m_fence_key << ", ";
#if !defined(NDEBUG)
    cout << "locked: " << boolalpha << m_locked << ", ";
    cout << "writer_id: " << m_writer_id << ", ";
    cout << "rebalancer_id: " << m_rebalancer_id << ", ";
#endif
    cout << "rebalance requested: " << boolalpha << has_requested_rebalance() << ", ";
    cout << "crawler: " << m_crawler << endl;
}

void Segment::dump_and_validate(std::ostream& out, Context& context, bool* integrity_check) {
    Segment* segment = context.m_segment;
    assert(segment != nullptr);

    print_tabs(out, 1);
    out << "+-- [SEGMENT #"  << context.segment_id() << "] " << ((void*) segment);
        out << ", state: " << segment->get_state();
#if !defined(NDEBUG)
        out << ", locked: ";
        if(segment->m_locked){
            out << "yes, by thread id " << segment->m_owned_by;
        } else {
            out << "no";
        }
        if(segment->m_writer_id != -1){
            out << ", writer_id: " << segment->m_writer_id;
        }
        if(segment->m_rebalancer_id != -1){
            out << ", rebalancer_id: " << segment->m_rebalancer_id;
        }
#endif
    if(segment->has_requested_rebalance()){
        out << ", rebalance requested";
    }
    out << ", used space: " << segment->m_used_space << " qwords";
    out << ", fence keys = [" << Segment::get_lfkey(context) << ", " << Segment::get_hfkey(context) << ") \n";

    dump_file(out, context, integrity_check);
}

void Segment::dump_file(std::ostream& out, Context& context, bool* integrity_check) {
    Segment* segment = context.m_segment;
    if(segment->is_sparse()){
        segment->sparse_file(context)->dump_and_validate(out, context, integrity_check);
    } else {
        assert(segment->is_dense());
        segment->dense_file(context)->dump_and_validate(out, context, integrity_check);
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
