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

#include "teseo/memstore/context.hpp"

#include <cassert>
#include <mutex>
#include <ostream>

#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/index_entry.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/util/thread.hpp"

using namespace std;


namespace teseo::memstore {

Context::Context(Memstore* tree, transaction::TransactionImpl* transaction) :
    m_transaction(transaction), m_tree(tree), m_leaf(nullptr), m_segment(nullptr) {
}

uint64_t Context::segment_id() const {
    assert(m_leaf != nullptr && "No leaf set");
    assert(m_segment != nullptr && "No segment set");

    Leaf* base_leaf = m_leaf;
    Segment* base_segment = reinterpret_cast<Segment*>(base_leaf +1);
    return (m_segment - base_segment) / sizeof(Segment);
}


SparseFile* Context::sparse_file() const {
    return sparse_file(m_leaf, segment_id());
}

SparseFile* Context::sparse_file(const Leaf* base_leaf, uint64_t segment_id){
    Segment* base_segment = const_cast<Segment*>(reinterpret_cast<const Segment*>(base_leaf + 1));
    uint64_t* base_file = reinterpret_cast<uint64_t*>(base_segment + context::StaticConfiguration::memstore_num_segments_per_leaf);
    return reinterpret_cast<SparseFile*>(base_file + segment_id * context::StaticConfiguration::memstore_segment_size);
}

/*****************************************************************************
 *                                                                           *
 *   Writer enter/exit                                                       *
 *                                                                           *
 *****************************************************************************/
void Context::writer_enter(Key search_key){
    profiler::ScopedTimer profiler { profiler::SA_WRITER_ON_ENTRY };

    context::ThreadContext* context = context::thread_context();
    assert(context != nullptr);
    context->epoch_enter();

    profiler::ScopedTimer prof_index { profiler::SA_WRITER_ON_ENTRY_INDEX_FIND };
    IndexEntry entry = m_tree->index()->find(search_key.source(), search_key.destination());
    Leaf* leaf = entry.leaf();
    int64_t segment_id = entry.segment_id();
    Segment* segment = nullptr;
    prof_index.stop();

    bool done = false;
    profiler::ScopedTimer prof_gate_find { profiler::SA_WRITER_ON_ENTRY_GET_GATE, false };
    do {
        prof_gate_find.start();
        segment = leaf->get_segment(segment_id);

        unique_lock<Segment> lock(*segment);

        if(leaf->check_fence_keys(segment_id, search_key)){ // -> it can raise an abort
            switch(segment->m_state){
            case Segment::State::FREE:
                assert(segment->m_num_active_threads == 0 && "Precondition not satisfied");
                segment->m_state = Segment::State::WRITE;
                segment->m_num_active_threads = 1;
#if !defined(NDEBUG) /* for debugging purposes only */
                segment->m_writer_id = util::Thread::get_thread_id();
#endif

                done = true; // done, proceed with the insertion
                break;
            case Segment::State::READ:
            case Segment::State::WRITE:
            case Segment::State::REBAL:
                segment->wait<Segment::State::WRITE>(lock);
            }
        }
        prof_gate_find.stop();
    } while(!done);

    m_leaf = leaf;
    m_segment = segment;
}


void Context::writer_exit(){
    assert(m_leaf != nullptr && m_segment != nullptr);

    profiler::ScopedTimer profiler { profiler::SA_WRITER_ON_EXIT };

    m_segment->lock();
    m_segment->m_num_active_threads = 0;

#if !defined(NDEBUG)
    assert(m_segment->m_writer_id == util::Thread::get_thread_id());
    m_segment->m_writer_id = -1;
#endif

    switch(m_segment->m_state){
    case Segment::State::WRITE:
        // same state as before
        m_segment->m_state = Segment::State::FREE;
        break;
    case Segment::State::REBAL:
        // the rebalancer wants to process this gate => nop
        break;
    default:
        assert(0 && "Invalid state");
    }

    m_segment->wake_next(); // the rebalancer may be at the front of the list
    m_segment->unlock();

    m_leaf = nullptr;
    m_segment = nullptr;
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

std::ostream& operator<<(std::ostream& out, const Context& context){
    out << "[";
    out << "transaction: " << context.m_transaction << ", ";
    out << "tree: " << context.m_tree << ", ";
    out << "leaf: " << context.m_leaf << ", ";
    out << "segment: " << context.m_segment;
    out << "]";
    return out;
}

}


