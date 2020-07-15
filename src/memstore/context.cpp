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
#include <cinttypes>
#include <limits>
#include <mutex>
#include <ostream>
#include <string>

#include "teseo/aux/view.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/dense_file.hpp"
#include "teseo/memstore/direct_pointer.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/index_entry.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/assembly.hpp"
#include "teseo/util/thread.hpp"

using namespace std;


namespace teseo::memstore {

Context::Context(Memstore* tree, transaction::TransactionImpl* transaction) :
    m_transaction(transaction), m_tree(tree), m_leaf(nullptr), m_segment(nullptr), m_version(numeric_limits<uint64_t>::max()) {
}

uint64_t Context::segment_id() const {
    assert(m_leaf != nullptr && "No leaf set");
    assert(m_segment != nullptr && "No segment set");

    Leaf* base_leaf = m_leaf;
    Segment* base_segment = reinterpret_cast<Segment*>(base_leaf +1);
    return m_segment - base_segment;
}

SparseFile* Context::sparse_file() const {
    return sparse_file(m_leaf, segment_id());
}

SparseFile* Context::sparse_file(const Leaf* base_leaf, uint64_t segment_id){
    Segment* base_segment = const_cast<Segment*>(reinterpret_cast<const Segment*>(base_leaf + 1));
    uint64_t* base_file = reinterpret_cast<uint64_t*>(base_segment + context::StaticConfiguration::memstore_num_segments_per_leaf);
    return reinterpret_cast<SparseFile*>(base_file + segment_id * context::StaticConfiguration::memstore_segment_size); // okay
}

DenseFile* Context::dense_file() const {
    return dense_file(m_leaf, segment_id());
}

DenseFile* Context::dense_file(const Leaf* leaf, uint64_t segment_id){
    return reinterpret_cast<DenseFile*>(sparse_file(leaf, segment_id));
}

/*****************************************************************************
 *                                                                           *
 *   Writer enter/exit                                                       *
 *                                                                           *
 *****************************************************************************/
void Context::writer_enter(Key search_key){
    profiler::ScopedTimer profiler { profiler::CONTEXT_WRITER_ENTER };

    context::ThreadContext* context = context::thread_context();
    assert(context != nullptr);
    context->epoch_enter();

    profiler::ScopedTimer prof_index { profiler::CONTEXT_WRITER_ENTER_INDEX };
    IndexEntry entry = m_tree->index()->find(search_key.source(), search_key.destination());
    Leaf* leaf = entry.leaf();
    int64_t segment_id = entry.segment_id();
    Segment* segment = nullptr;
    prof_index.stop();

    bool done = false;
    profiler::ScopedTimer prof_browse { profiler::CONTEXT_WRITER_ENTER_BROWSE };
    do {
        if(check_fence_keys(leaf, &segment_id, search_key)){ // it can raise an abort
            segment = leaf->get_segment(segment_id);
            segment->writer_enter();

            m_leaf = leaf;
            m_segment = segment;

            // we need to check again the fence keys, this time under the segment's latch
            auto rc = leaf->check_fence_keys(segment_id, search_key);
            if(rc == FenceKeysDirection::OK){
                assert(segment->get_state() == Segment::State::WRITE && "We should have acquired an xlock to the segment");
                done = true;
            } else { // we failed, restart the search
                writer_exit(); // it expects m_leaf & m_segment already set

                m_leaf = nullptr;
                m_segment = nullptr;

                handle_fence_keys_direction(rc, &segment_id);
            }
        }
    } while(!done);
}

void Context::writer_exit(){
    profiler::ScopedTimer profiler { profiler::CONTEXT_WRITER_EXIT };
    assert(m_leaf != nullptr && m_segment != nullptr);

    m_segment->writer_exit();

    m_leaf = nullptr;
    m_segment = nullptr;
}

/*****************************************************************************
 *                                                                           *
 *   Reader enter/exit                                                       *
 *                                                                           *
 *****************************************************************************/

void Context::reader_enter(Key search_key){
    profiler::ScopedTimer profiler { profiler::CONTEXT_READER_ENTER };

    context::ThreadContext* context = context::thread_context();
    assert(context != nullptr);
    context->epoch_enter();

    profiler::ScopedTimer prof_index { profiler::CONTEXT_READER_ENTER_INDEX };
    IndexEntry entry = m_tree->index()->find(search_key.source(), search_key.destination());
    Leaf* leaf = entry.leaf();
    int64_t segment_id = entry.segment_id();
    prof_index.stop();

    reader_enter_impl(search_key, leaf, segment_id);
}

void Context::reader_direct_access(Key search_key, DirectPointer& pointer){
    profiler::ScopedTimer profiler { profiler::CONTEXT_READER_DIRECT_ACCESS };

    bool success = false;

    if(pointer.leaf() != nullptr){ // is the direct pointer set?
        try {
            reader_enter_impl(search_key, pointer.leaf(), pointer.get_segment_id() );
            if(m_segment != pointer.segment() || (pointer.has_filepos() && m_segment->get_version() != pointer.get_segment_version())){
                pointer.unset();
            }
            success = true;
        } catch( Abort ){
            // we're going to fallback to the index
            pointer.unset(); // invalidate the pointer
        }
    }

    while(!success){
        try {
            reader_enter(search_key);
            success = true;
        } catch ( Abort ) {
            /* try again ... */
        }
    }
}

void Context::reader_enter_impl(Key search_key, Leaf* leaf, int64_t segment_id){
    profiler::ScopedTimer profiler { profiler::CONTEXT_READER_ENTER_BROWSE };

    auto tcntxt = context::thread_context();
    Segment* segment = nullptr;
    bool done = false;
    do {
        if(check_fence_keys(leaf, &segment_id, search_key)){ // unsafe check, without the lock. We need to check again once we acquire the latch
            segment = leaf->get_segment(segment_id);

            // Bug fix 30/May/2020: we cannot be fair in the usage of the latch when this thread already holds other
            // latches. There is a potential source of deadlocks in presence of nested iterators: this thread can try
            // to access the same segment twice, and a writer may be in between the two read accesses, causing a deadlock.
            bool fair_lock = tcntxt->num_reader_latches() == 0;

            segment->reader_enter(fair_lock); // it can raise an exception (too many readers)

            m_leaf = leaf;
            m_segment = segment;

            // check again the fence keys are correct, after having locked the segment
            auto rc = leaf->check_fence_keys(segment_id, search_key);
            if(rc == FenceKeysDirection::OK){
                assert(segment->get_state() == Segment::State::READ && "We didn't acquire a read lock to the segment?");
                done = true;
            } else { // we failed
                segment->reader_exit(); // it expects m_leaf & m_segment already set

                m_leaf = nullptr;
                m_segment = nullptr;

                handle_fence_keys_direction(rc, &segment_id); // it can throw an abort
            }
        }
    } while(!done);

    tcntxt->incr_num_reader_latches();

    if(context::StaticConfiguration::memstore_prefetch){
        // the first two blocks
        uint64_t* block1 = reinterpret_cast<uint64_t*>(sparse_file());
        uint64_t* block2 = block1 + 8;

        util::prefetch(block1);
        util::prefetch(block2);
    }
}

void Context::reader_next(Key search_key){
    profiler::ScopedTimer profiler { profiler::CONTEXT_READER_NEXT };

    // if the leaf or the segment are not present, it means we already left the previous segment
    assert(m_leaf != nullptr && "No leaf set");
    assert(m_segment != nullptr && "No segment set");

    Leaf* leaf = m_leaf;
    int64_t segment_id = this->segment_id();

    reader_exit();

    segment_id++; // next segment
    if(segment_id < (int64_t) leaf->num_segments()){
        reader_enter_impl(search_key, leaf, segment_id);
    } else {
        reader_enter(search_key);
    }
}

void Context::reader_exit(){
    profiler::ScopedTimer profiler { profiler::CONTEXT_READER_EXIT };
    assert(m_leaf != nullptr && m_segment != nullptr);

    m_segment->reader_exit();

    m_leaf = nullptr;
    m_segment = nullptr;

    context::thread_context()->decr_num_reader_latches();
}

/*****************************************************************************
 *                                                                           *
 *   Optimistic readers                                                      *
 *                                                                           *
 *****************************************************************************/
void Context::optimistic_enter(Key search_key){
    profiler::ScopedTimer profiler { profiler::CONTEXT_OPTIMISTIC_ENTER };

    context::ThreadContext* context = context::thread_context();
    assert(context != nullptr);
    context->epoch_enter();

    IndexEntry entry = m_tree->index()->find(search_key.source(), search_key.destination());
    Leaf* leaf = entry.leaf();
    int64_t segment_id = entry.segment_id();

    optimistic_enter_impl(search_key, leaf, segment_id);
}

void Context::optimistic_bump(Key search_key){
    Leaf* leaf = m_leaf;
    int64_t segment_id = this->segment_id();
    optimistic_reset();
    optimistic_enter_impl(search_key, leaf, segment_id);
}

void Context::optimistic_enter_impl(Key search_key, Leaf* leaf, int64_t segment_id){
    uint64_t version = 0;
    Segment* segment = nullptr;
    bool done = false;

    do {
        // unsafe check, without holding the segment's lock. We need to validate it again
        if(check_fence_keys(leaf, &segment_id, search_key)){ // it can raise an abort
            segment = leaf->get_segment(segment_id);
            version = segment->optimistic_enter();

            // validate this is the correct segment
            auto rc = leaf->check_fence_keys(segment_id, search_key);
            segment->optimistic_validate(version); // it can raise an abort
            if(rc == FenceKeysDirection::OK){
                done = true;
            } else {
                handle_fence_keys_direction(rc, &segment_id); // it can raise an abort
            }
        }
    } while (!done);

    m_leaf = leaf;
    m_segment = segment;
    m_version = version;
}

void Context::optimistic_next(Key search_key){
    profiler::ScopedTimer profiler { profiler::CONTEXT_OPTIMISTIC_NEXT };

    // if the leaf or the segment are not present, it means we already left the previous segment
    assert(m_leaf != nullptr && "No leaf set");
    assert(m_segment != nullptr && "No segment set");

    Leaf* leaf = m_leaf;
    int64_t segment_id = this->segment_id();

    //optimistic_exit();
    optimistic_reset();

    segment_id++; // next segment
    if(segment_id < (int64_t) leaf->num_segments()){
        optimistic_enter_impl(search_key, leaf, segment_id);
    } else {
        optimistic_enter(search_key);
    }
}

void Context::optimistic_exit() {
    validate_version();
    optimistic_reset();
}

void Context::optimistic_reset() {
    m_leaf = nullptr;
    m_segment = nullptr;
    m_version = numeric_limits<uint64_t>::max();
}

/*****************************************************************************
 *                                                                           *
 *   Async rebalancers                                                       *
 *                                                                           *
 *****************************************************************************/

void Context::async_rebalancer_enter(Key search_key, rebalance::Crawler* crawler) {
    context::ThreadContext* context = context::thread_context();
    assert(context != nullptr);

    while(true) {
        context->epoch_enter();

        try {
            IndexEntry entry = m_tree->index()->find(search_key.source(), search_key.destination());
            Leaf* leaf = entry.leaf();
            int64_t segment_id = entry.segment_id();

            while(true) {
                if(check_fence_keys(leaf, &segment_id, search_key)){ // it can raise an abort
                    m_leaf = leaf;
                    m_segment = leaf->get_segment(segment_id);
                    Segment::async_rebalancer_enter(*this, search_key, crawler);

                    return; // done
                }
            }

        } catch( Abort ){
            // try again ...
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *   Fence keys (helpers)                                                    *
 *                                                                           *
 *****************************************************************************/
bool Context::check_fence_keys(Leaf* leaf, int64_t* /* in/out*/ segment_id, Key search_key){
    assert(segment_id != nullptr && "Null pointer (segment_id)");
    auto rc = leaf->check_fence_keys(*segment_id, search_key);
    return handle_fence_keys_direction(rc, segment_id);
}

bool Context::handle_fence_keys_direction(FenceKeysDirection rc, int64_t* /* in/out*/ segment_id){
    switch(rc){
    case FenceKeysDirection::INVALID:
        throw Abort {};
    case FenceKeysDirection::LEFT:
        if(*segment_id == 0){
            throw Abort{};
        } else {
            *segment_id = *segment_id -1;
        }
        return false;
    case FenceKeysDirection::OK:
        return true;
    case FenceKeysDirection::RIGHT:
        if(*segment_id == static_cast<int64_t>(Leaf::num_segments()) -1){
            throw Abort{}; // next leaf
        } else {
            *segment_id = *segment_id +1;
        }
        return false;
    default:
        assert(0 && "Invalid case");
        return false;
    }
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
    if(context.m_version != numeric_limits<uint64_t>::max()){
        out << ", version set: " << context.m_version;
    }
    out << "]";
    return out;
}

} // namespace
