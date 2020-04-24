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

#include "teseo/rebalance/merge_operator.hpp"

#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/rebalance/crawler.hpp"
#include "teseo/rebalance/plan.hpp"
#include "teseo/rebalance/scratchpad.hpp"
#include "teseo/rebalance/spread_operator.hpp"
#include "teseo/util/thread.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"


using namespace std;

namespace teseo::rebalance {

MergeOperator::MergeOperator(const memstore::Context& context) : m_context(context), m_scratchpad(nullptr){
    m_scratchpad = new ScratchPad(context::StaticConfiguration::memstore_segment_size); // okay, even though it's in terms of elements
}

MergeOperator::~MergeOperator(){
    delete m_scratchpad; m_scratchpad = nullptr;
}

void MergeOperator::execute(){
    COUT_DEBUG("init");
    profiler::ScopedTimer profiler { profiler::MERGER_EXECUTE };

    memstore::Key key = memstore::KEY_MIN; // the min fence key for the current chunk
    memstore::Leaf* previous = nullptr; // the last visited chunk
    uint64_t prev_sz = 0; // the number of slots occupied in the previous chunk
    const uint64_t MERGE_THRESHOLD = 0.5 * context::StaticConfiguration::memstore_segment_size * context::StaticConfiguration::memstore_num_segments_per_leaf; // 50%

    do {
        context::ScopedEpoch epoch; // protect from the GC, before using #index_find()

        memstore::IndexEntry entry = m_context.m_tree->index()->find(key.source(), key.destination());
        memstore::Leaf* current = entry.leaf();
        m_context.m_leaf = current;
        uint64_t cur_sz = visit_and_prune();
        bool has_merged = false;

        // check #1: before acquiring any lock, can we, say at least hope to merge the previous &
        // current chunks together?
        assert(previous != current && "As only this service can merge together chunks, we cannot jump again on the same chunk twice");
        if(
                previous != nullptr && /* do we have a previous chunk ? */
                prev_sz + cur_sz < MERGE_THRESHOLD /* the space used is less than 50% */
                && previous->get_hfkey() == key /* previous & current are still siblings, i.e. no leaf splits occurred in the meanwhile */
        ) {

            memstore::Context ctxt_previous { m_context.m_tree };
            ctxt_previous.m_leaf = previous;
            Crawler crawler_previous { ctxt_previous };

            memstore::Context ctxt_current { m_context.m_tree };
            ctxt_current.m_leaf = current;
            Crawler crawler_current { ctxt_current };

            crawler_previous.lock2merge();
            crawler_current.lock2merge();

            prev_sz = crawler_previous.used_space();
            cur_sz = crawler_current.used_space();

            // check #2: this time we should have a better estimate of the actual size of both previous & current
            if(prev_sz + cur_sz < MERGE_THRESHOLD && previous->get_hfkey() == key) {
                m_context.m_leaf = previous; // we're going to save the previous leaf
                prev_sz = merge(previous, current, crawler_previous.cardinality() + crawler_current.cardinality());
                crawler_current.invalidate();
                m_context.m_tree->global_context()->gc()->mark(current, memstore::destroy_leaf);
                has_merged = true;
            }
        }

        if(!has_merged){  // next iteration
            key = current->get_hfkey();
            previous = current;
            prev_sz = cur_sz;
        }

        m_context.m_leaf = nullptr;
    } while (key != memstore::KEY_MAX);

    COUT_DEBUG("done");
}

uint64_t MergeOperator::visit_and_prune(){
    profiler::ScopedTimer profiler { profiler::MERGER_VISIT_AND_PRUNE };

    uint64_t cur_sz = 0; // number of slots in use in the chunk
    for(uint64_t segment_id = 0; segment_id < context::StaticConfiguration::memstore_num_segments_per_leaf; segment_id++){
        memstore::Segment* segment = m_context.m_leaf->get_segment(segment_id);
        m_context.m_segment = segment;

        xlock();

        if(segment->is_sparse()){
            memstore::SparseFile* sf = m_context.sparse_file();
            sf->prune();
            segment->cancel_rebalance_request();
        }

        cur_sz += segment->used_space();

        xunlock();
    }

    m_context.m_segment = nullptr;
    return cur_sz;
}

void MergeOperator::xlock(){
    memstore::Segment* segment = m_context.m_segment;
    assert(segment != nullptr);

    bool done = false;

    do {
        unique_lock<memstore::Segment> lock(*segment);
        switch(segment->get_state()){
        case memstore::Segment::State::FREE:
            assert(segment->get_num_active_threads() == 0 && "Precondition not satisfied");
            segment->set_state( memstore::Segment::State::WRITE );
            segment->incr_num_active_threads();
#if !defined(NDEBUG) /* for debugging purposes only */
            segment->m_writer_id = util::Thread::get_thread_id();
#endif
            done = true;
            break;
        case memstore::Segment::State::READ:
        case memstore::Segment::State::WRITE:
        case memstore::Segment::State::REBAL:
            segment->wait<memstore::Segment::State::WRITE>(lock);
        }
    } while(!done);
}

void MergeOperator::xunlock(){
    memstore::Leaf* leaf = m_context.m_leaf;
    m_context.writer_exit();
    m_context.m_leaf = leaf;
}

uint64_t MergeOperator::merge(memstore::Leaf* previous, memstore::Leaf* current, uint64_t cardinality){
    profiler::ScopedTimer profiler { profiler::MERGER_MERGE };
    COUT_DEBUG("leaf 1: " << previous << ", leaf 2: " << current);
    Plan plan = Plan::create_merge(cardinality, previous, current);
    SpreadOperator spread { m_context, *m_scratchpad, plan };
    spread();

    uint64_t filled_space = 0;
    for(uint64_t segment_id = 0; segment_id < context::StaticConfiguration::memstore_num_segments_per_leaf; segment_id++){
        filled_space += m_context.m_leaf->get_segment(segment_id)->used_space();
    }
    return filled_space;
}
} // namespace


