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

//#define DEBUG
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

    memstore::Leaf* previous_leaf = nullptr; // the last visited chunk
    memstore::Key previous_key = memstore::KEY_MIN; // the min fence key for the previous chunk
    uint64_t previous_size = 0; // the number of slots occupied in the previous chunk
    memstore::Key current_key = memstore::KEY_MIN; // the min fence key for the current chunk
    constexpr uint64_t MERGE_THRESHOLD = 0.75 * memstore::SparseFile::max_num_qwords() * context::StaticConfiguration::memstore_max_num_segments_per_leaf; // 75%

    do {
        context::ScopedEpoch epoch; // protect from the GC, before using #index_find()
        bool abort_on_previous = false; // discriminate whether the Abort{} raised originated from `previous_leaf' or `current_leaf'

        try {
            memstore::IndexEntry current_entry = m_context.m_tree->index()->find(current_key.source(), current_key.destination());
            memstore::Leaf* current_leaf = current_entry.leaf();
            m_context.m_leaf = current_leaf;
            uint64_t current_size = visit_and_prune(); // it can Abort{} if `current' is not valid anymore

            // check #1: before acquiring any lock, can we, say at least hope to merge the previous &
            // current chunks together?
            if( previous_leaf != nullptr && /* do we have a previous chunk ? */
                previous_size + current_size <= MERGE_THRESHOLD && /* the space used is less than 75% */
                previous_leaf != current_leaf /* the sibling of `previous' was not relinked yet in the index due to a resize */
            ) {
                COUT_DEBUG("[candidates] previous: " << previous_leaf << ", current: " << current_leaf);

                // We need to repeat the search for previous because we released the epoch at the start of the while loop and the previous leaf
                // may have been deleted in the meanwhile by a resize
                memstore::IndexEntry previous_entry = m_context.m_tree->index()->find(previous_key.source(), previous_key.destination());
                previous_leaf = previous_entry.leaf();

                // Lock previous
                abort_on_previous = true;
                memstore::Context ctxt_previous { m_context.m_tree };
                ctxt_previous.m_leaf = previous_leaf;
                Crawler previous_crawler { ctxt_previous };
                previous_crawler.lock2merge(); // it can raise an Abort
                previous_size = previous_crawler.used_space(); // update the space used
                previous_key = previous_leaf->get_lfkey(); // update the min fence key for the next iterations, in case it is needed
                abort_on_previous = false;

                // Lock current
                memstore::Context ctxt_current { m_context.m_tree };
                ctxt_current.m_leaf = current_leaf;
                Crawler current_crawler { ctxt_current };
                current_crawler.lock2merge(); // it can raise an Abort
                current_size = current_crawler.used_space(); // update the space used

                // Check that previous and current are siblings. This check is safe because both leaves are now fully locked
                if(previous_leaf->get_hfkey() /* = current_key */ != current_leaf->get_lfkey()){ throw Abort{}; }

                // check #2: this time we should have a better estimate of the actual size of both previous & current
                if(previous_size + current_size <= MERGE_THRESHOLD) {
                    previous_crawler.set_lock_ownership(false); // transfer the locks to the spread operator
                    current_crawler.set_lock_ownership(false);

                    std::tie(current_leaf, current_size) = merge(previous_leaf, current_leaf, previous_crawler.cardinality() + current_crawler.cardinality(), previous_size + current_size);
                }
            }

            // next iteration
            previous_leaf = current_leaf;
            previous_key = current_key;
            previous_size = current_size;
            current_key = current_leaf->get_hfkey();

        } catch (Abort){ // try again
            if(abort_on_previous){ // reset previous
                previous_leaf = nullptr;
                previous_size = 0; // it doesn't matter, ease debug
                previous_key = memstore::KEY_MIN; // it doesn't matter, ease debug
            }
        }

        m_context.m_leaf = nullptr;
        m_context.m_segment = nullptr;
    } while (current_key != memstore::KEY_MAX);

    COUT_DEBUG("done");
}

uint64_t MergeOperator::visit_and_prune(){
    profiler::ScopedTimer profiler { profiler::MERGER_VISIT_AND_PRUNE };

    uint64_t cur_sz = 0; // number of slots in use in the chunk
    for(uint64_t segment_id = 0; segment_id < m_context.m_leaf->num_segments(); segment_id++){
        memstore::Segment* segment = m_context.m_leaf->get_segment(segment_id);
        m_context.m_segment = segment;
        cur_sz += memstore::Segment::prune(m_context);
    }

    m_context.m_segment = nullptr;
    return cur_sz;
}

std::pair</* last leaf */ memstore::Leaf*, /* space used */ uint64_t> MergeOperator::merge(memstore::Leaf* previous, memstore::Leaf* current, uint64_t cardinality, uint64_t used_space) {
    profiler::ScopedTimer profiler { profiler::MERGER_MERGE };
    COUT_DEBUG("leaf 1: " << previous << ", leaf 2: " << current);
    Plan plan = Plan::create_merge(cardinality, used_space, previous, current);
    SpreadOperator spread { m_context, *m_scratchpad, plan};
    spread();


    // Compute the amount of used space in the last leaf of the interval rebalanced
    // This operation is thread-safe because the spread operator still holds the locks to the leaves rebalanced
    memstore::Context context { m_context.m_tree };
    memstore::Leaf* last = context.m_leaf = spread.last_leaf();
    uint64_t filled_space = 0;
    for(uint64_t segment_id = 0; segment_id < last->num_segments(); segment_id++){
        context.m_segment = last->get_segment(segment_id);
        filled_space += memstore::Segment::used_space(context);
    }

    return make_pair(last, filled_space);
}

} // namespace


