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

#include "teseo/memstore/memstore.hpp"


#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/debug.hpp"

using namespace std;


namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
Memstore::Memstore(context::GlobalContext* global_context, bool is_directed) :
        m_is_directed(is_directed), m_index(new Index()), m_global_context(global_context) {
    COUT_DEBUG("qwords per segment (excl header): " << get_num_qwords_per_segment());
    COUT_DEBUG("num segments per gate: " << get_num_segments_per_lock());
    COUT_DEBUG("qwords per gate: " << get_num_qwords_per_gate());

    // Create an empty leaf
    Leaf* leaf = create_leaf();
    leaf->set_lfkey(KEY_MIN);
    context::ScopedEpoch epoch; // before operating in the index, we always must have already a thread context and a gc running ...
    m_index->insert(0, 0, IndexEntry(leaf, 0));

    // FIXME
//    // Start the merger
//    m_merger = new MergerService(this, 60s);
//    m_merger->start();
//
//    // Start the asynchronous rebalancer
//    m_async_rebal = new AsyncRebalancerService(this);
//    m_async_rebal->start();
}

Memstore::~Memstore() {
    COUT_DEBUG("Terminated");
    // FIXME
//    delete m_async_rebal; m_async_rebal = nullptr;
//    delete m_merger; m_merger = nullptr;
    delete m_index; m_index = nullptr;
}

void Memstore::clear(){
    // FIXME
//    m_async_rebal->stop();
//    m_merger->stop();

    COUT_DEBUG("Removing all chunks & pending undos...");
    auto deleter = [this](Leaf* leaf){ destroy_leaf(leaf); };
    context::ScopedEpoch epoch; // index_find() requires being inside an epoch

    Key key = KEY_MIN; // KEY_MIN is always present in the index
    do {
        IndexEntry e = m_index->find(key.source(), key.destination());
        Leaf* leaf = e.leaf();

        // remove all pending transaction undo's
        for(uint64_t segment_id = 0; segment_id < leaf->num_segments(); segment_id++){
            SparseFile* sf = Context::sparse_file(leaf, segment_id);
            sf->clear_versions();
        }

        // next iteration
        key = leaf->get_hfkey();

        context::global_context()->gc()->mark(e.leaf, deleter); // directly release the chunk
    } while(key != KEY_MAX);
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

context::GlobalContext* Memstore::global_context(){
    return m_global_context;
}

bool Memstore::is_directed() const {
    return m_is_directed;
}

bool Memstore::is_undirected() const {
    return !is_directed();
}

/*****************************************************************************
 *                                                                           *
 *   Updates                                                                 *
 *                                                                           *
 *****************************************************************************/

void Memstore::insert_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id) {
    profiler::ScopedTimer profiler { profiler::SA_INSERT_VERTEX };

    Context context { this, transaction };

    // First, insert the update in the undo, flagged as remove
    Update update { true, false, Key(vertex_id) };
    transaction->add_undo(this, update);

    // Now, set the update as an insertion
    update.flip();

    // Jump to the write impl~
    write(context, update);
}

void Memstore::write(Context& context, const Update& update, bool has_source_vertex) {
    assert(context.m_transaction != nullptr && "Transaction not given");
    assert(!context.m_transaction->is_terminated() && "The given transaction is already terminated");

    bool done = false;

    do {
        context::ScopedEpoch epoch;

        try {
            // Acquire an xlock to the segment we're going to alter
            context.writer_enter(update.key());

            // Perform the update
            done = context.sparse_file()->update(context, update, has_source_vertex);

            // Release the xlock to the segment
            context.writer_exit();
        } catch (Abort){
            if(context.m_segment != nullptr){ context.writer_exit(); }  // release the lock on the segment
        } catch(...){
            if(context.m_segment != nullptr){ context.writer_exit(); }  // release the lock on the segment
            throw;
        }
    } while (!done);
}


} // namespace

