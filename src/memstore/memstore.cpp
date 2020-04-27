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

#include <iostream>
#include <sstream>
#include <string>

#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/remove_vertex.hpp"
#include "teseo/memstore/sparse_file.hpp"
#include "teseo/memstore/update.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/rebalance/merger_service.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"
#include "teseo/util/error.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;


namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
Memstore::Memstore(context::GlobalContext* global_context, bool is_directed) :
        m_is_directed(is_directed), m_index(new Index()), m_global_context(global_context),
        m_merger( nullptr ) {

    COUT_DEBUG("num segments per leaf: " << context::StaticConfiguration::memstore_num_segments_per_leaf);
    COUT_DEBUG("segment size: " << context::StaticConfiguration::memstore_segment_size << " words");

    // Create an empty leaf
    Leaf* leaf = create_leaf();
    leaf->set_lfkey(KEY_MIN);
    context::ScopedEpoch epoch; // before operating in the index, we always must have already a thread context and a gc running ...
    m_index->insert(0, 0, IndexEntry(leaf, 0));

    // Start the merger
    m_merger = new rebalance::MergerService(this);
    m_merger->start();;
}

Memstore::~Memstore() {
    COUT_DEBUG("Terminated");
    delete m_merger; m_merger = nullptr;
    delete m_index; m_index = nullptr;
}

void Memstore::clear(){
    m_merger->stop();

    COUT_DEBUG("Removing all leaves & pending undos...");
    auto deleter = [](Leaf* leaf){ destroy_leaf(leaf); };
    context::ScopedEpoch epoch; // index_find() requires being inside an epoch

    Key key = KEY_MIN; // KEY_MIN is always present in the index
    Context context { this };
    do {
        IndexEntry e = m_index->find(key.source(), key.destination());
        Leaf* leaf = e.leaf();
        context.m_leaf = leaf;

        // remove all pending transaction undo's
        for(uint64_t segment_id = 0; segment_id < leaf->num_segments(); segment_id++){
            context.m_segment = leaf->get_segment(segment_id);
            Segment::clear_versions(context);
        }
        context.m_segment = nullptr;

        // next iteration
        key = leaf->get_hfkey();

        context.m_leaf = nullptr;
        context::global_context()->gc()->mark(e.leaf(), deleter);
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

rebalance::MergerService* Memstore::merger(){
    return m_merger;
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
    profiler::ScopedTimer profiler { profiler::MEMSTORE_INSERT_VERTEX };

    Context context { this, transaction };

    // First, insert the update in the undo, flagged as remove
    Update update { true, false, Key(vertex_id) };
    assert(update.is_remove());
    transaction->add_undo(this, update);

    // Now, set the update as an insertion
    update.flip();

    // Jump to the write impl~
    write(context, update);
}

uint64_t Memstore::remove_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id, std::vector<uint64_t>* out_edges){
    profiler::ScopedTimer profiler { profiler::MEMSTORE_REMOVE_VERTEX };

    Context context { this, transaction };
    RemoveVertex remover{context, vertex_id, out_edges};
    return remover();
}

void Memstore::insert_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination, double weight){
    profiler::ScopedTimer profiler { profiler::MEMSTORE_INSERT_EDGE };
    if(source == destination) throw Error { Key { source, destination }, Error::EdgeSelf };

    Context context { this, transaction };

    COUT_DEBUG(source << " -> " << destination << ", weight: " << weight);

    // First, insert the update in the undo, flagged as deletion
    Update update { /* vertex ? */ false, /* insert ? */ false, Key(source, destination), weight };
    transaction->add_undo(this, update);

    // Now, set the update as an insertion
    update.flip();
    assert(update.is_insert() && update.is_edge() && update.source() == source && update.destination() == destination && update.weight() == weight);

    if(is_directed()){
        // explicitly check whether the destination vertex exists
        if(!has_item(context, Key{ destination }, /* unlocked ? */ true)){ throw Error{ Key{ destination }, Error::VertexDoesNotExist }; }

        // perform the update, the routine #update_edge ensures  that the source vertex exists
        do_insert_edge(context, update);
    } else {
        // first, insert the edge source -> destination. The first call will ensure that source exists
        do_insert_edge(context, update);

        // second, insert the edge destination -> source. This call will ensure that destination exists
        update.swap();
        update.flip();
        transaction->add_undo(this, update);
        update.flip();
        assert(update.is_insert() && update.is_edge() && update.source() == destination && update.destination() == source && update.weight() == weight);

        try {
            do_insert_edge(context, update);
        } catch(...){
            transaction->do_rollback(1); // revert the insertion source -> destination first
            throw;
        }
    }
}

void Memstore::do_insert_edge(Context& context, const Update& update){
    // we don't make any assumption whether the destination of the edge already exists, this need to be checked independently

    try {
        // insert/remove the edge, the writer will try to ensure the source already exists `a la best effort'
        write(context, update, /* source vertex exists ? */ false);
    } catch (NotSureIfItHasSourceVertex){
        // okay, this means that the writer is not sure whether the source vertex exists, we need to check for it explicitly
        if(!has_item(context, Key{ update.source() }, /* unlocked ? */ true)){
            throw Error{ Key{ update.source() }, Error::VertexDoesNotExist };
        }

        write(context, update, /* source vertex exists ? */ true);
    }
}

void Memstore::remove_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination){
    profiler::ScopedTimer profiler { profiler::MEMSTORE_REMOVE_EDGE };
    COUT_DEBUG(source << " -> " << destination);

    Context context { this, transaction };

    // First, insert the update in the undo, flagged as insert
    Update update { /* vertex ? */ false, /* insert ? */ true, Key(source, destination) };
    transaction->add_undo(this, update);

    // Now, flag the update as a deletion
    update.flip();
    assert(update.is_remove() && update.is_edge());

    // Jump to the write impl~
    write(context, update);

    if(is_undirected()){ // undirected graphs actually store two edges a -> b and b -> a
        update.swap();

        // Add the update in the undo buffer
        update.flip();
        assert(update.is_insert() && "As we are removing an edge, the second item in the history must be an insertion");
        transaction->add_undo(this, update);

        // This time, the update for the storage is a deletion
        update.flip();
        assert(update.is_remove());

        try {
            write(context, update); // remove ( b -> a )
        } catch (...){
            // revert the first deletion, that is a -> b
            transaction->do_rollback(1);

            throw;
        }
    } // undirected
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
            context.m_segment->update(context, update, has_source_vertex);

            // Release the xlock to the segment
            context.writer_exit();

            // Exit from the while loop
            done = true;
        } catch( Abort ) {
            assert(context.m_segment == nullptr && "Segment still locked");
        } catch( ... ) {
            if(context.m_segment != nullptr){ context.writer_exit(); }  // release the lock on the segment
            throw;
        }
    } while (!done);
}

void Memstore::do_rollback(void* undo_payload, transaction::Undo* next) {
    profiler::ScopedTimer profiler { profiler::MEMSTORE_ROLLBACK };
    if(undo_payload == nullptr) RAISE_EXCEPTION(InternalError, "Undo record missing");
    Context context { this };
    const Update& update = *(reinterpret_cast<Update*>(undo_payload));

    // similarly to #write, we need to gain exclusive access to the interested segment in sparse array
    bool done = false;

    do {
        context::ScopedEpoch epoch;

        try {
            context.writer_enter(update.key());
            context.m_segment->rollback(context, update, next);
            context.writer_exit();
            done = true;
        } catch ( Abort ) {
            /* nop, segment being rebalanced in the meanwhile, retry ...  */
            assert(context.m_segment == nullptr && "Segment still locked");
        } catch ( ... ){
            assert(0 && "This code path should never fail!");
            throw;
        }

    } while(!done);
}

/*****************************************************************************
 *                                                                           *
 *   Point look ups                                                          *
 *                                                                           *
 *****************************************************************************/

bool Memstore::has_vertex(transaction::TransactionImpl* transaction, uint64_t vertex_id) const {
    profiler::ScopedTimer profiler { profiler::MEMSTORE_HAS_VERTEX };
    Context context { const_cast<Memstore*>(this), transaction };
    return has_item(context, Key { vertex_id });
}

bool Memstore::has_edge(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination) const {
    profiler::ScopedTimer profiler { profiler::MEMSTORE_HAS_EDGE };
    Context context { const_cast<Memstore*>(this), transaction };
    return has_item(context, Key {source, destination} );
}

bool Memstore::has_item(Context& context, const Key& key, bool is_unlocked) const {
    do {
        context::ScopedEpoch epoch;

        try {
            context.optimistic_enter(key);
            bool result = context.m_segment->has_item_optimistic(context, key, is_unlocked);
            context.optimistic_exit();

            return result; // done
        } catch (Abort) {
            /* retry ...  */
            context.optimistic_reset();
        } catch ( ... ){
            // before throwing an error to the user, check we didn't read rubbish and the error was actually
            // caused by the rubbish we read
            bool genuine_error = context.m_segment->m_latch.is_version(context.m_version);
            context.optimistic_reset();
            if(genuine_error) throw;

            // otherwise try again...
        }

    } while(true);
}

double Memstore::get_weight(transaction::TransactionImpl* transaction, uint64_t source, uint64_t destination) const{
    profiler::ScopedTimer profiler { profiler::MEMSTORE_GET_WEIGHT };
    Context context { const_cast<Memstore*>(this), transaction };
    Key key { source, destination };

    do {
        context::ScopedEpoch epoch;

        try {
            context.optimistic_enter(key);
            double result = context.m_segment->get_weight_optimistic(context, key);
            context.optimistic_exit();

            return result; // done
        } catch (Abort) {
            /* retry ...  */
            context.optimistic_reset();
        } catch ( ... ){
            // before throwing an error to the user, check we didn't read rubbish and the error was actually
            // caused by the rubbish we read
            bool genuine_error = context.m_segment->m_latch.is_version(context.m_version);
            context.optimistic_reset();
            if(genuine_error) throw;

            // otherwise try again...
        }
    } while(true);
}

/*****************************************************************************
 *                                                                           *
 *   Debug & dump                                                            *
 *                                                                           *
 *****************************************************************************/
string Memstore::str_undo_payload(const void* object) const {
    const Update* update = reinterpret_cast<const Update*>(object);
    stringstream ss;
    if(update == nullptr){
        ss << "nullptr";
    } else {
        ss << *update;
    }
    return ss.str();
}

void Memstore::dump() const {
    //m_async_rebal->stop();
    //m_merger->stop();

    context::ScopedEpoch epoch; // index find requires being inside an epoch

    cout << "[Memstore] directed: " << boolalpha << is_directed() << ", ";
    cout << "num segments per leaf: " << context::StaticConfiguration::memstore_num_segments_per_leaf << ", ";
    cout << "segment size: " << context::StaticConfiguration::memstore_segment_size << " qwords\n";

    cout << "Index: \n";
    m_index->dump();

    uint64_t num_leaves = 0;
    bool integrity_check = true;
    cout << "\nLeaves: " << endl;

    Context context { const_cast<Memstore*>(this) };

    IndexEntry entry = m_index->find(0);
    assert(entry.segment_id() == 0 && "The first entry should always be the first segment");

    context.m_leaf = entry.leaf();
    while(context.m_leaf != nullptr && integrity_check){
        Leaf::dump_and_validate(cout, context, &integrity_check);
        num_leaves++; // number of leaves visited so far

        // next leaf
        auto next_key = context.m_leaf->get_hfkey();
        if(next_key != KEY_MAX){
            entry = m_index->find(next_key.source(), next_key.destination());
            assert(entry.segment_id() == 0 && "The first entry should always be the first segment");
            Leaf* next = entry.leaf();
            assert(context.m_leaf != next && "Infinite loop");
            assert(next_key == next->get_lfkey() && "Fence keys mismatch");

            // next iteration
            context.m_leaf = next;
        } else { // done
            context.m_leaf = nullptr;
        }
    }

    cout << "Number of visited leaves: " << num_leaves << endl;
    if(!integrity_check){
        cout << "\n!!! INTEGRITY CHECK FAILED !!!" << endl;
        assert(false && "Integrity check failed");
    }

    //m_merger->start();
    //m_async_rebal->start();
}

} // namespace

