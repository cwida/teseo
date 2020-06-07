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

#include "teseo/aux/dynamic_view.hpp"

#include <limits>
#include <mutex>

#include "teseo/aux/builder.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/numa.hpp"
#include "teseo/util/thread.hpp"

using namespace std;
using namespace teseo::util;

using scoped_lock_t = std::lock_guard<OptimisticLatch<0>>;

namespace teseo::aux {

DynamicView::DynamicView(CountingTree&& tree) : View(/* is static ? */ false), m_tree(move(tree)) {

}

DynamicView::~DynamicView(){

}

DynamicView* DynamicView::create_undirected(memstore::Memstore* memstore, transaction::TransactionImpl* transaction) {
    profiler::ScopedTimer profiler { profiler::AUX_DYNAMIC_CREATE };
    assert(!transaction->is_read_only() && "Expected a read-write transaction");

    Builder builder;
    memstore->aux_view(transaction, &builder);
    CountingTree* ct = builder.create_ct_undirected();

    // it doesn't matter where the view is allocated, the inodes/leaves of the CountingTree were still allocated with ::malloc
    void* heap = util::NUMA::malloc(sizeof(DynamicView));
    DynamicView* view = new (heap) DynamicView{ move(*ct) };

    delete ct; ct = nullptr;

    return view;
}

uint64_t DynamicView::vertex_id(uint64_t logical_id) const noexcept {
    // avoid to mess up with an epoch previously set
    auto tcntxt = context::thread_context();
    const bool acquire_epoch = tcntxt->epoch() == numeric_limits<uint64_t>::max();

    while(true){
        try { // the beauty of optimistic latches, they say...
            if(acquire_epoch) tcntxt->epoch_enter(); // protect from the GC

            ItemUndirected item;
            uint64_t version = m_latch.read_version();
            bool found = m_tree.get_by_rank_optimistic(logical_id, m_latch, version, &item);

            if(acquire_epoch) tcntxt->epoch_exit();

            if(found){
                return item.m_vertex_id;
            } else {
                return NOT_FOUND;
            }

        } catch(Abort) {
            // and try again...
        }
    }
}

uint64_t DynamicView::logical_id(uint64_t vertex_id) const noexcept {
    // avoid to mess up with an epoch previously set
    auto tcntxt = context::thread_context();
    const bool acquire_epoch = tcntxt->epoch() == numeric_limits<uint64_t>::max();

    while(true){
        try { // the beauty of optimistic latches, they say...
            if(acquire_epoch) tcntxt->epoch_enter(); // protect from the GC

            uint64_t rank { NOT_FOUND };
            uint64_t version = m_latch.read_version();
            bool found = m_tree.get_by_vertex_id_optimistic(vertex_id, m_latch, version, nullptr, &rank);

            if(acquire_epoch) tcntxt->epoch_exit();

            if(found){
                return rank;
            } else {
                return NOT_FOUND;
            }

        } catch(Abort) {
            // and try again...
        }
    }
}

uint64_t DynamicView::degree(uint64_t id, bool is_logical) const noexcept {
    // avoid to mess up with an epoch previously set
    auto tcntxt = context::thread_context();
    const bool acquire_epoch = tcntxt->epoch() == numeric_limits<uint64_t>::max();

    while(true){
        try { // the beauty of optimistic latches, they say...
            if(acquire_epoch) tcntxt->epoch_enter(); // protect from the GC

            bool found { false };
            ItemUndirected item;

            uint64_t version = m_latch.read_version();

            if(is_logical){
                found = m_tree.get_by_rank_optimistic(id, m_latch, version, &item);
            } else {
                found = m_tree.get_by_vertex_id_optimistic(id, m_latch, version, &item);
            }

            if(acquire_epoch) tcntxt->epoch_exit();

            if(found){
                return item.m_degree;
            } else {
                return NOT_FOUND;
            }
        } catch(Abort) {
            // and try again...
        }
    }
}

uint64_t DynamicView::num_vertices() const noexcept {
    while(true){
        try {
            uint64_t version = m_latch.read_version();
            uint64_t result = m_tree.size();
            m_latch.validate_version(version);
            return result;
        } catch(Abort){
            // and try again ...
        }
    }
}

memstore::IndexEntry DynamicView::direct_pointer(uint64_t id, bool is_logical) const {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Expected to be already set by the caller");

    while(true){
        try { // the beauty of optimistic latches, they say...
            bool found { false };
            ItemUndirected item;
            uint64_t version = m_latch.read_version();

            if(is_logical){
                found = m_tree.get_by_rank_optimistic(id, m_latch, version, &item);
            } else {
                found = m_tree.get_by_vertex_id_optimistic(id, m_latch, version, &item);
            }

            if(found){
                return item.m_pointer; // it can still be invalid, that is, leaf == nullptr
            } else {
                RAISE(InternalError, "Invalid ID: " << id << " (logical: " << is_logical << ")");
            }
        } catch (Abort){
            // and try again...
        }
    }
}

void DynamicView::update_pointer(uint64_t id, bool is_logical, memstore::IndexEntry pointer_old, memstore::IndexEntry pointer_new) {
    scoped_lock_t xlock(m_latch);

    const ItemUndirected* item { nullptr };
    if(is_logical){
        item = m_tree.get_by_rank(id);
    } else {
        item = m_tree.get_by_vertex_id(id).first;
    }

    if(item == nullptr){ RAISE(InternalError, "Invalid ID: " << id << " (logical: " << is_logical << ")"); }

    if(item->m_pointer == pointer_old){
        item->m_pointer = pointer_new;

        auto leaf_old = pointer_old.leaf();
        auto leaf_new = pointer_new.leaf();

        if(leaf_new != leaf_old){
            leaf_new->incr_ref_count();
            if(leaf_old != nullptr){ // when we insert a new vertex in the view, we don't set the pointer to its leaf/segment
                leaf_old->decr_ref_count();
            }
        }
    }
}

void DynamicView::insert_vertex(uint64_t vertex_id){
    scoped_lock_t xlock(m_latch);

    m_tree.insert(ItemUndirected{vertex_id, 0, memstore::IndexEntry{}});
}

void DynamicView::remove_vertex(uint64_t vertex_id) {
    scoped_lock_t xlock(m_latch);

    bool success = m_tree.remove(vertex_id);
    assert(success == true);
    if(!success) RAISE(InternalError, "The vertex " << vertex_id << " does not exist");
}

void DynamicView::change_degree(uint64_t vertex_id, int64_t diff){
    scoped_lock_t xlock(m_latch);

    auto item = m_tree.get_by_vertex_id(vertex_id).first;
    assert(item != nullptr && "Vertex not found");
    if(!item) RAISE(InternalError, "The vertex " << vertex_id << " does not exist");

    assert(static_cast<int64_t>(item->m_degree) + diff >= 0 && "Underflow");
    item->m_degree += diff;
}

void DynamicView::dump() const{
    m_tree.dump();
}


} // namespace


