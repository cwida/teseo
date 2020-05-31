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

#include "teseo/aux/builder.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/numa.hpp"
#include "teseo/util/thread.hpp"

using namespace std;
using namespace teseo::util;

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
    ReadLatch slock(m_latch);

    auto item = m_tree.get_by_rank(logical_id);
    if(item == nullptr){
        return NOT_FOUND;
    } else {
        return item->m_vertex_id;
    }
}

uint64_t DynamicView::logical_id(uint64_t vertex_id) const noexcept {
    ReadLatch slock(m_latch);

    auto item = m_tree.get_by_vertex_id(vertex_id);
    if(item.first == nullptr){
        return NOT_FOUND;
    } else {
        return item.second;
    }
}

uint64_t DynamicView::degree(uint64_t id, bool is_logical) const noexcept {
    ReadLatch slock(m_latch);

    const ItemUndirected* item { nullptr };
    if(is_logical){
        item = m_tree.get_by_rank(id);
    } else {
        item = m_tree.get_by_vertex_id(id).first;
    }

    if(item == nullptr){
        return NOT_FOUND;
    } else {
        return item->m_degree;
    }
}

uint64_t DynamicView::num_vertices() const noexcept {
    ReadLatch slock(m_latch);

    return m_tree.size();
}

memstore::IndexEntry DynamicView::direct_pointer(uint64_t id, bool is_logical) const {
    ReadLatch slock(m_latch);

    const ItemUndirected* item { nullptr };
    if(is_logical){
        item = m_tree.get_by_rank(id);
    } else {
        item = m_tree.get_by_vertex_id(id).first;
    }

    if(item == nullptr){
        RAISE(InternalError, "Invalid ID: " << id << " (logical: " << is_logical << ")");
    } else {
        return item->m_pointer; // it can still be invalid, that is leaf == nullptr
    }
}

void DynamicView::update_pointer(uint64_t id, bool is_logical, memstore::IndexEntry pointer_old, memstore::IndexEntry pointer_new) {
    ReadLatch slock(m_latch);

    const ItemUndirected* item { nullptr };
    if(is_logical){
        item = m_tree.get_by_rank(id);
    } else {
        item = m_tree.get_by_vertex_id(id).first;
    }

    if(item == nullptr){ RAISE(InternalError, "Invalid ID: " << id << " (logical: " << is_logical << ")"); }

    uint64_t* pointer = reinterpret_cast<uint64_t*>(&(item->m_pointer));
    uint64_t* expected = reinterpret_cast<uint64_t*>(&pointer_old);
    uint64_t* desired = reinterpret_cast<uint64_t*>(&pointer_new);

    bool success = __atomic_compare_exchange(pointer, expected, desired, /* the rest is blah blah for non x86 archs */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    if(success){ // ref count
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
    WriteLatch xlock(m_latch);

    m_tree.insert(ItemUndirected{vertex_id, 0, memstore::IndexEntry{}});
}

void DynamicView::remove_vertex(uint64_t vertex_id) {
    WriteLatch xlock(m_latch);

    bool success = m_tree.remove(vertex_id);
    assert(success == true);
    if(!success) RAISE(InternalError, "The vertex " << vertex_id << " does not exist");
}

void DynamicView::change_degree(uint64_t vertex_id, int64_t diff){
    WriteLatch xlock(m_latch);

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


