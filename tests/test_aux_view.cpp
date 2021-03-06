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

#include "catch.hpp"

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "teseo/aux/builder.hpp"
#include "teseo/aux/counting_tree.hpp"
#include "teseo/aux/item.hpp"
#include "teseo/aux/partial_result.hpp"
#include "teseo/aux/static_view.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/permutation.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::aux;

/**
 * Check that we don't fetch any vertex from an empty memstore
 */
TEST_CASE("aux_builder_empty1", "[aux]") {
    Teseo teseo;
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    auto tx0 = teseo.start_transaction(/* read only */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx0.handle_impl());

    {
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx_impl, partial_result);
        auto p0 = builder.next();
        REQUIRE(p0 == partial_result);
        REQUIRE(p0->empty() == true);
        delete partial_result;
    }

    auto tx1 = teseo.start_transaction();
    tx1.insert_vertex(10);
    tx1.insert_vertex(20);
    tx1.insert_edge(10, 20, 1020);
    tx1.commit();

    { // result should not change for older transactions
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx_impl, partial_result);
        auto p0 = builder.next();
        REQUIRE(p0 == partial_result);
        REQUIRE(p0->empty() == true);
        delete partial_result;
    }

    auto tx2 = teseo.start_transaction();
    tx2.insert_vertex(30);
    tx2.insert_edge(10, 30, 1030);

    { // result should not change for older transactions
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx_impl, partial_result);
        auto p0 = builder.next();
        REQUIRE(p0 == partial_result);
        REQUIRE(p0->empty() == true);
        delete partial_result;
    }
}

/**
 * Create a static view out of an empty memstore
 */
TEST_CASE("aux_builder_empty2", "[aux]") {
    Teseo teseo;
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    auto tx0 = teseo.start_transaction(/* read only */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx0.handle_impl());

    context::ScopedEpoch epoch; // protect from the GC
    Builder builder;
    PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
    memstore->aux_partial_result(tx_impl, partial_result);
    auto dv = builder.create_dv_undirected(0);
    REQUIRE(dv != nullptr);

    auto view = StaticView::create_undirected(0, dv);
    REQUIRE(view->degree_vector() == dv);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(1, false) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * Create a static view out of a single sparse file, only considering the LHS
 */
TEST_CASE("aux_builder_sparse_file1", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    auto tx0 = teseo.start_transaction(/* read only */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx0.handle_impl());

    context::ScopedEpoch epoch; // protect from the GC
    Builder builder;
    PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
    memstore->aux_partial_result(tx_impl, partial_result);
    auto dv = builder.create_dv_undirected(tx0.num_vertices());
    REQUIRE(dv != nullptr);

    auto view = StaticView::create_undirected(tx0.num_vertices(), dv);
    REQUIRE(view->degree_vector() == dv);
    REQUIRE(view->num_vertices() == tx0.num_vertices());

    // vertex IDs
    REQUIRE(view->vertex_id(0) == 11); // 10 + 1 => 11 due to E2I
    REQUIRE(view->vertex_id(1) == 21);
    REQUIRE(view->vertex_id(2) == 31);
    REQUIRE(view->vertex_id(3) == 41);

    // logical IDs
    REQUIRE(view->logical_id(11) == 0);
    REQUIRE(view->logical_id(21) == 1);
    REQUIRE(view->logical_id(31) == 2);
    REQUIRE(view->logical_id(41) == 3);

    // degree vector for vertex IDs
    REQUIRE(view->degree(11, false) == 2);
    REQUIRE(view->degree(21, false) == 1);
    REQUIRE(view->degree(31, false) == 1);
    REQUIRE(view->degree(41, false) == 0);

    // degree vector for logical IDs
    REQUIRE(view->degree(0, true) == 2);
    REQUIRE(view->degree(1, true) == 1);
    REQUIRE(view->degree(2, true) == 1);
    REQUIRE(view->degree(3, true) == 0);

    // invalid vertex IDs
    REQUIRE(view->vertex_id(4) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(10) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(11) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(12) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(numeric_limits<uint64_t>::max()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(12) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(40) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(42) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(numeric_limits<uint64_t>::max()) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(12, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(40, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(42, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(numeric_limits<uint64_t>::max(), false) == aux::NOT_FOUND);
    REQUIRE(view->degree(4, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(11, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(12, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(numeric_limits<uint64_t>::max(), true) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * Create a static view out of multiple (dirty) sparse files, over multiple leaves
 */
TEST_CASE("aux_builder_sparse_file2", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    const uint64_t max_vertex_id = 300;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    context::global_context()->runtime()->rebalance_first_leaf();
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());

    StaticView* view = nullptr;
    {
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx.num_vertices());
        view = StaticView::create_undirected(tx.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx.num_vertices());
        REQUIRE(view->degree_vector() == dv);
    }

    // vertex IDs
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        uint64_t expected_vertex_id = (i +1) * 10 +1; // 10 -> 11 due to E2I
        REQUIRE( view->vertex_id(i) == expected_vertex_id );
    }

    // logical IDs
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = (vertex_id / 10) -1;
        REQUIRE( view->logical_id(vertex_id +1) == expected_logical_id );
    }

    // degree for vertex IDs
    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(view->degree(10 +1, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(view->degree(vertex_id +1, false) == 1);
    }

    // degree for logical IDs
    REQUIRE(view->degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(view->degree(logical_id, true) == 1);
    }

    // invalid IDs
    REQUIRE(view->vertex_id(tx.num_vertices()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->degree(tx.num_vertices(), true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * Create a static view out of multiple (clean) sparse files, over multiple leaves
 */
TEST_CASE("aux_builder_sparse_file3", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    const uint64_t max_vertex_id = 300;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();
    context::global_context()->runtime()->rebalance_first_leaf();

    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());

    StaticView* view = nullptr;
    {
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx.num_vertices());
        view = StaticView::create_undirected(tx.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx.num_vertices());
        REQUIRE(view->degree_vector() == dv);
    }

    // vertex IDs
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        uint64_t expected_vertex_id = (i +1) * 10 +1; // 10 -> 11 due to E2I
        REQUIRE( view->vertex_id(i) == expected_vertex_id );
    }

    // logical IDs
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = (vertex_id / 10) -1;
        REQUIRE( view->logical_id(vertex_id +1) == expected_logical_id );
    }

    // degree for vertex IDs
    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(view->degree(10 +1, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(view->degree(vertex_id +1, false) == 1);
    }

    // degree for logical IDs
    REQUIRE(view->degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(view->degree(logical_id, true) == 1);
    }

    // invalid IDs
    REQUIRE(view->vertex_id(tx.num_vertices()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->degree(tx.num_vertices(), true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * Create a static view out of a dense file, with the transactions in different states:
 * committed / uncommitted / data items inserted or removed
 */
TEST_CASE("aux_builder_dense_file", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();


    {// transform the first segment into a dense file
        context::ScopedEpoch epoch;
        memstore::Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
        memstore::Segment::to_dense_file(context);
    }


    auto tx1 = teseo.start_transaction(/* read only ? */ true);
    auto tx1_impl = reinterpret_cast<transaction::TransactionImpl*>(tx1.handle_impl());

    {
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx1_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx1.num_vertices());
        auto view = StaticView::create_undirected(tx1.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx1.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    // do not commit yet

    { // tx1
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx1_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx1.num_vertices());
        auto view = StaticView::create_undirected(tx1.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx1.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }

    auto tx2 = teseo.start_transaction(/* read only ? */ true);
    auto tx2_impl = reinterpret_cast<transaction::TransactionImpl*>(tx2.handle_impl());

    { // tx2
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx2_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx2.num_vertices());
        auto view = StaticView::create_undirected(tx2.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx2.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }

    tx.commit();

    auto tx3 = teseo.start_transaction(/* read only ? */ true);
    auto tx3_impl = reinterpret_cast<transaction::TransactionImpl*>(tx3.handle_impl());

    { // tx3
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx3_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx3.num_vertices());
        auto view = StaticView::create_undirected(tx3.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx3.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == 0);
        REQUIRE(view->logical_id(21) == 1);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == 11);
        REQUIRE(view->vertex_id(1) == 21);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == 1);
        REQUIRE(view->degree(1, true) == 1);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == 1);
        REQUIRE(view->degree(21, false) == 1);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }

    tx = teseo.start_transaction();
    tx.remove_vertex(10);
    // do not commit yet

    { // tx1
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx1_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx1.num_vertices());
        auto view = StaticView::create_undirected(tx1.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx1.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }
    { // tx2
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx2_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx2.num_vertices());
        auto view = StaticView::create_undirected(tx2.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx2.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }
    { // tx3
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx3_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx3.num_vertices());
        auto view = StaticView::create_undirected(tx3.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx3.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == 0);
        REQUIRE(view->logical_id(21) == 1);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == 11);
        REQUIRE(view->vertex_id(1) == 21);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == 1);
        REQUIRE(view->degree(1, true) == 1);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == 1);
        REQUIRE(view->degree(21, false) == 1);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }


    // expect the same results of tx3 as tx did not commit yet
    auto tx4 = teseo.start_transaction(/* read only ? */ true);
    auto tx4_impl = reinterpret_cast<transaction::TransactionImpl*>(tx4.handle_impl());

    { // tx4
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx4_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx4.num_vertices());
        auto view = StaticView::create_undirected(tx4.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx4.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == 0);
        REQUIRE(view->logical_id(21) == 1);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == 11);
        REQUIRE(view->vertex_id(1) == 21);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == 1);
        REQUIRE(view->degree(1, true) == 1);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == 1);
        REQUIRE(view->degree(21, false) == 1);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }

    tx.commit();

    { // tx1
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx1_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx1.num_vertices());
        auto view = StaticView::create_undirected(tx1.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx1.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }
    { // tx2
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx2_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx2.num_vertices());
        auto view = StaticView::create_undirected(tx2.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx2.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }
    { // tx3
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx3_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx3.num_vertices());
        auto view = StaticView::create_undirected(tx3.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx3.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == 0);
        REQUIRE(view->logical_id(21) == 1);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == 11);
        REQUIRE(view->vertex_id(1) == 21);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == 1);
        REQUIRE(view->degree(1, true) == 1);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == 1);
        REQUIRE(view->degree(21, false) == 1);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }
    { // tx4
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx4_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx4.num_vertices());
        auto view = StaticView::create_undirected(tx4.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx4.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == 0);
        REQUIRE(view->logical_id(21) == 1);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == 11);
        REQUIRE(view->vertex_id(1) == 21);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == 1);
        REQUIRE(view->degree(1, true) == 1);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == 1);
        REQUIRE(view->degree(21, false) == 1);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }

    auto tx5 = teseo.start_transaction(/* read only ? */ true);
    auto tx5_impl = reinterpret_cast<transaction::TransactionImpl*>(tx5.handle_impl());

    { // tx5
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        PartialResult* partial_result = builder.issue(memstore::KEY_MIN, memstore::KEY_MAX);
        memstore->aux_partial_result(tx5_impl, partial_result);
        auto dv = builder.create_dv_undirected(tx5.num_vertices());
        auto view = StaticView::create_undirected(tx5.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx5.num_vertices());
        REQUIRE(view->degree_vector() == dv);
        REQUIRE(view->logical_id(1) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND);
        REQUIRE(view->logical_id(21) == 0);
        REQUIRE(view->logical_id(31) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(0) == 21);
        REQUIRE(view->vertex_id(1) == aux::NOT_FOUND);
        REQUIRE(view->vertex_id(2) == aux::NOT_FOUND);
        REQUIRE(view->degree(0, true) == 0);
        REQUIRE(view->degree(1, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(2, true) == aux::NOT_FOUND);
        REQUIRE(view->degree(1, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND);
        REQUIRE(view->degree(21, false) == 0);
        REQUIRE(view->degree(31, false) == aux::NOT_FOUND);
        view->decr_ref_count(); // delete the view
    }
}

/**
 * Ensure that a degree vector can be created from multiple partial results.
 * The segments are dirty, that is, they contain versions.
 */
TEST_CASE("aux_builder_multiple_intermediates1", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    const uint64_t max_vertex_id = 300;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    context::global_context()->runtime()->rebalance_first_leaf();
    tx.commit();

    { // make the second and fourth segments a dense file
        context::ScopedEpoch epoch;
        memstore::Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        memstore::Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(3);
        memstore::Segment::to_dense_file(context);
    }


    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    //memstore->dump();

    StaticView* view = nullptr;
    {
        using namespace memstore;
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        memstore->aux_partial_result(tx_impl, builder.issue(KEY_MIN, Key{11, 31})); // break at the middle of the LHS of segment #0; expected degree: 1 (10->20)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 31}, Key{11, 31})); // special case, this interval is empty; expected degree: 0
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 31}, Key{11, 35})); // only one edge; expected degree: 1 (10 -> 30)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 35}, Key{11, 41})); // special case, this interval is empty; expected degree: 1
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 41}, Key{11, 71})); // from the middle of LHS to the middle of RHS of segment #0; expected degree: 3 (40, 50, 60)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 71}, Key{11, 121})); // up to the middle of the DF in segment #1; expected degree: 5 (70, 80, 90, 100, 110)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 121}, Key{11, 141})); // internally in the middle of the DF of segment #1; expected degree: 2 (120, 130)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 141}, Key{11, 141})); // special case, this interval is empty; expected degree: 0
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 141}, Key{11, 145})); // only one edge; expected degree: 1 (10 -> 140)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 145}, Key{11, 151})); // special case, this interval is empty; expected degree: 0
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 151}, Key{11, 211})); // up to the start of the RHS of segment #2; expected degree: 6 (150, 160, 170, 180, 190, 200)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 211}, Key{11, 251})); // up to the start of the DF of segment #3; expected degree: 4 (210, 220, 230, 240)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 251}, Key{31, 0})); // up to the next leaf; expected degree for vertex 10: 6 (250, 260, 270, 280, 290, 300)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{31, 0}, KEY_MAX)); // remaining keys
        auto dv = builder.create_dv_undirected(tx.num_vertices());
        view = StaticView::create_undirected(tx.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx.num_vertices());
        REQUIRE(view->degree_vector() == dv);
    }

    // vertex IDs
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        uint64_t expected_vertex_id = (i +1) * 10 +1; // 10 -> 11 due to E2I
        REQUIRE( view->vertex_id(i) == expected_vertex_id );
    }

    // logical IDs
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = (vertex_id / 10) -1;
        REQUIRE( view->logical_id(vertex_id +1) == expected_logical_id );
    }

    // degree for vertex IDs
    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(view->degree(10 +1, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(view->degree(vertex_id +1, false) == 1);
    }

    // degree for logical IDs
    REQUIRE(view->degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(view->degree(logical_id, true) == 1);
    }

    // invalid IDs
    REQUIRE(view->vertex_id(tx.num_vertices()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->degree(tx.num_vertices(), true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * Ensure that a degree vector can be created from multiple partial results.
 * The segments are clean, that is, there are no undo chains around. Otherwise the test
 * is the same as aux_builder_multiple_intermediates1.
 */
TEST_CASE("aux_builder_multiple_intermediates2", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    const uint64_t max_vertex_id = 300;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();
    context::global_context()->runtime()->rebalance_first_leaf();

    { // make the second and fourth segments a dense file
        context::ScopedEpoch epoch;
        memstore::Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        memstore::Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(3);
        memstore::Segment::to_dense_file(context);
    }


    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    //memstore->dump();

    StaticView* view = nullptr;
    {
        using namespace memstore;
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        memstore->aux_partial_result(tx_impl, builder.issue(KEY_MIN, Key{11, 31})); // break at the middle of the LHS of segment #0; expected degree: 1 (10->20)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 31}, Key{11, 31})); // special case, this interval is empty; expected degree: 0
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 31}, Key{11, 35})); // only one edge; expected degree: 1 (10 -> 30)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 35}, Key{11, 41})); // special case, this interval is empty; expected degree: 1
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 41}, Key{11, 71})); // from the middle of LHS to the middle of RHS of segment #0; expected degree: 3 (40, 50, 60)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 71}, Key{11, 121})); // up to the middle of the DF in segment #1; expected degree: 5 (70, 80, 90, 100, 110)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 121}, Key{11, 141})); // internally in the middle of the DF of segment #1; expected degree: 2 (120, 130)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 141}, Key{11, 141})); // special case, this interval is empty; expected degree: 0
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 141}, Key{11, 145})); // only one edge; expected degree: 1 (10 -> 140)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 145}, Key{11, 151})); // special case, this interval is empty; expected degree: 0
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 151}, Key{11, 211})); // up to the start of the RHS of segment #2; expected degree: 6 (150, 160, 170, 180, 190, 200)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 211}, Key{11, 251})); // up to the start of the DF of segment #3; expected degree: 4 (210, 220, 230, 240)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{11, 251}, Key{31, 0})); // up to the next leaf; expected degree for vertex 10: 6 (250, 260, 270, 280, 290, 300)
        memstore->aux_partial_result(tx_impl, builder.issue(Key{31, 0}, KEY_MAX)); // remaining keys
        auto dv = builder.create_dv_undirected(tx.num_vertices());
        view = StaticView::create_undirected(tx.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx.num_vertices());
        REQUIRE(view->degree_vector() == dv);
    }

    // vertex IDs
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        uint64_t expected_vertex_id = (i +1) * 10 +1; // 10 -> 11 due to E2I
        REQUIRE( view->vertex_id(i) == expected_vertex_id );
    }

    // logical IDs
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = (vertex_id / 10) -1;
        REQUIRE( view->logical_id(vertex_id +1) == expected_logical_id );
    }

    // degree for vertex IDs
    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(view->degree(10 +1, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(view->degree(vertex_id +1, false) == 1);
    }

    // degree for logical IDs
    REQUIRE(view->degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(view->degree(logical_id, true) == 1);
    }

    // invalid IDs
    REQUIRE(view->vertex_id(tx.num_vertices()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->degree(tx.num_vertices(), true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * As aux_builder_multiple_intermediates2, but the order in which the partial results are received
 * by the builder is scrambled.
 */
TEST_CASE("aux_builder_multiple_intermediates3", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    const uint64_t max_vertex_id = 300;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();
    context::global_context()->runtime()->rebalance_first_leaf();

    { // make the second and fourth segments a dense file
        context::ScopedEpoch epoch;
        memstore::Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        memstore::Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(3);
        memstore::Segment::to_dense_file(context);
    }


    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    //memstore->dump();

    StaticView* view = nullptr;
    {
        using namespace memstore;
        context::ScopedEpoch epoch; // protect from the GC
        Builder builder;
        auto p0 = builder.issue(KEY_MIN, Key{11, 31}); // break at the middle of the LHS of segment #0; expected degree: 1 (10->20)
        auto p1 = builder.issue(Key{11, 31}, Key{11, 31}); // special case, this interval is empty; expected degree: 0
        auto p2 = builder.issue(Key{11, 31}, Key{11, 35}); // only one edge; expected degree: 1 (10 -> 30)
        auto p3 = builder.issue(Key{11, 35}, Key{11, 41}); // special case, this interval is empty; expected degree: 1
        auto p4 = builder.issue(Key{11, 41}, Key{11, 71}); // from the middle of LHS to the middle of RHS of segment #0; expected degree: 3 (40, 50, 60)
        auto p5 = builder.issue(Key{11, 71}, Key{11, 121}); // up to the middle of the DF in segment #1; expected degree: 5 (70, 80, 90, 100, 110)
        auto p6 = builder.issue(Key{11, 121}, Key{11, 141}); // internally in the middle of the DF of segment #1; expected degree: 2 (120, 130)
        auto p7 = builder.issue(Key{11, 141}, Key{11, 141}); // special case, this interval is empty; expected degree: 0
        auto p8 = builder.issue(Key{11, 141}, Key{11, 145}); // only one edge; expected degree: 1 (10 -> 140)
        auto p9 = builder.issue(Key{11, 145}, Key{11, 151}); // special case, this interval is empty; expected degree: 0
        auto p10 = builder.issue(Key{11, 151}, Key{11, 211}); // up to the start of the RHS of segment #2; expected degree: 6 (150, 160, 170, 180, 190, 200)
        auto p11 = builder.issue(Key{11, 211}, Key{11, 251}); // up to the start of the DF of segment #3; expected degree: 4 (210, 220, 230, 240)
        auto p12 = builder.issue(Key{11, 251}, Key{31, 0}); // up to the next leaf; expected degree for vertex 10: 6 (250, 260, 270, 280, 290, 300)
        auto p13 = builder.issue(Key{31, 0}, KEY_MAX); // remaining keys

        memstore->aux_partial_result(tx_impl, p6);
        memstore->aux_partial_result(tx_impl, p3);
        memstore->aux_partial_result(tx_impl, p12);
        memstore->aux_partial_result(tx_impl, p4);
        memstore->aux_partial_result(tx_impl, p9);
        memstore->aux_partial_result(tx_impl, p2);
        memstore->aux_partial_result(tx_impl, p1);
        memstore->aux_partial_result(tx_impl, p11);
        memstore->aux_partial_result(tx_impl, p7);
        memstore->aux_partial_result(tx_impl, p0);
        memstore->aux_partial_result(tx_impl, p5);
        memstore->aux_partial_result(tx_impl, p13);
        memstore->aux_partial_result(tx_impl, p8);
        memstore->aux_partial_result(tx_impl, p10);

        auto dv = builder.create_dv_undirected(tx.num_vertices());
        view = StaticView::create_undirected(tx.num_vertices(), dv);
        REQUIRE(view->num_vertices() == tx.num_vertices());
        REQUIRE(view->degree_vector() == dv);
    }

    // vertex IDs
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        uint64_t expected_vertex_id = (i +1) * 10 +1; // 10 -> 11 due to E2I
        REQUIRE( view->vertex_id(i) == expected_vertex_id );
    }

    // logical IDs
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = (vertex_id / 10) -1;
        REQUIRE( view->logical_id(vertex_id +1) == expected_logical_id );
    }

    // degree for vertex IDs
    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(view->degree(10 +1, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(view->degree(vertex_id +1, false) == 1);
    }

    // degree for logical IDs
    REQUIRE(view->degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(view->degree(logical_id, true) == 1);
    }

    // invalid IDs
    REQUIRE(view->vertex_id(tx.num_vertices()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->degree(tx.num_vertices(), true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);

    view->decr_ref_count(); // delete the view
}

/**
 * Check we can create the auxiliary view throught the runtime.
 * Let's start with an empty memstore.
 */
TEST_CASE("aux_runtime1", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    auto tx = teseo.start_transaction(/* read only */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    auto view0 = tx_impl->aux_view();
    REQUIRE(view0->num_vertices() == 0);
    REQUIRE(view0->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view0->vertex_id(0) == aux::NOT_FOUND);
    REQUIRE(view0->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view0->degree(0, true) == aux::NOT_FOUND);

    // check it doesn't recompute the view once it has been already computed before
    auto view1 = tx_impl->aux_view();
    REQUIRE(view0 == view1);
}

/**
 * Again, simple usage of the runtime to compute the view. There is only a single
 * populated segment to visit.
 */
TEST_CASE("aux_runtime2", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    tx = teseo.start_transaction(/* read only */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    auto view = tx_impl->aux_view();

    REQUIRE(view->num_vertices() == tx.num_vertices());

    // vertex IDs
    REQUIRE(view->vertex_id(0) == 11); // 10 + 1 => 11 due to E2I
    REQUIRE(view->vertex_id(1) == 21);
    REQUIRE(view->vertex_id(2) == 31);
    REQUIRE(view->vertex_id(3) == 41);

    // logical IDs
    REQUIRE(view->logical_id(11) == 0);
    REQUIRE(view->logical_id(21) == 1);
    REQUIRE(view->logical_id(31) == 2);
    REQUIRE(view->logical_id(41) == 3);

    // degree vector for vertex IDs
    REQUIRE(view->degree(11, false) == 2);
    REQUIRE(view->degree(21, false) == 1);
    REQUIRE(view->degree(31, false) == 1);
    REQUIRE(view->degree(41, false) == 0);

    // degree vector for logical IDs
    REQUIRE(view->degree(0, true) == 2);
    REQUIRE(view->degree(1, true) == 1);
    REQUIRE(view->degree(2, true) == 1);
    REQUIRE(view->degree(3, true) == 0);

    // invalid vertex IDs
    REQUIRE(view->vertex_id(4) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(10) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(11) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(12) == aux::NOT_FOUND);
    REQUIRE(view->vertex_id(numeric_limits<uint64_t>::max()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(12) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(40) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(42) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(numeric_limits<uint64_t>::max()) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(12, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(40, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(42, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(numeric_limits<uint64_t>::max(), false) == aux::NOT_FOUND);
    REQUIRE(view->degree(4, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(11, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(12, true) == aux::NOT_FOUND);
    REQUIRE(view->degree(numeric_limits<uint64_t>::max(), true) == aux::NOT_FOUND);
}

/**
 * Check the runtime is used to create the auxiliary view. The memstore consists
 * of multiple leaves.
 */
TEST_CASE("aux_runtime3", "[aux]") {
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();

    const uint64_t max_vertex_id = 800;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    context::global_context()->runtime()->rebalance_first_leaf();
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
    auto view = tx_impl->aux_view();

    // vertex IDs
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        uint64_t expected_vertex_id = (i +1) * 10 +1; // 10 -> 11 due to E2I
        REQUIRE( view->vertex_id(i) == expected_vertex_id );
    }

    // logical IDs
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = (vertex_id / 10) -1;
        REQUIRE( view->logical_id(vertex_id +1) == expected_logical_id );
    }

    // degree for vertex IDs
    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(view->degree(10 +1, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(view->degree(vertex_id +1, false) == 1);
    }

    // degree for logical IDs
    REQUIRE(view->degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(view->degree(logical_id, true) == 1);
    }

    // invalid IDs
    REQUIRE(view->vertex_id(tx.num_vertices()) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(0) == aux::NOT_FOUND);
    REQUIRE(view->logical_id(10) == aux::NOT_FOUND);
    REQUIRE(view->degree(tx.num_vertices(), true) == aux::NOT_FOUND);
    REQUIRE(view->degree(0, false) == aux::NOT_FOUND);
    REQUIRE(view->degree(10, false) == aux::NOT_FOUND);
}

/**
 * Check that the auxiliary view is initialised by only one thread even
 * in presence of multiple threads.
 */
TEST_CASE("aux_init1", "[aux]"){
    Teseo teseo;
    context::global_context()->disable_aux_cache();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();
    const uint64_t max_vertex_id = 100000;
    const uint64_t num_threads = 40;
    const uint64_t num_repetitions = 40;

    // put some data in the storage
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();

    // thread
    bool ready = false;
    int done = 0;
    mutex mutex_;
    condition_variable condvar;
    transaction::TransactionImpl* tx_impl = nullptr;
    aux::View* view = nullptr;
    auto concurrent_init = [&](){
        teseo.register_thread();

        for(uint64_t r = 0; r < num_repetitions; r++){
            {
                unique_lock<mutex> lock(mutex_);
                condvar.wait(lock, [&ready](){ return ready == true; });
                done++;
            }

            auto local_view = tx_impl->aux_view();

            condvar.notify_all(); // as we changed done
            { // check that all threads have the same view
                unique_lock<mutex> lock(mutex_);
                condvar.wait(lock, [&ready](){ return ready == false; });
                if(view == nullptr){
                    view = local_view;
                } else {
                    REQUIRE(view == local_view);
                }

                done--;
            }

            condvar.notify_all();
        }

        teseo.unregister_thread();
    };

    vector<thread> threads;
    for(uint64_t i = 0; i < num_threads; i++){ threads.emplace_back(concurrent_init); }

    for(uint64_t r = 0; r < num_repetitions; r++){
        auto tx = teseo.start_transaction(/* read only ? */ true);
        tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
        view = nullptr;
        {
            scoped_lock<mutex> lock(mutex_);
            ready = true;
        }
        condvar.notify_all();

        {
            unique_lock<mutex> lock(mutex_);
            condvar.wait(lock, [&done](){ return done == num_threads; } );
            ready = false;
            condvar.notify_all();
            condvar.wait(lock, [&done](){ return done == 0; } );
        }
    }

    for(auto& t: threads) t.join();
}

/**
 * This test case is in response to a bug accidentally found in aux_init1. When the first transaction
 * did not commit, the graph is still empty for the following transactions. Still, sometimes, an aux
 * with existing vertices was computed.
 */
TEST_CASE("aux_init2", "[aux]"){
    Teseo teseo;
    context::global_context()->disable_aux_cache();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();
    const uint64_t max_vertex_id = 10000;
    const uint64_t num_threads = 40;
    const uint64_t num_repetitions = 100;

    // put some data in the storage
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }

    // thread
    bool ready = false;
    int done = 0;
    mutex mutex_;
    condition_variable condvar;
    transaction::TransactionImpl* tx_impl = nullptr;
    aux::View* view = nullptr;
    auto concurrent_init = [&](){
        teseo.register_thread();

        for(uint64_t r = 0; r < num_repetitions; r++){
            {
                unique_lock<mutex> lock(mutex_);
                condvar.wait(lock, [&ready](){ return ready == true; });
                done++;
            }

            auto local_view = tx_impl->aux_view();

            condvar.notify_all(); // as we changed done
            { // check that all threads have the same view
                unique_lock<mutex> lock(mutex_);
                condvar.wait(lock, [&ready](){ return ready == false; });
                if(view == nullptr){
                    view = local_view;
                } else {
                    REQUIRE(view == local_view);
                }

                done--;
            }

            condvar.notify_all();
        }

        teseo.unregister_thread();
    };

    vector<thread> threads;
    for(uint64_t i = 0; i < num_threads; i++){ threads.emplace_back(concurrent_init); }

    for(uint64_t r = 0; r < num_repetitions; r++){
        auto tx = teseo.start_transaction(/* read only ? */ true);
        tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
        view = nullptr;
        {
            scoped_lock<mutex> lock(mutex_);
            ready = true;
        }
        condvar.notify_all();

        {
            unique_lock<mutex> lock(mutex_);
            condvar.wait(lock, [&done](){ return done == num_threads; } );
            ready = false;
            condvar.notify_all();
            condvar.wait(lock, [&done](){ return done == 0; } );
        }

        REQUIRE(view->num_vertices() == 0);
        REQUIRE(view->logical_id(11) == aux::NOT_FOUND );
        REQUIRE(view->vertex_id(0) == aux::NOT_FOUND );
        REQUIRE(view->degree(11, true) == aux::NOT_FOUND );
        REQUIRE(view->degree(11, false) == aux::NOT_FOUND );
        REQUIRE(view->degree(0, true) == aux::NOT_FOUND );
        REQUIRE(view->degree(0, false) == aux::NOT_FOUND );
    }

    for(auto& t: threads) t.join();
}

/**
 * Check that the cached view is being reused among eligible transactions
 */
TEST_CASE("aux_cache", "[aux]"){
    Teseo teseo;
    context::global_context()->enable_aux_cache();

    auto tx0 = teseo.start_transaction(/* read only ? */ true);
    auto tx0_impl = reinterpret_cast<transaction::TransactionImpl*>(tx0.handle_impl());
    auto tx1 = teseo.start_transaction(/* read only ? */ true);
    auto tx1_impl = reinterpret_cast<transaction::TransactionImpl*>(tx1.handle_impl());
    auto tx2 = teseo.start_transaction(/* read only ? */ true);
    auto tx2_impl = reinterpret_cast<transaction::TransactionImpl*>(tx2.handle_impl());

    auto view1 = tx1_impl->aux_view(); // compute the aux view
    auto view2 = tx2_impl->aux_view();
    REQUIRE(view2 == view1); // cached view
    auto view0 = tx0_impl->aux_view();
    REQUIRE(view0 != view1); // it needs to be recomputed because tx0 < tx1

    auto tx3 = teseo.start_transaction(/* read only ? */ true);
    auto tx3_impl = reinterpret_cast<transaction::TransactionImpl*>(tx3.handle_impl());
    auto view3 = tx3_impl->aux_view();
    REQUIRE(view3 == view1); // cached view

    auto tx_rw = teseo.start_transaction(/* read only ? */ false);

    auto tx4 = teseo.start_transaction(/* read only ? */ true);
    auto tx4_impl = reinterpret_cast<transaction::TransactionImpl*>(tx4.handle_impl());
    auto view4 = tx4_impl->aux_view();
    REQUIRE(view4 != view1); // unsafe to use tx1's view. Well, we could have waited for tx_rw to commit first in truth.
}

/**
 * After `context::StaticConfiguration::aux_degree_threshold' times, a query for the degree of a vertex
 * should be answer through the auxiliary view.
 */
TEST_CASE("aux_degree_threshold", "[aux]"){
    Teseo teseo;
    context::global_context()->enable_aux_degree();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());

    if(context::StaticConfiguration::aux_degree_threshold > 0){
        for(uint64_t i = 0; i < context::StaticConfiguration::aux_degree_threshold; i++){
            REQUIRE(tx_impl->has_aux_view() == false);
            REQUIRE(tx.degree(10) == 1);
        }
        REQUIRE(tx_impl->has_aux_view() == false);
    }
    REQUIRE(tx.degree(10) == 1);
    REQUIRE(tx_impl->has_aux_view() == true);
}

/**
 * Query the degree of logical vertices through the interface
 */
TEST_CASE("aux_degree_logical", "[aux]"){
    Teseo teseo;
    context::global_context()->enable_aux_degree();
    const uint64_t max_vertex_id = 10000;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 100000 + vertex_id);
    }
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);

    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;

    REQUIRE(tx.degree(0, true) == expected_degree_10);
    for(uint64_t logical_id = 1; logical_id < tx.num_vertices(); logical_id++){
        REQUIRE(tx.degree(logical_id, true) == 1);
    }
    REQUIRE_THROWS_WITH(tx.degree(tx.num_vertices(), true), Catch::Contains("Invalid logical vertex identifier"));
}

/**
 * Query the degree of the vertices through the interface
 */
TEST_CASE("aux_degree_vertices", "[aux]"){
    Teseo teseo;
    context::global_context()->enable_aux_degree();
    const uint64_t max_vertex_id = 10000;
    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 100000 + vertex_id);
    }
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);

    uint64_t expected_degree_10 = (max_vertex_id / 10) -1;
    REQUIRE(tx.degree(10, false) == expected_degree_10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10 ){
        REQUIRE(tx.degree(vertex_id, false) == 1);
    }

    REQUIRE_THROWS_WITH(tx.degree(max_vertex_id + 10, false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(9, false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(11, false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(0, false), Catch::Contains("does not exist"));
}

/**
 * Query the logical ID of the vertices through the interface
 */
TEST_CASE("aux_logical_id", "[aux]"){
    Teseo teseo;
    context::global_context()->enable_aux_degree();
    const uint64_t max_vertex_id = 1000;
    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);

    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        uint64_t expected_logical_id = vertex_id / 10 - 1;
        REQUIRE(tx.logical_id(vertex_id) == expected_logical_id);
    }

    REQUIRE_THROWS_WITH(tx.logical_id(max_vertex_id + 10), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(9), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(11), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(0), Catch::Contains("does not exist"));
}

/**
 * Query the vertex identifiers from the logical IDs through the interface
 */
TEST_CASE("aux_vertex_id", "[aux]"){
    Teseo teseo;
    context::global_context()->enable_aux_degree();
    const uint64_t max_vertex_id = 1000;
    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ true);

    for(uint64_t i = 0, end = tx.num_vertices(); i < end; i++){
        uint64_t expected_vertex_id = (i + 1) * 10;
        REQUIRE(tx.vertex_id(i) == expected_vertex_id);
    }

    REQUIRE_THROWS_WITH(tx.vertex_id(tx.num_vertices()), Catch::Contains("Invalid logical vertex identifier"));
}

/**
 * Validate a scan with the iterator
 */
TEST_CASE("aux_iterator", "[aux]"){
    Teseo teseo;
    [[maybe_unused]] auto memstore = context::global_context()->memstore();
    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    uint64_t max_vertex_id = 400;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    const uint64_t expected_num_edges = max_vertex_id / 10 -1;

    // manually rebalance
    context::global_context()->runtime()->rebalance_first_leaf();

    tx.commit();

    { // make the first and fourth segment a dense file
        context::ScopedEpoch epoch;
        memstore::Context context { memstore };
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        memstore::Segment::to_dense_file(context);
        context.m_segment = context.m_leaf->get_segment(3);
        memstore::Segment::to_dense_file(context);
    }

    uint64_t num_hits = 0;
    auto check = [&num_hits](uint64_t destination, double weight){
        num_hits++;
        uint64_t expected_logical_id = num_hits;
        uint64_t expected_vertex_id = 10 + 10 * num_hits;
        double expected_weight = 1000 + expected_vertex_id;

        REQUIRE(destination == expected_logical_id);
        REQUIRE(weight == expected_weight);

        return true;
    };

    auto it_ro = teseo.start_transaction(/* read only ? */ true).iterator();
    num_hits = 0;
    it_ro.edges(0, /* logical ? */ true, check);
    REQUIRE(num_hits == expected_num_edges);
}

///**
// * Check that scans with direct pointers are lazily updated.
// * Scans are with the real vertex IDs.
// */
//TEST_CASE("aux_update_pointers1", "[aux]"){
//    Teseo teseo;
//    auto memstore = context::global_context()->memstore();
//    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
//    const uint64_t max_vertex_id = 400;
//    const uint64_t num_vertices = max_vertex_id / 10;
//
//    auto tx = teseo.start_transaction();
//    tx.insert_vertex(10);
//    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
//        tx.insert_vertex(vertex_id);
//        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
//    }
//    tx.commit();
//
//    tx = teseo.start_transaction(/* read only ?  */ true);
//    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
//
//    // create the logical view
//    const uint64_t expected_num_edges = num_vertices  -1;
//    REQUIRE(tx.degree(0, /* logical ? */ true) == expected_num_edges);
//
//    auto view = tx_impl->aux_view();
//    // all pointer should refer to the first leaf and the first segment
//    memstore::IndexEntry entry0;
//    { // restrict the scope
//        context::ScopedEpoch epoch;
//        entry0 = memstore->index()->find(0);
//    }
//    for(uint64_t i = 0; i < num_vertices; i++){
//        auto pointer = view->direct_pointer(i, true);
//        REQUIRE(pointer == entry0);
//    }
//    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
//        auto pointer = view->direct_pointer(vertex_id /* E2I */ +1, false);
//        REQUIRE(entry0 == pointer);
//    }
//
//    // manually rebalance
//    context::global_context()->runtime()->rebalance_first_leaf();
//
//    // perform a scan with all vertices (real IDs)
//    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
//        uint64_t num_hits = 0;
//        auto check = [vertex_id, &num_hits](uint64_t destination, double weight){
//            num_hits ++;
//            if(vertex_id == 10){
//                uint64_t expected_vertex_id = num_hits * 10 + 10;
//                REQUIRE(destination == expected_vertex_id);
//            } else {
//                REQUIRE(destination == 10);
//            }
//            return true;
//        };
//        tx.iterator().edges(vertex_id, /* logical ? */ false, check);
//
//        if(vertex_id == 10){
//            REQUIRE(num_hits == expected_num_edges);
//        } else {
//            REQUIRE(num_hits == 1);
//        }
//    }
//
//    // check that the direct pointers have been updated to the proper positions
//    for(uint64_t i = 0; i < num_vertices; i++){
//        auto pointer = view->direct_pointer(i, /* logical ? */ true);
//
//        context::ScopedEpoch epoch;
//        auto expected = memstore->index()->find( view->vertex_id(i) );
//        REQUIRE(pointer == expected);
//    }
//    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
//        auto pointer = view->direct_pointer(vertex_id /* E2I */ +1, /* logical ? */ false);
//
//        context::ScopedEpoch epoch;
//        auto expected = memstore->index()->find( vertex_id /* E2I */ +1 );
//        REQUIRE(pointer == expected);
//    }
//}
//
///**
// * Check that scans with direct pointers are lazily updated.
// * Scans are performed with the logical IDs.
// */
//TEST_CASE("aux_update_pointers2", "[aux]"){
//    Teseo teseo;
//    auto memstore = context::global_context()->memstore();
//    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
//    const uint64_t max_vertex_id = 400;
//    const uint64_t num_vertices = max_vertex_id / 10;
//
//    auto tx = teseo.start_transaction();
//    tx.insert_vertex(10);
//    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
//        tx.insert_vertex(vertex_id);
//        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
//    }
//    tx.commit();
//
//    tx = teseo.start_transaction(/* read only ?  */ true);
//    auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
//
//    // create the logical view
//    const uint64_t expected_num_edges = num_vertices  -1;
//    REQUIRE(tx.degree(0, /* logical ? */ true) == expected_num_edges);
//
//    auto view = tx_impl->aux_view();
//    // all pointer should refer to the first leaf and the first segment
//    memstore::IndexEntry entry0;
//    { // restrict the scope
//        context::ScopedEpoch epoch;
//        entry0 = memstore->index()->find(0);
//    }
//    for(uint64_t i = 0; i < num_vertices; i++){
//        auto pointer = view->direct_pointer(i, true);
//        REQUIRE(pointer == entry0);
//    }
//    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
//        auto pointer = view->direct_pointer(vertex_id /* E2I */ +1, false);
//        REQUIRE(entry0 == pointer);
//    }
//
//    // manually rebalance
//    context::global_context()->runtime()->rebalance_first_leaf();
//
//    // perform a scan with all vertices (real IDs)
//    for(uint64_t i = 0; i < num_vertices; i++){
//        uint64_t num_hits = 0;
//        auto check = [i, &num_hits](uint64_t destination, double weight){
//            num_hits ++;
//            if(i == 0){
//                REQUIRE(destination == num_hits); // 1, 2, 3, so on.
//            } else {
//                REQUIRE(destination == 0);
//            }
//            return true;
//        };
//        tx.iterator().edges(i, /* logical ? */ true, check);
//
//        if(i == 0){
//            REQUIRE(num_hits == expected_num_edges);
//        } else {
//            REQUIRE(num_hits == 1);
//        }
//    }
//
//    // check that the direct pointers have been updated to the proper positions
//    for(uint64_t i = 0; i < num_vertices; i++){
//        auto pointer = view->direct_pointer(i, /* logical ? */ true);
//
//        context::ScopedEpoch epoch;
//        auto expected = memstore->index()->find( view->vertex_id(i) );
//        REQUIRE(pointer == expected);
//    }
//    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
//        auto pointer = view->direct_pointer(vertex_id /* E2I */ +1, /* logical ? */ false);
//
//        context::ScopedEpoch epoch;
//        auto expected = memstore->index()->find( vertex_id /* E2I */ +1 );
//        REQUIRE(pointer == expected);
//    }
//}

/**
 * Validate we can initialise & destroy an empty counting tree
 */
TEST_CASE("aux_counting_tree1", "[aux]"){
    Teseo teseo;
    CountingTree ct;

    REQUIRE(ct.size() == 0);
    REQUIRE(ct.empty() == true);
    REQUIRE(ct.get_by_vertex_id(10).first == nullptr);
    REQUIRE(ct.get_by_rank(10) == nullptr);
    REQUIRE(ct.get_by_rank(0) == nullptr);
}

/**
 * Simple counting tree with 2 elements
 */
TEST_CASE("aux_counting_tree2", "[aux]"){
    Teseo teseo;
    CountingTree ct;

    ct.insert(ItemUndirected{/* vertex id */ 10, /* degree */ 15});
    ct.insert(ItemUndirected{/* vertex id */ 20, /* degree */ 25});

    REQUIRE(ct.size() == 2);
    REQUIRE(ct.empty() == false);

    // get by vertex
    auto vtx_result = ct.get_by_vertex_id(10);
    REQUIRE(vtx_result.first != nullptr);
    REQUIRE(vtx_result.first->m_vertex_id == 10);
    REQUIRE(vtx_result.first->m_degree == 15);
    REQUIRE(vtx_result.second == 0);
    vtx_result = ct.get_by_vertex_id(20);
    REQUIRE(vtx_result.first != nullptr);
    REQUIRE(vtx_result.first->m_vertex_id == 20);
    REQUIRE(vtx_result.first->m_degree == 25);
    REQUIRE(vtx_result.second == 1);

    // non existing vertices
    REQUIRE(ct.get_by_vertex_id(0).first == nullptr);
    REQUIRE(ct.get_by_vertex_id(5).first == nullptr);
    REQUIRE(ct.get_by_vertex_id(15).first == nullptr);
    REQUIRE(ct.get_by_vertex_id(25).first == nullptr);

    // get by rank
    auto rank_result = ct.get_by_rank(0);
    REQUIRE(rank_result != nullptr);
    REQUIRE(rank_result->m_vertex_id == 10);
    REQUIRE(rank_result->m_degree == 15);
    rank_result = ct.get_by_rank(1);
    REQUIRE(rank_result != nullptr);
    REQUIRE(rank_result->m_vertex_id == 20);
    REQUIRE(rank_result->m_degree == 25);
    REQUIRE(ct.get_by_rank(2) == nullptr);

    // update the degree
    ct.get_by_vertex_id(10).first->m_degree ++;
    ct.get_by_rank(1)->m_degree ++;
    REQUIRE(ct.get_by_rank(0)->m_degree == 16);
    REQUIRE(ct.get_by_rank(1)->m_degree == 26);
    REQUIRE(ct.get_by_vertex_id(10).first->m_degree == 16);
    REQUIRE(ct.get_by_vertex_id(20).first->m_degree == 26);

    // remove non existing vertices
    REQUIRE(ct.remove(5) == false);
    REQUIRE(ct.remove(15) == false);
    REQUIRE(ct.remove(25) == false);

    // remove the first vertex
    bool success = ct.remove(10);
    REQUIRE(success == true);
    REQUIRE(ct.get_by_vertex_id(10).first == nullptr);
    REQUIRE(ct.get_by_vertex_id(20).first != nullptr);
    REQUIRE(ct.get_by_vertex_id(20).first->m_vertex_id == 20);
    REQUIRE(ct.get_by_rank(0) != nullptr);
    REQUIRE(ct.get_by_rank(0)->m_vertex_id == 20);
    REQUIRE(ct.get_by_rank(1) == nullptr);
    REQUIRE(!ct.empty());
    REQUIRE(ct.size() == 1);

    // remove the second vertex
    success = ct.remove(20);
    REQUIRE(success == true);
    REQUIRE(ct.get_by_vertex_id(10).first == nullptr);
    REQUIRE(ct.get_by_vertex_id(20).first == nullptr);
    REQUIRE(ct.get_by_rank(0) == nullptr);
    REQUIRE(ct.size() == 0);
    REQUIRE(ct.empty());
}

/**
 * Counting tree with many elements. The elements are inserted sequentially (as a builder would do), but
 * eventually removed in random order.
 */
TEST_CASE("aux_counting_tree3", "[aux]"){
    Teseo teseo; // we need a thread context for the GC
    CountingTree ct;
    const uint64_t max_vertex_id = 10000;
    const uint64_t num_elts = max_vertex_id / 10;

    // insert the elements
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        ct.insert(ItemUndirected{/* vertex */ vertex_id, /* degree */vertex_id + 5});
    }

    // check all elements exist
    REQUIRE(!ct.empty());
    REQUIRE(ct.size() == num_elts);

    // retrieve the elements by vertex_id
    for(uint64_t vertex_id = 5; vertex_id <= max_vertex_id + 5; vertex_id += 5){
        if(vertex_id % 10 != 0){ // multiple of 5, the element does not exist
            auto result = ct.get_by_vertex_id(vertex_id);
            REQUIRE(result.first == nullptr);
        } else { // the element exist
            auto result = ct.get_by_vertex_id(vertex_id);
            REQUIRE(result.first != nullptr);
            REQUIRE(result.first->m_vertex_id == vertex_id);
            REQUIRE(result.first->m_degree == vertex_id + 5);
            uint64_t expected_rank = vertex_id / 10 -1;
            REQUIRE(result.second == expected_rank);
        }
    }

    // retrieve the elements by rank
    REQUIRE(ct.get_by_rank(num_elts) == nullptr);
    for(uint64_t rank = 0; rank < num_elts; rank++){
        auto result = ct.get_by_rank(rank);
        REQUIRE(result != nullptr);
        uint64_t expected_vertex_id = (rank +1) * 10;
        REQUIRE(result->m_vertex_id == expected_vertex_id);
        REQUIRE(result->m_degree == expected_vertex_id + 5);
    }

    // remove the elements
    auto permutation = util::random_permutation(num_elts, /* seed */ 42);
    unordered_set<uint64_t> removed_elts; // keep track which elements we already removed
    for(uint64_t i = 0; i < num_elts; i++){
        { // restrict the scope
            uint64_t rank = permutation[i];
            uint64_t vertex_id = (rank + 1) * 10;
            bool success = ct.remove(vertex_id);
            REQUIRE(success == true);
            removed_elts.insert(vertex_id);
        }

        uint64_t expected_num_elts = num_elts -1 -i;
        REQUIRE(ct.size() == expected_num_elts);

        uint64_t expected_rank = 0;
        for(uint64_t candidate = 10; candidate <= max_vertex_id; candidate += 10){
            if(removed_elts.count(candidate) > 0){ // this vertex has already been removed
                auto result = ct.get_by_vertex_id(candidate);
                REQUIRE(result.first == nullptr);
            } else {
                auto vtx_result = ct.get_by_vertex_id(candidate);
                REQUIRE(vtx_result.first != nullptr);
                REQUIRE(vtx_result.first->m_vertex_id == candidate);
                REQUIRE(vtx_result.first->m_degree == candidate + 5);
                REQUIRE(vtx_result.second == expected_rank);

                auto rank_result = ct.get_by_rank(expected_rank);
                REQUIRE(rank_result != nullptr);
                REQUIRE(rank_result == vtx_result.first);

                expected_rank ++;
            }
        }

        REQUIRE(ct.get_by_rank(expected_num_elts) == nullptr);
    }

    REQUIRE(ct.size() == 0);
    REQUIRE(ct.empty() == true);
}

/**
 * Insert the elements in random order. Eventually remove implicitly them with the destructor. If running with ASAN or
 * valgrind, check that all nodes (inodes & leaves) created are released by the destructor.
 */
TEST_CASE("aux_counting_tree4", "[aux]"){
    Teseo teseo; // we need a thread context for the GC

    CountingTree ct;
    const uint64_t max_vertex_id = (1ull << 16) * 10;
    const uint64_t num_elts = max_vertex_id / 10;

    // insert the elements
    auto permutation = util::random_permutation(num_elts, /* seed */ 42);
    for(uint64_t i = 0; i < num_elts; i++){
        uint64_t vertex_id = (permutation[i] + 1) * 10; // still 10, 20, 30, ... max_vertex_id; but in random order
        ct.insert(ItemUndirected{/* vertex */ vertex_id, /* degree */vertex_id + 5});
    }

    // check that all elements inserted can be retrieved
    REQUIRE(ct.size() == num_elts);

    // retrieve the elements by vertex_id
    for(uint64_t vertex_id = 5; vertex_id <= max_vertex_id + 5; vertex_id += 5){
        if(vertex_id % 10 != 0){ // multiple of 5, the element does not exist
            auto result = ct.get_by_vertex_id(vertex_id);
            REQUIRE(result.first == nullptr);
        } else { // the element exist
            auto result = ct.get_by_vertex_id(vertex_id);
            REQUIRE(result.first != nullptr);
            REQUIRE(result.first->m_vertex_id == vertex_id);
            REQUIRE(result.first->m_degree == vertex_id + 5);
            uint64_t expected_rank = vertex_id / 10 -1;
            REQUIRE(result.second == expected_rank);
        }
    }

    // retrieve the elements by rank
    REQUIRE(ct.get_by_rank(num_elts) == nullptr);
    for(uint64_t rank = 0; rank < num_elts; rank++){
        auto result = ct.get_by_rank(rank);
        REQUIRE(result != nullptr);
        uint64_t expected_vertex_id = (rank +1) * 10;
        REQUIRE(result->m_vertex_id == expected_vertex_id);
        REQUIRE(result->m_degree == expected_vertex_id + 5);
    }

    // check that ct doesn't cause any memory leaks when destroyed ...
}

/**
 * Start with an empty dynamic view. Perform a few alterations.
 */
TEST_CASE("aux_dynamic_view1", "[aux]"){
    Teseo teseo;
    auto tx = teseo.start_transaction(/* read only ? */ false);

    REQUIRE_THROWS_WITH(tx.degree(0, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(1, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(9, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(10, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(11, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(9), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(10), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(11), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.vertex_id(0), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.vertex_id(1), Catch::Contains("Invalid logical vertex"));

    tx.insert_vertex(10);

    REQUIRE(tx.degree(0, /* logical */ true) == 0);
    REQUIRE_THROWS_WITH(tx.degree(1, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(9, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE(tx.degree(10, /* logical */ false) == 0);
    REQUIRE_THROWS_WITH(tx.degree(11, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(0), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(9), Catch::Contains("does not exist"));
    REQUIRE(tx.logical_id(10) == 0);
    REQUIRE_THROWS_WITH(tx.logical_id(11), Catch::Contains("does not exist"));
    REQUIRE(tx.vertex_id(0) == 10);
    REQUIRE_THROWS_WITH(tx.vertex_id(1), Catch::Contains("Invalid logical vertex"));

    tx.insert_vertex(20);
    tx.insert_vertex(30);

    REQUIRE(tx.degree(0, /* logical */ true) == 0);
    REQUIRE(tx.degree(1, /* logical */ true) == 0);
    REQUIRE(tx.degree(2, /* logical */ true) == 0);
    REQUIRE_THROWS_WITH(tx.degree(3, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE(tx.vertex_id(0) == 10);
    REQUIRE(tx.vertex_id(1) == 20);
    REQUIRE(tx.vertex_id(2) == 30);
    REQUIRE_THROWS_WITH(tx.vertex_id(4), Catch::Contains("Invalid logical vertex"));
    REQUIRE(tx.degree(10, /* logical */ false) == 0);
    REQUIRE(tx.degree(20, /* logical */ false) == 0);
    REQUIRE(tx.degree(30, /* logical */ false) == 0);
    REQUIRE(tx.logical_id(10) == 0);
    REQUIRE(tx.logical_id(20) == 1);
    REQUIRE(tx.logical_id(30) == 2);

    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);

    REQUIRE(tx.degree(0, /* logical */ true) == 2);
    REQUIRE(tx.degree(1, /* logical */ true) == 1);
    REQUIRE(tx.degree(2, /* logical */ true) == 1);
    REQUIRE(tx.degree(10, /* logical */ false) == 2);
    REQUIRE(tx.degree(20, /* logical */ false) == 1);
    REQUIRE(tx.degree(30, /* logical */ false) == 1);

    tx.remove_edge(10, 20);
    REQUIRE(tx.degree(0, /* logical */ true) == 1);
    REQUIRE(tx.degree(1, /* logical */ true) == 0);
    REQUIRE(tx.degree(2, /* logical */ true) == 1);
    REQUIRE(tx.degree(10, /* logical */ false) == 1);
    REQUIRE(tx.degree(20, /* logical */ false) == 0);
    REQUIRE(tx.degree(30, /* logical */ false) == 1);

    tx.remove_vertex(10);

    REQUIRE(tx.degree(0, /* logical */ true) == 0);
    REQUIRE(tx.degree(1, /* logical */ true) == 0);
    REQUIRE_THROWS_WITH(tx.degree(2, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(10, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE(tx.degree(20, /* logical */ false) == 0);
    REQUIRE(tx.degree(30, /* logical */ false) == 0);

    tx.remove_vertex(20);
    tx.remove_vertex(30);

    REQUIRE_THROWS_WITH(tx.degree(0, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(1, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(2, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(3, /* logical */ true), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.degree(9, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(10, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(11, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(20, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.degree(30, /* logical */ false), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(9), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(10), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(11), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(20), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.logical_id(30), Catch::Contains("does not exist"));
    REQUIRE_THROWS_WITH(tx.vertex_id(0), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.vertex_id(1), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.vertex_id(2), Catch::Contains("Invalid logical vertex"));
    REQUIRE_THROWS_WITH(tx.vertex_id(3), Catch::Contains("Invalid logical vertex"));
}

/**
 * Check we can use the dynamic view with an iterator
 */
TEST_CASE("aux_dynamic_view2", "[aux]"){
    Teseo teseo;
    const uint64_t max_vertex_id = 100;
    const uint64_t num_vertices = max_vertex_id / 10;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ false);
    auto it = tx.iterator();
    uint64_t num_hits = 0;
    auto check = [&tx, &num_hits](uint64_t destination, double weight){
        num_hits++;
        REQUIRE(destination == num_hits);
        uint64_t expected_vertex_id = ( num_hits +1 ) * 10;
        REQUIRE(tx.vertex_id(destination) == expected_vertex_id);
        double expected_weight = 1000 + expected_vertex_id;
        REQUIRE(weight == expected_weight);
        return true;
    };
    it.edges(0, /* logical ? */ true, check);
    REQUIRE(num_hits == num_vertices -1);
}


/**
 * Alter the snapshot inside the iterator, by removing the vertex just retrieved.
 */
TEST_CASE("aux_dynamic_view3", "[aux]"){
    Teseo teseo;
    const uint64_t max_vertex_id = 100;
    const uint64_t num_vertices = max_vertex_id / 10;

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    for(uint64_t vertex_id = 20; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
        tx.insert_edge(10, vertex_id, 1000 + vertex_id);
    }
    tx.commit();

    tx = teseo.start_transaction(/* read only ? */ false);
    auto it = tx.iterator();
    uint64_t num_hits = 0;
    auto check = [&tx, &num_hits](uint64_t destination, double weight){
        num_hits++;
        REQUIRE(destination == /* logical vertex id */ 1);
        uint64_t expected_vertex_id = ( num_hits +1 ) * 10;
        REQUIRE(tx.vertex_id(destination) == expected_vertex_id);
        double expected_weight = 1000 + expected_vertex_id;
        REQUIRE(weight == expected_weight);

        tx.remove_vertex(expected_vertex_id);

        return true;
    };
    it.edges(0, /* logical ? */ true, check);
    REQUIRE(num_hits == num_vertices -1);
}

///**
// * Check that the created view properly manage the reference count to the leaves
// */
//TEST_CASE("aux_leaf_reference_counting", "[aux]"){
//    Teseo teseo;
//    context::global_context()->disable_aux_cache();
//
//    auto tx = teseo.start_transaction();
//    tx.insert_vertex(10);
//    tx.insert_vertex(20);
//    tx.insert_edge(10, 20, 1020);
//    tx.commit();
//
//    // Retrieve the first leaf
//    [[maybe_unused]] auto memstore = context::global_context()->memstore();
//    memstore::Leaf* leaf { nullptr };
//    { // restrict the scope
//        context::ScopedEpoch epoch;
//        leaf = memstore->index()->find(0).leaf();
//    }
//    REQUIRE(leaf->ref_count() == 1);
//
//    tx = teseo.start_transaction(/* read only */ true);
//    tx.vertex_id(0); // ignore the result, just to create the aux static view as side effect
//    REQUIRE(leaf->ref_count() == (1 + 2 * context::StaticConfiguration::numa_num_nodes)); // 1 + 2 logical vertices
//    tx.commit();
//
//    tx = teseo.start_transaction(/* read only */ false); // RW transaction
//    REQUIRE(leaf->ref_count() == 1); // the old view is not reachable anymore, the reference counting should have been reset to 1
//
//    tx.vertex_id(0); // ignore the result, just to create the aux static view as side effect
//    REQUIRE(leaf->ref_count() == 3); // again 1 + 2 logical vertices
//    tx.commit();
//
//    // Make tx out of scope
//    tx = teseo.start_transaction(/* whatever */ false);
//    REQUIRE(leaf->ref_count() == 1); // check the ref. count. is back to 1
//
//    // done ...
//}

