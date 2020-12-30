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
#include <cstdlib> // calloc, malloc, free
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/direct_pointer.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/memstore/vertex_table.hpp"
#include "teseo/rebalance/merger_service.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::memstore;

constexpr static uint64_t NUM_NUMA_NODES = context::StaticConfiguration::numa_num_nodes;

/**
 * Conversion of a direct pointer into its compressed representation.
 * Set the filepos.
 */
TEST_CASE("vt_cdptr1", "[vt][vertex_table]"){
    Leaf* leaf = internal::allocate_leaf(512);
    uint64_t segment_id = 511;
    uint64_t segment_version = 127482023;
    uint64_t pos_vertex = 723;
    uint64_t pos_backptr = 121;

    DirectPointer dptr0;
    dptr0.set_leaf(leaf);
    dptr0.set_segment(segment_id, segment_version);
    dptr0.set_filepos(pos_vertex, 0, pos_backptr);

    // Compress
    CompressedDirectPointer cdptr = dptr0.compress();

    // Decompress
    DirectPointer dptr1 { cdptr };

    // Check they are equals
    REQUIRE(dptr0.leaf() == dptr1.leaf());
    REQUIRE(dptr0.get_segment_id() == dptr1.get_segment_id());
    REQUIRE(dptr0.get_segment_version() == dptr1.get_segment_version());
    REQUIRE(dptr0.has_filepos());
    uint64_t pos_vertex0, pos_edge0, pos_backptr0;
    dptr0.get_filepos(&pos_vertex0, &pos_edge0, &pos_backptr0);
    REQUIRE(pos_vertex0 == pos_vertex);
    REQUIRE(pos_edge0 == 0);
    REQUIRE(pos_backptr0 == pos_backptr);
    REQUIRE(dptr1.has_filepos());
    uint64_t pos_vertex1, pos_edge1, pos_backptr1;
    dptr1.get_filepos(&pos_vertex1, &pos_edge1, &pos_backptr1);
    REQUIRE(pos_vertex0 == pos_vertex1);
    REQUIRE(pos_edge0 == pos_edge1);
    REQUIRE(pos_backptr0 == pos_backptr1);

    internal::deallocate_leaf(leaf); leaf = nullptr;
}

/**
 * Conversion of a direct pointer into its compressed representation.
 * Do not set the filepos.
 */
TEST_CASE("vt_cdptr2", "[vt][vertex_table]"){
    Leaf* leaf = internal::allocate_leaf(512);
    uint64_t segment_id = 511;
    uint64_t segment_version = 127482023;

    DirectPointer dptr0;
    dptr0.set_leaf(leaf);
    dptr0.set_segment(segment_id, segment_version);
    DirectPointer dptr1;
    dptr1.set_leaf((Leaf*) 0x1); // bogus
    dptr1.set_segment(76, 24781); // bogus
    dptr1.set_filepos(1204, 1206, 12); // bogus

    // Compress
    CompressedDirectPointer cdptr = dptr0.compress();

    // Decompress
    dptr1 = cdptr;

    // Check they are equals
    REQUIRE(dptr0.leaf() == dptr1.leaf());
    REQUIRE(dptr0.get_segment_id() == dptr1.get_segment_id());
    REQUIRE(dptr0.get_segment_version() == dptr1.get_segment_version());
    REQUIRE(!dptr0.has_filepos());
    REQUIRE(!dptr1.has_filepos());

    internal::deallocate_leaf(leaf); leaf = nullptr;
}

/**
 * Base usage of the vertex table. Create an item, update and remove it.
 */
TEST_CASE("vt_sanity", "[vt][vertex_table]") {
    // Random pointers. We need a memory location so that direct pointer can actually alter the reference
    // counters for the associated leaves.
    constexpr uint64_t num_leaves = 32;
    Leaf* leaves[num_leaves];
    for(uint64_t i = 0; i < num_leaves; i++){ leaves[i] = internal::allocate_leaf(); }

    Teseo teseo; // we need a context to operate
    context::ScopedEpoch epoch;

    VertexTable vt;

    // Empty table
    DirectPointer dp = vt.get(10, /* numa node */ 0);
    REQUIRE(dp.leaf() == nullptr);
    REQUIRE(dp.segment() == nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Update vertex 10 => it fails because the vertex needs to be inserted first
    DirectPointer t0;
    t0.set_leaf(leaves[0]);
    auto result = vt.update(10, t0);
    REQUIRE(result == false); // because the vertex 10 does not exist
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == nullptr);
    REQUIRE(dp.segment() == nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Insert vertex 10
    vt.upsert(10, t0);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == (leaves[0]));
    REQUIRE(dp.segment() != nullptr); // segment #0 of leaf #0
    REQUIRE(dp.has_filepos() == false);

    // Change the leaf in vertex 10 with #upsert
    t0.set_leaf(leaves[1]);
    result = vt.update(10, t0);
    REQUIRE(result == true);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == (leaves[1]));
    REQUIRE(dp.segment() != nullptr); // segment #0 of leaf #1
    REQUIRE(dp.has_filepos() == false);

    // Change the leaf in vertex 10 with #update
    t0.set_leaf(leaves[2]);
    result = vt.update(10, t0);
    REQUIRE(result == true); // success
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == (leaves[2]));
    REQUIRE(dp.segment() != nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Remove a non existing vertex
    vt.remove(20);
    dp = vt.get(20,  /* numa node */ 0);
    REQUIRE(dp.leaf() == nullptr);
    REQUIRE(dp.segment() == nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Remove vertex 10
    vt.remove(10);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == nullptr);
    REQUIRE(dp.segment() == nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Updates should fail now
    t0.set_leaf(leaves[4]);
    result = vt.update(10, t0);
    REQUIRE(result == false);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == nullptr);
    REQUIRE(dp.segment() == nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Reinsert vertex 10
    t0.set_leaf(leaves[5]);
    vt.upsert(10, t0);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == leaves[5]);
    REQUIRE(dp.segment() != nullptr);
    REQUIRE(dp.has_filepos() == false);

    // Update again vertex 10
    t0.set_leaf(leaves[6]);
    result = vt.update(10, t0);
    REQUIRE(result == true);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == leaves[6]);
    REQUIRE(dp.segment() != nullptr);
    REQUIRE(dp.has_filepos() == false);

    // And remove it again...
    vt.remove(10);
    dp = vt.get(10,  /* numa node */ 0);
    REQUIRE(dp.leaf() == nullptr);
    REQUIRE(dp.segment() == nullptr);
    REQUIRE(dp.has_filepos() == false);

    // We're done
    for(uint64_t i = 0; i < num_leaves; i++){ internal::deallocate_leaf(leaves[i]); }
}

/**
 * Check that the hash table is expanded when it becomes overfilled, around 60% of the capacity
 */
TEST_CASE("vt_expand", "[vt][vertex_table]") {
    Leaf* leaf = internal::allocate_leaf();

    Teseo teseo; // we need a context to operate
    context::ScopedEpoch epoch;
    constexpr uint64_t max_vertex_id = 40; // it expands with vertex 40

    VertexTable vt;

    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        DirectPointer dp;
        dp.set_leaf(leaf);
        vt.upsert(vertex_id, dp);
    }

    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        DirectPointer dp = vt.get(vertex_id, /* numa node */ 0);
        REQUIRE(dp.leaf() == leaf);
        REQUIRE(dp.has_filepos() == false);
    }

    // Remove the vertices & check again
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        vt.remove(vertex_id);
    }
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        REQUIRE( vt.get(vertex_id, /* numa node */ 0).leaf() == nullptr );
    }

    // we're done
    internal::deallocate_leaf(leaf);
}

/**
 * The key 1 is a special case as it conflicts with the value reserved for the tombstone. It is always stored at the slot -1.
 */
TEST_CASE("vt_special_case", "[vt][vertex_table]"){
    Leaf* leaf0 = internal::allocate_leaf();
    Leaf* leaf1 = internal::allocate_leaf();

    Teseo teseo; // we need a context to operate
    context::ScopedEpoch epoch;
    VertexTable vt;

    // insert the key with value `1`
    DirectPointer t0;
    t0.set_leaf(leaf0);
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == nullptr );
    REQUIRE( vt.update(1, t0) == false);
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == nullptr );
    vt.upsert(1, t0);
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == leaf0 );

    // check that it is preserved during an expansion
    vt.upsert(10, t0);
    vt.upsert(20, t0);
    vt.upsert(30, t0);
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == leaf0 );

    // check that update works
    t0.set_leaf(leaf1);
    REQUIRE( vt.update(1, t0) == true );
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == leaf1 );
    // check remove works
    vt.remove(1);
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == nullptr );
    REQUIRE( vt.update(1, t0) == false );
    REQUIRE( vt.get(1, /* numa node */ 0).leaf() == nullptr );

    // clean up
    vt.remove(10);
    vt.remove(20);
    vt.remove(30);

    // we're done
    internal::deallocate_leaf(leaf0);
    internal::deallocate_leaf(leaf1);
}

/**
 * Check the vertex table is properly updated and maintained during rebalances and prunes.
 */
TEST_CASE("vt_rebalances", "[vt][vertex_table]"){
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    auto memstore = context::global_context()->memstore();
    memstore->merger()->stop();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.commit();

    context::global_context()->runtime()->rebalance_first_leaf();

    auto vt = memstore->vertex_table();
    {
        context::ScopedEpoch epoch;
        // Only the merger service can register the vertices in the index
        REQUIRE(vt->get(9, /* numa node */ 0).leaf() == nullptr);
        REQUIRE(vt->get(10, /* numa node */ 0).leaf() == nullptr);
        REQUIRE(vt->get(11, /* numa node */ 0).leaf() == nullptr);
        REQUIRE(vt->get(21, /* numa node */ 0).leaf() == nullptr);
        REQUIRE(vt->get(31, /* numa node */ 0).leaf() == nullptr);
        REQUIRE(vt->get(41, /* numa node */ 0).leaf() == nullptr);
    }

    Leaf* leaf = nullptr;
    { // Perform a pass of the merger service
        context::ScopedEpoch epoch;
        Context context { memstore };
        leaf = context.m_leaf = memstore->index()->find(0).leaf();
        Segment* segment0 = leaf->get_segment(0);
        Segment* segment1 = leaf->get_segment(1);

        REQUIRE(segment0->need_rebuild_vertex_table() == true); // because the vertices 10 and 20 are unindexed
        context.m_segment = segment0;
        Segment::prune(context, /* vertex table ? */ true);
        REQUIRE(segment0->need_rebuild_vertex_table() == false);

        REQUIRE(segment1->need_rebuild_vertex_table() == true); // because the vertices 30 and 40 are unindexed
        context.m_segment = segment1;
        Segment::prune(context, /* vertex table ? */ true);
        REQUIRE(segment1->need_rebuild_vertex_table() == false);

        // Validate the pointers
        for(uint64_t node = 0; node < NUM_NUMA_NODES; node++){
            // Vertex 10
            REQUIRE(vt->get(11, node).leaf() == leaf);
            REQUIRE(vt->get(11, node).segment() == segment0);
            REQUIRE(vt->get(11, node).get_segment_id() == 0);
            REQUIRE(vt->get(11, node).get_segment_version() == segment0->get_version());
            REQUIRE(vt->get(11, node).has_filepos() == true);
            uint64_t vertex, edge, backptr;
            REQUIRE_NOTHROW( vt->get(11, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 0);
            REQUIRE(backptr == 0);
            // Vertex 20
            REQUIRE(vt->get(21, node).leaf() == leaf);
            REQUIRE(vt->get(21, node).segment() == segment0);
            REQUIRE(vt->get(21, node).get_segment_id() == 0);
            REQUIRE(vt->get(21, node).get_segment_version() == segment0->get_version());
            REQUIRE(vt->get(21, node).has_filepos() == true);
            REQUIRE_NOTHROW( vt->get(21, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 2);
            REQUIRE(backptr == 1);
            // Vertex 30
            REQUIRE(vt->get(31, node).leaf() == leaf);
            REQUIRE(vt->get(31, node).segment() == segment1);
            REQUIRE(vt->get(31, node).get_segment_id() == 1);
            REQUIRE(vt->get(31, node).get_segment_version() == segment1->get_version());
            REQUIRE(vt->get(31, node).has_filepos() == true);
            REQUIRE_NOTHROW( vt->get(31, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 0);
            REQUIRE(backptr == 0);
            // Vertex 40
            REQUIRE(vt->get(41, node).leaf() == leaf);
            REQUIRE(vt->get(41, node).segment() == segment1);
            REQUIRE(vt->get(41, node).get_segment_id() == 1);
            REQUIRE(vt->get(41, node).get_segment_version() == segment1->get_version());
            REQUIRE(vt->get(41, node).has_filepos() == true);
            REQUIRE_NOTHROW( vt->get(41, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 2);
            REQUIRE(backptr == 1);
        }
    }

    tx = teseo.start_transaction();
    tx.insert_vertex(5);
    REQUIRE(leaf->get_segment(0)->need_rebuild_vertex_table() == false);
    context::global_context()->runtime()->rebalance_first_leaf();
    REQUIRE(leaf->get_segment(0)->need_rebuild_vertex_table() == true); // vertex 5
    REQUIRE(leaf->get_segment(1)->need_rebuild_vertex_table() == false);

    { // Validate the pointers again
        context::ScopedEpoch epoch;

        Segment* segment0 = leaf->get_segment(0);
        Segment* segment1 = leaf->get_segment(1);

        for(uint64_t node = 0; node < NUM_NUMA_NODES; node++){
            // Vertex 5
            REQUIRE(vt->get(6, node).leaf() == nullptr);

            // Vertex 10
            REQUIRE(vt->get(11, node).leaf() == leaf);
            REQUIRE(vt->get(11, node).segment() == segment0);
            REQUIRE(vt->get(11, node).get_segment_id() == 0);
            REQUIRE(vt->get(11, node).get_segment_version() == segment0->get_version());
            REQUIRE(vt->get(11, node).has_filepos() == true);
            uint64_t vertex, edge, backptr;
            REQUIRE_NOTHROW( vt->get(11, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 0); // rhs
            REQUIRE(backptr == 0);
            // Vertex 20
            REQUIRE(vt->get(21, node).leaf() == leaf);
            REQUIRE(vt->get(21, node).segment() == segment1);
            REQUIRE(vt->get(21, node).get_segment_id() == 1);
            REQUIRE(vt->get(21, node).get_segment_version() == segment1->get_version());
            REQUIRE(vt->get(21, node).has_filepos() == true);
            REQUIRE_NOTHROW( vt->get(21, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 0);
            REQUIRE(backptr == 0);
            // Vertex 30
            REQUIRE(vt->get(31, node).leaf() == leaf);
            REQUIRE(vt->get(31, node).segment() == segment1);
            REQUIRE(vt->get(31, node).get_segment_id() == 1);
            REQUIRE(vt->get(31, node).get_segment_version() == segment1->get_version());
            REQUIRE(vt->get(31, node).has_filepos() == true);
            REQUIRE_NOTHROW( vt->get(31, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 2);
            REQUIRE(backptr == 1);
            // Vertex 40
            REQUIRE(vt->get(41, node).leaf() == leaf);
            REQUIRE(vt->get(41, node).segment() == segment1);
            REQUIRE(vt->get(41, node).get_segment_id() == 1);
            REQUIRE(vt->get(41, node).get_segment_version() == segment1->get_version());
            REQUIRE(vt->get(41, node).has_filepos() == true);
            REQUIRE_NOTHROW( vt->get(41, node).get_filepos(&vertex, &edge, &backptr) );
            REQUIRE(vertex == 0);
            REQUIRE(backptr == 0);
        }
    }
}

/**
 * Check that scans work even on outdated vertex tables.
 */
TEST_CASE("vt_outdated_pointer", "[vt][vertex_table]"){
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();
    [[maybe_unused]] auto memstore = context::global_context()->memstore();
    [[maybe_unused]] auto vt = memstore->vertex_table();

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(30);
    tx.insert_edge(30, 10, 1030);
    tx.commit();

    memstore->merger()->execute_now();

    tx = teseo.start_transaction();
    tx.insert_vertex(20);
    tx.insert_edge(10, 20, 1020);
    tx.commit();

    uint64_t vertex_id = 0;
    uint64_t num_hits = 0;
    auto check = [&vertex_id, &num_hits](uint64_t destination, double weight){
        if(vertex_id == 10){
            REQUIRE(num_hits <= 1); // 2 edges

            if(num_hits == 0){
                REQUIRE(destination == 20);
                REQUIRE(weight == 1020);
            } else if (num_hits == 1){
                REQUIRE(destination == 30);
                REQUIRE(weight == 1030);
            }
        } else if (vertex_id == 20){
            REQUIRE(num_hits == 0);
            REQUIRE(destination == 10);
            REQUIRE(weight == 1020);
        } else if (vertex_id == 30){
            REQUIRE(num_hits == 0);
            REQUIRE(destination == 10);
            REQUIRE(weight == 1030);
        } else {
            REQUIRE( false ); // invalid vertex ID
        }

        num_hits ++;
        return true;
    };

    tx = teseo.start_transaction(/* read only ? */ true);
    auto it = tx.iterator();

    vertex_id = 10;
    num_hits = 0;
    it.edges(10, false, check);

    vertex_id = 30;
    num_hits = 0;
    it.edges(30, false, check);

    vertex_id = 20;
    num_hits = 0;
    it.edges(20, false, check);
}

