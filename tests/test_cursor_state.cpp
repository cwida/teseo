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

#include <iostream>
#include <string>

#include "teseo/context/global_context.hpp"
#include "teseo/context/scoped_epoch.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/cursor_state.hpp"
#include "teseo/memstore/direct_pointer.hpp"
#include "teseo/memstore/index.hpp"
#include "teseo/memstore/latch_state.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
#include "teseo/memstore/scan.hpp"
#include "teseo/memstore/segment.hpp"
#include "teseo/runtime/runtime.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;
using namespace teseo::memstore;

/**
 * Check that we are able to use a CursorState in an empty segment, that is, it does not raise
 * any assertion or it does not crash.
 */
TEST_CASE("cs_empty", "[cs] [cursor_state]"){
    Teseo teseo;
    auto tx = teseo.start_transaction(/* read only ? */ true);

    Memstore* memstore = context::global_context()->memstore();
    Context context { memstore };
    context::ScopedEpoch epoch;
    context.m_leaf = context.m_tree->index()->find(0).leaf();
    context.m_segment = context.m_leaf->get_segment(0);
    Key key { 11 };

    CursorState cs;
    Segment::scan</* fetch weights ? */ true>(context, key, nullptr, &cs, [](uint64_t, uint64_t, double){ return true; });

    REQUIRE(cs.is_valid() == false);
}

/**
 * Check the usage of the cursor state on a sparse file.
 */
TEST_CASE("cs_sparse_file", "[cs] [cursor_state]"){
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();

    auto tx = teseo.start_transaction(/* read only ? */ false);
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(20, 30, 2030);
    tx.commit();

    context::global_context()->runtime()->rebalance_first_leaf();

    Memstore* memstore = context::global_context()->memstore();
    //memstore->dump();

    uint64_t num_hits = 0;
    Key key { 11 };

    auto check = [&num_hits, &key](uint64_t source, uint64_t destination, double weight){
        //cout << "[check] source: " << source << ", destination: " << destination << ", key: " << key << ", num_hits: " << num_hits << endl;
        if(source != key.source()) return false;
        num_hits ++;

        if(num_hits == 1){ // process the vertex
            REQUIRE(source == key.source());
            REQUIRE(destination == 0);
        } else if(key.source() == 11){
            uint64_t expected_destination = num_hits * 10 + 1; // 20, 30.. so on
            REQUIRE(source == key.source());
            REQUIRE(destination == expected_destination);
        } else if(key.source() == 21){
            REQUIRE(source == key.source());
            if(num_hits == 2){
                REQUIRE(destination == 11);
            } else {
                REQUIRE(destination == 31);
            }
        }

        return true;
    };

    Context context { memstore, (teseo::transaction::TransactionImpl*) tx.handle_impl() };
    {
        context::ScopedEpoch epoch;
        context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(0);
    }

    CursorState cs;

    // scan vertex 11
    num_hits = 0;
    key = 11;
    Segment::scan</* fetch weights ? */ true>(context, key, nullptr, &cs, check);
    REQUIRE(num_hits == 3); // the vertex 10 (e2i 11), the edges 10->20, 10->30 and 10->40
    REQUIRE(cs.is_valid() == true);
    REQUIRE(cs.key() == Key{21});
    REQUIRE(cs.position().leaf() == context.m_leaf);

    // scan vertex 21
    num_hits = 0;
    key = 21;
    bool read_next = Segment::scan</* fetch weights ? */ true>(context, key, &cs.position(), &cs, check);
    REQUIRE(read_next == true);
    REQUIRE(cs.is_valid() == false);
    // move to the second segment
    context.m_segment = context.m_leaf->get_segment(1);
    read_next = Segment::scan</* fetch weights ? */ true>(context, key, nullptr, &cs, check);
    REQUIRE(read_next == false);
    REQUIRE(num_hits == 3); // the vertex 20 (e2i 21), the edges 20->10 and 20->30
    REQUIRE(cs.is_valid() == true);
    REQUIRE(cs.key() == Key{31});

    // we didn't grab any latch in the first place
    cs.invalidate();
}

/**
 * Perform the same as `cs_sparse_file' using the interface from Memstore. Validate that the held
 * latches are eventually correctly released.
 */
TEST_CASE("cs_memstore1", "[cs] [cursor_state]"){
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();

    auto tx = teseo.start_transaction(/* read only ? */ false);
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(20, 30, 2030);
    tx.commit();

    context::global_context()->runtime()->rebalance_first_leaf();

    Memstore* memstore = context::global_context()->memstore();
    Segment* segment0 { nullptr };
    Segment* segment1 { nullptr };
    {
        context::ScopedEpoch epoch;
        auto leaf = memstore->index()->find(0).leaf();
        segment0 = leaf->get_segment(0);
        segment1 = leaf->get_segment(1);
    }

    uint64_t num_hits = 0;
    Key key { 11 };
    auto check = [&num_hits, &key](uint64_t source, uint64_t destination, double weight){
        //cout << "[check] source: " << source << ", destination: " << destination << ", key: " << key << ", num_hits: " << num_hits << endl;
        if(source != key.source()) return false;
        num_hits ++;

        if(num_hits == 1){ // process the vertex
            REQUIRE(source == key.source());
            REQUIRE(destination == 0);
        } else if(key.source() == 11){
            uint64_t expected_destination = num_hits * 10 + 1; // 20, 30.. so on
            REQUIRE(source == key.source());
            REQUIRE(destination == expected_destination);
        } else {
            REQUIRE(source == key.source());
            if(num_hits == 2){ // first edge
                REQUIRE(destination == 11);
            } else { // second edge
                REQUIRE(destination == 31);
            }
        }

        return true;
    };

    CursorState cs;
    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<teseo::transaction::TransactionImpl*>(tx.handle_impl());

    // scan vertex 11
    num_hits = 0;
    key = 11;
    memstore->scan</* fetch weights ? */ true>(tx_impl, 11, 0, &cs, check);
    REQUIRE(num_hits == 3); // the vertex 10 (e2i 11), the edges 10->20, 10->30 and 10->40
    REQUIRE(cs.is_valid() == true);
    REQUIRE(cs.key() == Key{21});

    // the iterator should still hold a latch to segment 0
    REQUIRE( segment0->get_state() == Segment::State::READ );
    REQUIRE( segment0->latch_state().m_readers == 1 );
    REQUIRE( segment1->get_state() == Segment::State::FREE );
    REQUIRE( segment1->latch_state().m_readers == 0 );

    // scan vertex 21
    num_hits = 0;
    key = 21;
    memstore->scan</* fetch weights ? */ true>(tx_impl, 21, 0, &cs, check);
    REQUIRE(num_hits == 3); // the vertex 20 (e2i 21), the edges 20->10 and 20->30
    REQUIRE(cs.is_valid() == true);
    REQUIRE(cs.key() == Key{31});

    REQUIRE( segment0->get_state() == Segment::State::FREE );
    REQUIRE( segment0->latch_state().m_readers == 0 );
    REQUIRE( segment1->get_state() == Segment::State::READ );
    REQUIRE( segment1->latch_state().m_readers == 1 );

    cs.close();

    REQUIRE( segment1->get_state() == Segment::State::FREE );
    REQUIRE( segment1->latch_state().m_readers == 0 );
}

/**
 * Again check that the latches are correctly released when using a cursor state. Slightly different
 * setting of cs_memstore1, with the last edge of vertex 10 at the end of the first segment.
 */
TEST_CASE("cs_memstore2", "[cs] [cursor_state]"){
    Teseo teseo;
    context::global_context()->runtime()->disable_rebalance();

    auto tx = teseo.start_transaction(/* read only ? */ false);
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    context::global_context()->runtime()->rebalance_first_leaf();

    Memstore* memstore = context::global_context()->memstore();
    //memstore->dump();

    Segment* segment0 { nullptr };
    Segment* segment1 { nullptr };
    {
        context::ScopedEpoch epoch;
        auto leaf = memstore->index()->find(0).leaf();
        segment0 = leaf->get_segment(0);
        segment1 = leaf->get_segment(1);
    }

    uint64_t num_hits = 0;
    Key key { 11 };
    auto check = [&num_hits, &key](uint64_t source, uint64_t destination, double weight){
        //cout << "[check] source: " << source << ", destination: " << destination << ", key: " << key << ", num_hits: " << num_hits << endl;
        if(source != key.source()) return false;
        num_hits ++;

        if(num_hits == 1){ // process the vertex
            REQUIRE(source == key.source());
            REQUIRE(destination == 0);
        } else if(key.source() == 11){
            uint64_t expected_destination = num_hits * 10 + 1; // 20, 30.. so on
            REQUIRE(source == key.source());
            REQUIRE(destination == expected_destination);
        } else {
            REQUIRE(source == key.source());
            REQUIRE(destination == 11);
        }

        return true;
    };

    CursorState cs;
    tx = teseo.start_transaction(/* read only ? */ true);
    auto tx_impl = reinterpret_cast<teseo::transaction::TransactionImpl*>(tx.handle_impl());

    // scan vertex 11
    num_hits = 0;
    key = 11;
    memstore->scan</* fetch weights ? */ true>(tx_impl, 11, 0, &cs, check);
    REQUIRE(num_hits == 4); // the vertex 10 (e2i 11), the edges 10->20, 10->30 and 10->40
    REQUIRE(cs.is_valid() == true);
    REQUIRE(cs.key() == Key{21});

    // the iterator should still hold a latch to segment 0
    REQUIRE( segment0->get_state() == Segment::State::FREE );
    REQUIRE( segment0->latch_state().m_readers == 0 );
    REQUIRE( segment1->get_state() == Segment::State::READ );
    REQUIRE( segment1->latch_state().m_readers == 1 );

    // scan vertex 21
    num_hits = 0;
    key = 21;
    memstore->scan</* fetch weights ? */ true>(tx_impl, 21, 0, &cs, check);
    REQUIRE(num_hits == 2); // the vertex 20 (e2i 21), the edge 20->10
    REQUIRE(cs.is_valid() == true);
    REQUIRE(cs.key() == Key{31});

    REQUIRE( segment0->get_state() == Segment::State::FREE );;
    REQUIRE( segment0->latch_state().m_readers == 0 );
    REQUIRE( segment1->get_state() == Segment::State::READ );
    REQUIRE( segment1->latch_state().m_readers == 1 );

    cs.close();

    REQUIRE( segment1->get_state() == Segment::State::FREE );
    REQUIRE( segment1->latch_state().m_readers == 0 );
}

/**
 * Use the iterator interface to scan over both sparse and dense files. Check that the held
 * latches are correctly released.
 * 30/Oct/2020 - Test case fixed for the new segment capacity
 */
TEST_CASE("cs_iterator", "[cs] [cursor_state]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = context::global_context()->memstore();
    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually
    const uint64_t max_vertex_id = 100;

    auto tx = teseo.start_transaction();
    for(uint64_t vertex_id = 10; vertex_id <= max_vertex_id; vertex_id += 10){
        tx.insert_vertex(vertex_id);
    }
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.insert_edge(10, 50, 1050);
    tx.insert_edge(10, 60, 1060);
    tx.insert_edge(10, 70, 1070);
    tx.insert_edge(10, 80, 1080);
    tx.insert_edge(10, 90, 1090);
    tx.insert_edge(10, 100, 10100);
    tx.commit();

    context::global_context()->runtime()->rebalance_first_leaf();

    Leaf* leaf { nullptr };
    {   // transform the second segment into a dense file
        context::ScopedEpoch epoch;
        Context context { memstore };
        leaf = context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = context.m_leaf->get_segment(1);
        Segment::to_dense_file(context);
    }

    //memstore->dump();

    uint64_t num_hits = 0;
    uint64_t vertex_id = 10;
    auto check = [&num_hits, &vertex_id](uint64_t destination, double){
        num_hits ++;

        if(vertex_id == 10){
            uint64_t expected_destination = (num_hits + 1) * 10;
            REQUIRE(destination == expected_destination);
        } else {
            REQUIRE(destination == 10);
            REQUIRE(num_hits == 1);
        }

        return true;
    };

    // scan vertex 10, it should end up into the dense file and therefore the CS should be invalid
    tx = teseo.start_transaction(/* read only ? */ true);
    auto it = tx.iterator();
    auto cs = reinterpret_cast<CursorState*>(it.state_impl());
    REQUIRE(cs != nullptr);

    num_hits = 0;
    vertex_id = 10;
    it.edges(vertex_id, /* logical ? */ false, check);
    REQUIRE(num_hits == 9); // 9 edges
    REQUIRE(cs->is_valid() == false); // because it terminates in a dense file

    // All latches should have been released
    REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 0 );
    REQUIRE( leaf->get_segment(1)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(1)->latch_state().m_readers == 0 );
    REQUIRE( leaf->get_segment(2)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(2)->latch_state().m_readers == 0 );

    num_hits = 0;
    vertex_id = 30;
    it.edges(vertex_id, /* logical ? */ false, check);
    REQUIRE(num_hits == 1); // 30 -> 10
    REQUIRE(cs->is_valid() == true); // it should end up on segment #2, a sparse file
    REQUIRE(cs->key() == Key{41});

    // segment #2 should still be locked
    REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 0 );
    REQUIRE( leaf->get_segment(1)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(1)->latch_state().m_readers == 0 );
    REQUIRE( leaf->get_segment(2)->get_state() == Segment::State::READ );
    REQUIRE( leaf->get_segment(2)->latch_state().m_readers == 1 );

    it.close();

    REQUIRE( leaf->get_segment(2)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(2)->latch_state().m_readers == 0 );
}

/**
 * Check that copy constructor creates different copies of the cursor state.
 */
TEST_CASE("cs_copy_ctor", "[cs] [cursor_state]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = context::global_context()->memstore();
    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    Leaf* leaf { nullptr };
    {
        context::ScopedEpoch epoch;
        Context context { memstore };
        leaf = context.m_leaf = memstore->index()->find(0).leaf();
        context.m_segment = leaf->get_segment(0);
        Segment::prune(context);
    }

    tx = teseo.start_transaction(/* read only ? */ true);
    { // restrict the scope
        auto it1 = tx.iterator();
        it1.edges(10, false, [](uint64_t, double){ return true; });
        auto cs1 = reinterpret_cast<CursorState*>(it1.state_impl());
        REQUIRE( cs1->is_valid() == true );
        REQUIRE( cs1->key() == Key{21} );

        REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
        REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 1 );

        { // restrict the scope
            auto it2 = it1;
            auto cs2 = reinterpret_cast<CursorState*>(it2.state_impl());
            REQUIRE( cs2->is_valid() == false );
            it2.edges(10, false, [](uint64_t, double){ return true; });
            REQUIRE( cs2->is_valid() == true );
            REQUIRE( cs2->key() == Key{21} );

            REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
            REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 2 );

        } // it2 goes out of scope

        REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
        REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 1 );

        // again, this time starting from vertex 20
        { // restrict the scope
            auto it2 = it1;
            auto cs2 = reinterpret_cast<CursorState*>(it2.state_impl());
            REQUIRE( cs2->is_valid() == false );
            it2.edges(20, false, [](uint64_t, double){ return true; });
            REQUIRE( cs2->is_valid() == true );
            REQUIRE( cs2->key() == Key{31} );

            REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
            REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 2 );

            // assignment operator
            it2 = it1;
            cs2 = reinterpret_cast<CursorState*>(it2.state_impl());
            REQUIRE(it2.is_open() == true );
            REQUIRE( cs2->is_valid() == false ); // reset
            REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
            REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 1 );

            it2.edges(20, false, [](uint64_t, double){ return true; });
            REQUIRE( cs2->is_valid() == true );
            REQUIRE( cs2->key() == Key{31} );

            REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
            REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 2 );
        } // it2 goes out of scope

        REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
        REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 1 );

        // cs1 should still be valid
        REQUIRE( cs1->is_valid() == true );
        REQUIRE( cs1->key() == Key{21} );
        it1.edges(20, false, [](uint64_t, double){ return true; });
        REQUIRE( cs1->is_valid() == true );
        REQUIRE( cs1->key() == Key{31} );

        REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::READ );
        REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 1 );
    } // it1 goes out of scope


    REQUIRE( leaf->get_segment(0)->get_state() == Segment::State::FREE );
    REQUIRE( leaf->get_segment(0)->latch_state().m_readers == 0 );
}

/**
 * Nested iterators. Check that the held latches are correctly released.
 */
TEST_CASE("cs_nested1", "[cs] [cursor_state]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = context::global_context()->memstore();
    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.commit();

    Segment* segment = nullptr;
    { // prune
        context::ScopedEpoch epoch;
        Context context { memstore };
        Leaf* leaf = context.m_leaf = memstore->index()->find(0).leaf();
        segment = context.m_segment = leaf->get_segment(0);
        Segment::prune(context);
    }

    tx = teseo.start_transaction(/* read only ? */ true);
    { // restrict the scope
        auto iter = tx.iterator();

        iter.edges(10, false, [segment, &iter](uint64_t, double){
            REQUIRE(segment->get_state() == Segment::State::READ);
            REQUIRE(segment->latch_state().m_readers == 1);

            iter.edges(10, false, [segment](uint64_t, double){
                // the nested iterator should have acquired a new
                REQUIRE(segment->get_state() == Segment::State::READ);
                REQUIRE(segment->latch_state().m_readers == 2);

                return false;
            });

            // nested iterators cannot use a cursor state, its latch should have been released
            // upon its termination
            REQUIRE(segment->get_state() == Segment::State::READ);
            REQUIRE(segment->latch_state().m_readers == 1);

            return false;

        });

        // due to the active cursor state, the latch should still be held
        REQUIRE(segment->get_state() == Segment::State::READ);
        REQUIRE(segment->latch_state().m_readers == 1);
    } // `iter' goes out of scope

    REQUIRE(segment->get_state() == Segment::State::FREE);
    REQUIRE(segment->latch_state().m_readers == 0);
}

/**
 * Nested iterator. Validate it over two segments this time.
 */
TEST_CASE("cs_nested2", "[cs] [cursor_state]"){
    Teseo teseo;
    [[maybe_unused]] Memstore* memstore = context::global_context()->memstore();
    context::global_context()->runtime()->disable_rebalance(); // we'll do the rebalances manually

    auto tx = teseo.start_transaction();
    tx.insert_vertex(10);
    tx.insert_vertex(20);
    tx.insert_vertex(30);
    tx.insert_vertex(40);
    tx.insert_vertex(50);
    tx.insert_edge(10, 20, 1020);
    tx.insert_edge(10, 30, 1030);
    tx.insert_edge(10, 40, 1040);
    tx.commit();

    context::global_context()->runtime()->rebalance_first_leaf();
    //memstore->dump();

    Leaf* leaf { nullptr };
    {
        context::ScopedEpoch epoch;
        leaf = memstore->index()->find(0).leaf();
    }

    tx = teseo.start_transaction(/* read only ? */ true);
    { // restrict the scope
        auto iter = tx.iterator();

        iter.edges(10, false, [leaf, &iter](uint64_t destination, double){
            //cout << "[outer] destination: " << destination << endl;

            REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::READ);
            REQUIRE(leaf->get_segment(0)->latch_state().m_readers == 1);
            REQUIRE(leaf->get_segment(1)->get_state() == Segment::State::FREE);
            REQUIRE(leaf->get_segment(1)->latch_state().m_readers == 0);


            iter.edges(30, false, [leaf](uint64_t, double){
                //cout << "[inner]" << endl;
                REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::READ);
                REQUIRE(leaf->get_segment(0)->latch_state().m_readers == 1);
                REQUIRE(leaf->get_segment(1)->get_state() == Segment::State::READ);
                REQUIRE(leaf->get_segment(1)->latch_state().m_readers == 1);

                return true;
            });

            // nested iterators do not have a cursor state, acquired latches should
            // be released upon their termination.
            REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::READ);
            REQUIRE(leaf->get_segment(0)->latch_state().m_readers == 1);
            REQUIRE(leaf->get_segment(1)->get_state() == Segment::State::FREE);
            REQUIRE(leaf->get_segment(1)->latch_state().m_readers == 0);

            return true;
        });

        // the outermost iterator should still hold a latch on segment #0, due to its cursor state
        REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::READ);
        REQUIRE(leaf->get_segment(0)->latch_state().m_readers == 1);
        REQUIRE(leaf->get_segment(1)->get_state() == Segment::State::FREE);
        REQUIRE(leaf->get_segment(1)->latch_state().m_readers == 0);
    } // `iter' goes out of scope

    REQUIRE(leaf->get_segment(0)->get_state() == Segment::State::FREE);
    REQUIRE(leaf->get_segment(0)->latch_state().m_readers == 0);
    REQUIRE(leaf->get_segment(1)->get_state() == Segment::State::FREE);
    REQUIRE(leaf->get_segment(1)->latch_state().m_readers == 0);
}
