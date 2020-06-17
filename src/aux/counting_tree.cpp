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
#include "teseo/aux/counting_tree.hpp"

#include <cassert>
#include <cstdlib> // malloc
#include <cstring> // memcpy
#include <iomanip>
#include <iostream>
#include <type_traits>

#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"

using namespace std;

namespace teseo::aux {

CountingTree::CountingTree() : m_root(create_leaf()), m_cardinality(0), m_height(1) {

}

CountingTree::CountingTree(CountingTree&& ct) : m_root(ct.m_root), m_cardinality(ct.m_cardinality), m_height(ct.m_height) {
    ct.m_root = nullptr;
    ct.m_cardinality = 0;
    ct.m_height = 0;
}



CountingTree::~CountingTree(){
    if(m_root != nullptr) { // it can be null when altered by the move ctor
        delete_node(m_root, 0);
        m_root = nullptr;
    }
}

bool CountingTree::Node::empty() const {
    return N == 0;
}

uint64_t* CountingTree::KEYS(const InternalNode* inode) const{
    InternalNode* instance = const_cast<InternalNode*>(inode);
    return reinterpret_cast<uint64_t*>(instance +1);
}

uint64_t* CountingTree::RANKS(const InternalNode* inode) const{
    return KEYS(inode) + m_inode_B;
}

CountingTree::Node** CountingTree::CHILDREN(const InternalNode* inode) const {
    return reinterpret_cast<Node**>(RANKS(inode) + m_inode_B +1);
}

ItemUndirected* CountingTree::ELEMENTS(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<ItemUndirected*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}

constexpr uint64_t CountingTree::memsize_internal_node() {
    return sizeof(InternalNode) + /* separator keys */ sizeof(uint64_t) * m_inode_B + /* ranks */ sizeof(uint64_t) * (m_inode_B +1) + /* children */ sizeof(Node*) * (m_inode_B +1);
}

constexpr uint64_t CountingTree::memsize_leaf() {
    return sizeof(Leaf) + /* elements */ sizeof(ItemUndirected) * m_leaf_B;
}


CountingTree::InternalNode* CountingTree::create_internal_node() {
    static_assert(!std::is_polymorphic<InternalNode>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(InternalNode) == 8, "Expected only 8 bytes for the cardinality");

    InternalNode* ptr = (InternalNode*) malloc(memsize_internal_node());
    if(ptr == nullptr) throw std::bad_alloc{};
    ptr->N = 0;

    return ptr;
}

CountingTree::Leaf* CountingTree::create_leaf() {
    static_assert(!std::is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + ptr previous + ptr next");

    // (cardinality) 1 + (ptr left/right) 2 + (keys=) leaf_b + (values) leaf_b == 2 * leaf_b + 1;
    Leaf* ptr  = (Leaf*) malloc(memsize_leaf());
    if(ptr == nullptr) throw std::bad_alloc{};
    ptr->N = 0;
    ptr->next = ptr->previous = nullptr;

    return ptr;
}

bool CountingTree::is_leaf(int depth) const {
    assert(depth < m_height);
    return (depth == m_height -1);
}

uint64_t CountingTree::size() const {
    return m_cardinality;
}

bool CountingTree::empty() const {
    return size() == 0;
}

void CountingTree::delete_node(Node* node, int depth) const {
    assert(node != nullptr);

    if(!is_leaf(depth)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);

        for(uint64_t i = 0; i < inode->N; i++){
            delete_node(children[i], depth +1);
        }
    }

    context::thread_context()->gc_mark(node, ::free);
}

void CountingTree::close(gc::GarbageCollector* gc){
    if(m_root != nullptr){
        close_rec(gc, m_root, 0);
        m_root = nullptr;
    }
}

void CountingTree::close_rec(gc::GarbageCollector* gc, Node* node, int depth){
    if(is_leaf(depth)){
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        for(uint64_t i = 0, sz = leaf->N; i < sz; i++){
            auto& item = ELEMENTS(leaf)[i];
            if(item.m_pointer.leaf() != nullptr){ // only if set
                item.m_pointer.leaf()->decr_ref_count(gc);
            }
            item.m_pointer = memstore::IndexEntry{}; // reset
        }
    } else {
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);

        for(uint64_t i = 0; i < inode->N; i++){
            close_rec(gc, children[i], depth +1);
        }
    }

    if(gc == nullptr){
        context::thread_context()->gc_mark(node, ::free);
    } else {
        gc->mark(node, ::free);
    }
}

void CountingTree::clear_item(ItemUndirected& item){
    if(item.m_pointer.leaf() != nullptr){
        item.m_pointer.leaf()->decr_ref_count();
        item.m_pointer = memstore::IndexEntry{};
    }
}

void CountingTree::insert(const ItemUndirected& item){
    // split the root when it is a leaf
    if(m_height == 1 && m_root->N == m_leaf_B){
        split_root();
    }

    do_insert(m_root, item, 0);

    // split the root when it is an internal node
    if(m_height > 1 && m_root->N > m_inode_B){
        split_root();
    }
}

void CountingTree::do_insert(Node* node, const ItemUndirected& element, int depth){
    assert(node != nullptr);
    uint64_t key = element.m_vertex_id;

    // tail recursion on the internal nodes
    while(depth < (m_height -1)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        assert(inode->N > 0);
        uint64_t i = 0, last_key = inode->N -1;
        uint64_t* __restrict keys = KEYS(inode);
        while(i < last_key && key > keys[i]) i++;
        node = CHILDREN(inode)[i];

        // before moving to its child, check whether it is full. If this is the case
        // we need to make a recursive call to check again whether we need to split the
        // node after an element has been inserted
        bool child_is_leaf = (depth + 1) >= m_height -1;
        if(child_is_leaf && node->N == m_leaf_B){
            split(inode, i, depth +1); // we already know we are going to insert an element
            if(key > KEYS(inode)[i]) node = CHILDREN(inode)[++i];
        } else if (!child_is_leaf && node->N == m_inode_B){
            do_insert(node, element, depth+1);
            RANKS(inode)[i]++;

            if(node->N > m_inode_B){ split(inode, i, depth+1); }
            return; // stop the loop
        }

        // Because we only use this tree internally, we assume that no duplicates can occur and all
        // insertions always succeed. Therefore, we can already increment the cumulative sum in advance.
        RANKS(inode)[i]++;

        depth++;
    }

    // finally, shift the elements & insert into the leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N < m_leaf_B);
    uint64_t i = leaf->N;
    ItemUndirected* __restrict elements = ELEMENTS(leaf);
    while(i > 0 && elements[i-1].m_vertex_id > key){
        elements[i] = elements[i-1];
        i--;
    }
    elements[i] = element;
    leaf->N++;

    m_cardinality += 1;
}

uint64_t CountingTree::compute_cumulative_sum(Node* node, int depth){
    if(is_leaf(depth)){
        return node->N;
    } else {
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        uint64_t* __restrict ranks = RANKS(inode);
        uint64_t sum = 0, N = inode->N;
        for(uint64_t i = 0; i < N; i++){
            sum += ranks[i];
        }
        return sum;
    }
}

void CountingTree::split_root(){
    InternalNode* root0 = create_internal_node();
    RANKS(root0)[0] = compute_cumulative_sum(m_root, 0);
    CHILDREN(root0)[0] = m_root;
    root0->N = 1;
    m_height++;
    split(root0, 0, 1);
    m_root = root0;
}

void CountingTree::split(InternalNode* inode, uint64_t child_index, int child_depth){
    assert(inode != nullptr);
    assert(child_index <= inode->N);

    bool child_is_leaf = child_depth >= m_height -1;
    uint64_t pivot { 0 };
    uint64_t rank { 0 }; // num elements / cumulative sum transferred in the new node
    Node* ptr = nullptr; // the new child

    if(child_is_leaf){
        // split a leaf in half
        Leaf* l1 = reinterpret_cast<Leaf*>(CHILDREN(inode)[child_index]);
        Leaf* l2 = create_leaf();

        assert(l1->N <= m_leaf_B);
        uint64_t thres = (l1->N +1) /2;
        l2->N = l1->N - thres;
        assert(l2->N >= m_leaf_B/2);
        l1->N = thres;
        assert(l1->N >= m_leaf_B/2);

        // move the elements from l1 to l2
        ::memcpy(ELEMENTS(l2), ELEMENTS(l1) + thres, l2->N * sizeof(ELEMENTS(l2)[0]));

        // adjust the links
        l2->next = l1->next;
        if( l2->next != nullptr ) { l2->next->previous = l2; }
        l2->previous = l1;
        l1->next = l2;

        // the threshold derives the new pivot
        pivot = ELEMENTS(l2)[0].m_vertex_id; // == l1->keys[thres]
        rank = l2->N;
        ptr = l2;

    } else { // split an internal node
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(inode)[child_index]);
        InternalNode* n2 = create_internal_node();

        uint64_t thres = n1->N /2;
        n2->N = n1->N - thres; // don't invert the assignment with n1->N
        n1->N = thres;
        assert(n1->N >= m_inode_B/2);
        assert(n2->N >= m_inode_B/2);

        // move the elements from n1 to n2
        assert(n2->N > 0);
        memcpy(KEYS(n2), KEYS(n1) + thres, (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(RANKS(n2), RANKS(n1) + thres, n2->N * sizeof(RANKS(n2)[0]));
        memcpy(CHILDREN(n2), CHILDREN(n1) + thres, n2->N * sizeof(CHILDREN(n2)[0]));

        // derive the new pivot
        pivot = KEYS(n1)[thres -1];

        // ... and the rank for n2
        rank = 0;
        uint64_t* __restrict ranks = RANKS(n2);
        for(uint64_t i = 0, N = n2->N; i < N; i++){ rank += ranks[i]; }

        ptr = n2;
    }

    // finally, add the pivot to the parent (current node)
    assert(inode->N <= m_inode_B); // when inserting, the parent is allowed to become b+1
    uint64_t* keys = KEYS(inode);
    uint64_t* ranks = RANKS(inode);
    Node** children = CHILDREN(inode);

    for(int64_t i = static_cast<int64_t>(inode->N) -1, start = child_index; i > start; i--){
        keys[i] = keys[i-1];
        ranks[i +1] = ranks[i];
        children[i +1] = children[i];
    }

    keys[child_index] = pivot;
    assert(ranks[child_index] >= rank && "Underflow");
    ranks[child_index] -= rank;
    ranks[child_index +1] = rank;
    children[child_index +1] = ptr;
    inode->N++;
}

bool CountingTree::remove(uint64_t vertex_id) {
    bool removed = do_remove(m_root, vertex_id, 0, nullptr);
    if(removed){
        m_cardinality--;

        // shorten the tree when the root contains only one child
        reduce_tree();
    }

    return removed;
}

bool CountingTree::do_remove(Node* node, uint64_t vertex_id, int depth, uint64_t* omin){
    assert(node != nullptr);

    if(is_leaf(depth)){ // leaves
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        uint64_t N = leaf->N;
        ItemUndirected* __restrict elts = ELEMENTS(leaf);
        bool removed = false;

        if(N > 0){
            if(elts[N-1].m_vertex_id == vertex_id){
                removed = true;
                clear_item(elts[N-1]);
                leaf->N -= 1;
            } else if(elts[N-1].m_vertex_id > vertex_id){
                uint64_t i = 0;
                while(i < N && elts[i].m_vertex_id < vertex_id) i++;
                if(i < N && elts[i].m_vertex_id == vertex_id){
                    removed = true;
                    clear_item(elts[i]);
                    for(uint64_t j = i; j < leaf->N -1; j++){
                        elts[j] = elts[j+1];
                    }
                    leaf->N -= 1;
                }
            }
        }

        if(omin && leaf->N > 0){
            *omin = elts[0].m_vertex_id;
        }

        return removed;

    } else { // internal nodes
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        uint64_t i = 0, N = node->N;
        assert(N > 0);
        while(i < N -1 && KEYS(inode)[i] <= vertex_id) i++;

        bool removed { false };
        if(i > 0 && KEYS(inode)[i -1] == vertex_id){ // we need to replace the pivot
            assert(omin == nullptr && "vertex_id is duplicated among the separator keys of multiple inodes");
            uint64_t pivot;
            removed = do_remove(CHILDREN(inode)[i], vertex_id, depth +1, &pivot);
            KEYS(inode)[i -1] = pivot;
        } else {
            removed = do_remove(CHILDREN(inode)[i], vertex_id, depth +1, omin);
        }

        if(removed){
            assert(RANKS(inode)[i] > 0 && "Underflow");
            RANKS(inode)[i]--;
            rebalance(inode, i, /* child depth */ depth +1);
        }

        return removed;
    }
}

void CountingTree::rebalance(InternalNode* node, uint64_t child_index, int child_depth){
    assert(node != nullptr);
    assert(node->N > 1 || node == m_root);
    assert(child_index < node->N);

    // the child already contains more than a elements => nop
    uint64_t child_sz = CHILDREN(node)[child_index]->N;
    const uint64_t lowerbound = is_leaf(child_depth) ? m_leaf_B /2 : m_inode_B /2;
    if(child_sz >= lowerbound){ return; } // nothing to do!

    // okay, if this is the root && it has only one child, there is not much we can do
    if(node == m_root && node->N <= 1) return;

    // how many nodes do we need?
    int64_t need = lowerbound - child_sz;

    // check if we can steal `need' nodes from its sibling
    bool can_rotate_right = false;
    if(child_index > 0){ // steal from left
        Node* child_left = CHILDREN(node)[child_index -1];
        if(child_left->N >= lowerbound + need +1){
            rotate_right(node, child_index, child_depth, need +1);
            return; // done
        } else {
            can_rotate_right = child_left->N >= lowerbound + need;
        }
    }

    bool can_rotate_left = false;
    if(child_index < node->N -1){ // steal from right
        Node* child_right = CHILDREN(node)[child_index +1];
        if(child_right->N >= lowerbound + need +1){
            rotate_left(node, child_index, child_depth, need +1);
            return; // done
        } else {
            can_rotate_left = child_right->N >= lowerbound + need;
        }
    }

    // we cannot steal `need +1' nodes, but maybe we can rotate just `need' nodes
    // bringing the size of child to |a|
    if(can_rotate_right){
        rotate_right(node, child_index, child_depth, need);
        return;
    }
    if(can_rotate_left){
        rotate_left(node, child_index, child_depth, need);
        return;
    }

    // both siblings contain |a -1 + a| elements, merge the nodes
    if(child_index < node->N -1){
        merge(node, child_index, child_depth);
    } else {
        assert(child_index > 0);
        merge(node, child_index -1, child_depth);
    }
}

void CountingTree::rotate_right(InternalNode* node, uint64_t child_index, int child_depth, uint64_t need){
    assert(node != nullptr);
    assert(0 < child_index && child_index < node->N);
    assert(need > 0);
    assert(CHILDREN(node)[child_index-1]->N >= need);

    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index -1];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index];

        decltype(ELEMENTS(l1)) __restrict l1_elts = ELEMENTS(l1);
        decltype(ELEMENTS(l2)) __restrict l2_elts = ELEMENTS(l2);

        // shift elements in l2 by `need'
        for(uint64_t i = l2->N -1 + need; i > 0; i--){
            l2_elts[i] = l2_elts[i - need];
        }

        // copy `need' elements from l1 to l2
        for(uint64_t i = 0; i < need; i++){
            l2_elts[i] = l1_elts[l1->N - need +i];
        }

        // update the split point
        KEYS(node)[child_index -1] = l2_elts[0].m_vertex_id;

        // update the ranks
        RANKS(node)[child_index -1] -= need;
        RANKS(node)[child_index] += need;

        // update the cardinalities
        l2->N += need;
        l1->N -= need;
    } else { // the children are internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index -1];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index];
        uint64_t rank_diff = 0;

        uint64_t* __restrict n1_keys = KEYS(n1);
        uint64_t* __restrict n1_ranks = RANKS(n1);
        Node** __restrict n1_children = CHILDREN(n1);
        uint64_t* __restrict n2_keys = KEYS(n2);
        uint64_t* __restrict n2_ranks = RANKS(n2);
        Node** __restrict n2_children = CHILDREN(n2);

        // shift elements in n2 by `need'
        if(n2->N > 0){
            n2_children[n2->N + need -1] = n2_children[n2->N -1];
            n2_ranks[n2->N + need -1] = n2_ranks[n2->N -1];
            for(uint64_t i = n2->N + need -2; i >= need; i--){
                n2_keys[i] = n2_keys[i - need];
                n2_ranks[i] = n2_ranks[i - need];
                n2_children[i] = n2_children[i - need];
            }
        }

        // move the pivot from node to n2
        n2_keys[need -1] = KEYS(node)[child_index-1];
        n2_ranks[need -1] = rank_diff = n1_ranks[n1->N -1];
        n2_children[need -1] = n1_children[n1->N -1];

        // copy the remaining elements from n1 to n2
        uint64_t idx = n1->N - need;
        for(uint64_t i = 0; i < need -1; i--){
            n2_keys[i] = n1_keys[idx];
            n2_ranks[i] = n1_ranks[idx];
            n2_children[i] = n1_children[idx];

            rank_diff += n1_ranks[idx];
            idx++;
        }

        // update the pivot
        KEYS(node)[child_index-1] = KEYS(n1)[n1->N - need -1];

        // update the ranks
        RANKS(node)[child_index -1] -= rank_diff;
        RANKS(node)[child_index] += rank_diff;

        n1->N -= need;
        n2->N += need;
    }
}

void CountingTree::rotate_left(InternalNode* node, uint64_t child_index, int child_depth, uint64_t need){
    assert(node != nullptr);
    assert(0 <= child_index && child_index < node->N);
    assert(CHILDREN(node)[child_index+1]->N >= need);

    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];

        decltype(ELEMENTS(l1)) __restrict l1_elts = ELEMENTS(l1);
        decltype(ELEMENTS(l2)) __restrict l2_elts = ELEMENTS(l2);

        // move `need' elements of l2 in l1
        for(uint64_t i = 0; i < need; i++){
            l1_elts[l1->N + i] = l2_elts[i];
        }

        // left shift elements by `need' in l2
        for(int64_t i = 0, sz = l2->N -need; i < sz; i++){
            l2_elts[i] = l2_elts[i + need];
        }

        // update the pivot
        KEYS(node)[child_index] = l2_elts[0].m_vertex_id;

        // update the ranks
        RANKS(node)[child_index] += need;
        RANKS(node)[child_index +1] -= need;

        // adjust the sizes
        l1->N += need;
        l2->N -= need;
    } else { // internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index +1];
        uint64_t rank_diff = 0;

        uint64_t* __restrict n1_keys = KEYS(n1);
        uint64_t* __restrict n1_ranks = RANKS(n1);
        Node** __restrict n1_children = CHILDREN(n1);
        uint64_t* __restrict n2_keys = KEYS(n2);
        uint64_t* __restrict n2_ranks = RANKS(n2);
        Node** __restrict n2_children = CHILDREN(n2);

        // add the pivot to n1
        assert(n1->N > 0);
        n1_keys[n1->N -1] = KEYS(node)[child_index];
        n1_ranks[n1->N] = rank_diff = n2_ranks[0];
        n1_children[n1->N] = n2_children[0];

        // move 'need -1' elements from n2 to n1
        uint64_t idx = n1->N;
        for(uint64_t i = 0; i < need -1; i++){
            n1_keys[idx] = n2_keys[i];
            n1_ranks[idx + 1] = n2_ranks[i];
            n1_children[idx +1] = n2_children[i +1];

            rank_diff += n2_ranks[i];
            idx++;
        }

        // update the pivot
        KEYS(node)[child_index] = n2_keys[need -1];

        // update the ranks
        RANKS(node)[child_index] += rank_diff;
        RANKS(node)[child_index +1] -= rank_diff;

        // left shift elements by `need' in n2
        for(uint64_t i = 0, sz = n2->N -need -1; i < sz; i++){
            n2_keys[i] = n2_keys[i+need];
            n2_ranks[i] = n2_ranks[i+need];
            n2_children[i] = n2_children[i+need];
        }
        n2_ranks[n2->N -need -1] = n2_ranks[n2->N -1];
        n2_children[n2->N -need -1] = n2_children[n2->N -1];

        // adjust the sizes
        n1->N += need;
        n2->N -= need;
    }
}

void CountingTree::merge(InternalNode* node, uint64_t child_index, int child_depth){
    assert(node != nullptr);
    assert(child_index +1 <= node->N);

    // merge two adjacent leaves
    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];
        assert(l1->N + l2->N <= m_leaf_B);

        // move all elements from l2 to l1
        ::memcpy(ELEMENTS(l1) + l1->N, ELEMENTS(l2), l2->N * sizeof(ELEMENTS(l2)[0]));

        // update the sizes of the two leaves
        l1->N += l2->N;
        l2->N = 0;

        // adjust the links
        l1->next = l2->next;
        if(l2->next != nullptr){ l2->next->previous = l1; }

        // free the memory from l2
        delete_node(l2, child_depth); l2 = nullptr;
    }

    // merge two adjacent internal nodes
    else {
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(node)[child_index]);
        InternalNode* n2 = reinterpret_cast<InternalNode*>(CHILDREN(node)[child_index +1]);
        assert(n1->N + n2->N + 1 <= m_inode_B);

        // move the pivot into n1
        KEYS(n1)[n1->N -1] = KEYS(node)[child_index];
        RANKS(n1)[n1->N] = RANKS(n2)[0];
        CHILDREN(n1)[n1->N] = CHILDREN(n2)[0];

        // move all elements from n2 to n1 (except the first pointer from n2)
        assert(n2->N > 0);
        ::memcpy(KEYS(n1) + n1->N, KEYS(n2), (n2->N -1) * sizeof(KEYS(n2)[0]));
        ::memcpy(RANKS(n1) + n1->N +1, RANKS(n2) +1, (n2->N -1) * sizeof(RANKS(n2)[0]));
        ::memcpy(CHILDREN(n1) + n1->N +1, CHILDREN(n2) +1, (n2->N -1) * sizeof(CHILDREN(n2)[0]));

        // update the sizes of the two nodes
        n1->N += n2->N;
        n2->N = 0;

        // deallocate the intermediate node
        delete_node(n2, child_depth); n2 = nullptr;
    }

    // Finally, remove the pivot from the parent (current node).
    // node->N might become |a-1|, this is still okay in a remove operation as we are
    // going to rebalance this node in post-order
    uint64_t* keys = KEYS(node);
    uint64_t* ranks = RANKS(node);
    Node** children = CHILDREN(node);
    ranks[child_index] += ranks[child_index +1]; // update the cumulative sum
    for(uint64_t i = child_index +1, last = node->N -1; i < last; i++){
        keys[i -1] = keys[i];
        ranks[i] = ranks[i +1];
        children[i] = children[i+1];
    }
    node->N--;
}

bool CountingTree::reduce_tree() {
    bool result = false;

    while(m_height > 1 && m_root->N == 1){
        InternalNode* inode = reinterpret_cast<InternalNode*>(m_root);
        m_root = CHILDREN(inode)[0];
        inode->N = 0;
        delete_node(inode, 0);
        m_height--;

        result = true;
    }

    return result;
}


pair<ItemUndirected*, uint64_t> CountingTree::get_by_vertex_id(uint64_t vertex_id) {
    Node* node = m_root; // start from the root
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    uint64_t cumulative_sum = 0;
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        uint64_t i = 0, N = inode->N -1;
        assert(N > 0 && N <= m_inode_B);
        uint64_t* __restrict keys = KEYS(inode);
        uint64_t* __restrict ranks = RANKS(inode);
        while(i < N && keys[i] <= vertex_id) {
            cumulative_sum += ranks[i];
            i++;
        }
        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    uint64_t i = 0, N = leaf->N;
    ItemUndirected* __restrict elts = ELEMENTS(leaf);
    while(i < N && elts[i].m_vertex_id < vertex_id) i++;
    if (i < N && elts[i].m_vertex_id == vertex_id){
        return make_pair(elts + i, cumulative_sum + i);
    } else { // not found
        return pair<ItemUndirected*, uint64_t>{nullptr, numeric_limits<uint64_t>::max()};
    }
}

pair<const ItemUndirected*, uint64_t> CountingTree::get_by_vertex_id(uint64_t vertex_id) const {
    return const_cast<CountingTree*>(this)->get_by_vertex_id(vertex_id); // C++ bloatware
}

bool CountingTree::get_by_vertex_id_optimistic(uint64_t vertex_id, util::OptimisticLatch<0>& latch, uint64_t version, ItemUndirected* output_item, uint64_t* output_rank) const {
    assert(context::thread_context()->epoch() != std::numeric_limits<uint64_t>::max() && "Usage of optimistic latches => Need to be inside an epoch to protect yourself from the G.C.");

    Node* node = m_root; // start from the root

    // use tail recursion on the internal nodes
    uint64_t cumulative_sum = 0;
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        latch.validate_version(version); // inode is a valid pointer
        uint64_t i = 0, N = inode->N -1;
        uint64_t* __restrict keys = KEYS(inode);
        uint64_t* __restrict ranks = RANKS(inode);
        latch.validate_version(version); // the fields accessed are valid

        while(i < N && keys[i] <= vertex_id) {
            cumulative_sum += ranks[i];
            i++;
        }
        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    latch.validate_version(version); // leaf is a valid pointer
    uint64_t i = 0, N = leaf->N;
    ItemUndirected* __restrict elts = ELEMENTS(leaf);
    latch.validate_version(version); // N is a valid value
    while(i < N && elts[i].m_vertex_id < vertex_id) i++;
    if (i < N && elts[i].m_vertex_id == vertex_id){
        if(output_item != nullptr){ *output_item = elts[i]; }
        if(output_rank != nullptr){ *output_rank = cumulative_sum + i; }
        latch.validate_version(version);
        return true;
    } else { // not found
        latch.validate_version(version);
        return false;
    }
}

ItemUndirected* CountingTree::get_by_rank(uint64_t rank) {
    if(rank >= size()) return nullptr; // it must be in [0, cardinality)

    Node* node = m_root; // start from the root
    uint64_t cumulative_sum = 0;
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        uint64_t i = 0;
        uint64_t* __restrict ranks = RANKS(inode);

        // there is no need to check the cardinality of the inode as we use the last rank in inode as as sentinel
        while(rank >= cumulative_sum + ranks[i]){
            cumulative_sum += ranks[i];
            i++;
        }

        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    uint64_t pos = rank - cumulative_sum;
    assert(pos < leaf->N && "This is not the correct leaf");
    return ELEMENTS(leaf) + pos;
}

const ItemUndirected* CountingTree::get_by_rank(uint64_t rank) const {
    return const_cast<CountingTree*>(this)->get_by_rank(rank); // C++ bloatware
}

bool CountingTree::get_by_rank_optimistic(uint64_t rank, util::OptimisticLatch<0>& latch, uint64_t version, ItemUndirected* output) const {
    assert(context::thread_context()->epoch() != std::numeric_limits<uint64_t>::max() && "Usage of optimistic latches => Need to be inside an epoch to protect yourself from the G.C.");

    if(rank >= size()){  // it must be in [0, cardinality)
        latch.validate_version(version);
        return false;
    }

    Node* node = m_root; // start from the root
    uint64_t cumulative_sum = 0;
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        latch.validate_version(version); // inode is a valid memory location
        uint64_t i = 0;
        uint64_t sz = inode->N;
        uint64_t* __restrict ranks = RANKS(inode);

        // before iterating over a node, check the cardinality read (sz) was correct
        latch.validate_version(version);

        while(i < sz && rank >= cumulative_sum + ranks[i]){
            cumulative_sum += ranks[i];
            i++;
        }

        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    latch.validate_version(version); // leaf is a valid memory location
    uint64_t pos = rank - cumulative_sum;
    ItemUndirected* item = ELEMENTS(leaf) + pos;
    *output = *item;

    // check that we are not returning a pile of rubbish to the user
    latch.validate_version(version);

    return true;
}


void CountingTree::dump() const {
    cout << "Counting Tree, capacity inodes: " << m_inode_B << ", capacity leaves: " << m_leaf_B <<
         ", cardinality: " << size() << endl;
    do_dump(cout, m_root, 0, 0);
}

void CountingTree::do_dump(ostream& out, Node* node, uint64_t cumulative_sum, int depth) const {
    assert(node != nullptr);
    assert(depth < m_height);

    const bool is_leaf = depth == m_height -1;

    { // preamble
        auto flags = out.flags();
        if (depth > 0) out << ' ';
        out << setw(depth * 2) << setfill(' '); // initial padding
        out << "[" << setw(2) << setfill('0') << depth << "] ";
        out << (is_leaf ? "L" : "I") << ' ';
        out << hex << node << dec << " N: " << node->N << '\n';
        out.setf(flags);
    }

    // tabs
    auto dump_tabs = [&out, depth](){
        auto flags = out.flags();
        out << setw(depth * 2 + 5) << setfill(' ') << ' ';
        out.setf(flags);
    };

    if (!is_leaf){ // internal node
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        auto flags = out.flags();
        dump_tabs();
        out << "Keys: ";
        if(inode->N > 0){
            for(uint64_t i = 0; i < inode->N -1; i++){
                if(i > 0) out << ", ";
                out << i << ": " << KEYS(inode)[i];
            }
        }
        out << '\n';
        dump_tabs();
        out << "Ranks: ";
        uint64_t cs0 = cumulative_sum;
        for(uint64_t i =0; i < inode->N; i++){
            if(i > 0) out << ", ";
            out << cs0 << " (" << RANKS(inode)[i] << ")";

            cs0 += RANKS(inode)[i];
        }

        out << "\n";
        dump_tabs();
        out << "Ptrs: " << hex;
        for(size_t i = 0; i < inode->N; i++){
            if(i > 0) out << ", ";
            out << i << ": " << CHILDREN(inode)[i];
        }
        out << dec << endl;
        out.setf(flags);

        // recurse
        cs0 = cumulative_sum;
        for(int i = 0, sz = inode->N; i < sz; i++){
            do_dump(out, CHILDREN(inode)[i], cs0, depth+1);
            cs0 += RANKS(inode)[i];
        }
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->empty()) return;

        auto flags = out.flags();
        for(size_t i = 0; i < leaf->N; i++){
            dump_tabs();
            out << i << "@" << (cumulative_sum +i) << ": " << ELEMENTS(leaf)[i] << "\n";
        }

        dump_tabs();
        out << "Prev: " <<  leaf->previous << ", Next: " << leaf->next;

        out << endl;

        out.setf(flags);
    }
}

} // namespace
