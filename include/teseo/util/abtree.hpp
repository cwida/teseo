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

#pragma once

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <cstdlib> // posix_memalign
#include <cstdio>
#include <cstring> // memcpy
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace teseo::util {

/**
 * A basic implementation of a B+ Tree with support for duplicate keys.
 * This class is not thread safe.
 */
template<typename K, typename V>
class ABTree {
    ABTree(const ABTree&) = delete;
    ABTree& operator= (ABTree&) = delete;

    // Superclass of a node in the tree, either a leaf or an internal node
    struct Node {
        // remove the ctors
        Node() = delete;
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

        size_t N;

        bool empty() const;
    };

    // An internal node of the tree, containing the separator keys
    struct InternalNode : public Node {
        // remove the ctors
        InternalNode() = delete;
        InternalNode(const InternalNode&) = delete;
        InternalNode& operator=(const InternalNode&) = delete;
    };

    K* KEYS(const InternalNode* inode) const;
    Node** CHILDREN(const InternalNode* inode) const;

    // A leaf of the tree, containing the final elements stored
    struct Leaf : public Node {
        // remove the ctors
        Leaf() = delete;
        Leaf(const Leaf&) = delete;
        Leaf& operator=(const Leaf&) = delete;

        Leaf* next;
        Leaf* previous;
    };

    K* KEYS(const Leaf* leaf) const;
    V* VALUES(const Leaf* leaf) const;

    // Java-like iterator interface
public:
    class Iterator {
        friend class ABTree;
        const ABTree<K, V>* m_tree;
        const K m_max;
        Leaf* m_block;
        size_t m_pos;

        Iterator(const ABTree* tree, const K& max, Leaf* leaf, int64_t pos);

    public:
        // Check if there is a next element to consume
        bool has_next() const;

        // Retrieve the next item from the tree
        bool next(K* out_key, V* out_value);
    };

private:
    const size_t m_intnode_a; // lower bound for the internal nodes
    const size_t m_intnode_b; // upper bound for the internal nodes
    const size_t m_leaf_a; // lower bound for the leaves
    const size_t m_leaf_b; // upper bound for the leaves
    const size_t m_min_sizeof_inode; // the minimum size, in bytes, of an allocated InternalNode
    const size_t m_min_sizeof_leaf; // the minimum size, in bytes, of an allocated Leaf
    Node* m_root = nullptr; // the root node of the B+ Tree
    int64_t m_cardinality = 0; // number of elements inside the B+ Tree
    int m_height = 1; // number of levels, or height of the tree
    mutable size_t m_num_nodes_allocated; // internal profiling
    mutable size_t m_num_leaves_allocated; // internal profiling

    // Create a new node / leaf
    InternalNode* create_internal_node() const;
    Leaf* create_leaf() const;

    // Determine the memory size of an internal node / leaf
    size_t init_memsize_internal_node() const;
    size_t init_memsize_leaf() const;

    // Get the memory size of an internal node / leaf
    size_t memsize_internal_node() const;
    size_t memsize_leaf() const;

    // Retrieve the minimum and maximum capacities for the nodes at the given depth
    size_t get_lowerbound(int depth) const;
    size_t get_upperbound(int depth) const;

    // Delete an existing internal node or leaf
    void delete_node(Node* node, int depth) const;

    // It splits the child of `node' at index `child' in half and adds the new node as a new child of `node'.
    void split(InternalNode* inode, size_t child_index, int child_depth);

    // It increases the height of the tree by 1, by splitting the current root in half and introducing a new root.
    void split_root();

    // Insert the given key/value in the subtree rooted at the given `node'.
    void insert(Node* node, const K& key, const V& value, int depth);

    // Merge two adjacent nodes together
    void merge(InternalNode* node, size_t child_index, int child_depth);
    void rotate_left(InternalNode* node, size_t child_index, int child_depth, size_t num_nodes);
    void rotate_right(InternalNode* node, size_t child_index, int child_depth, size_t num_nodes);
    void rebalance_lb(InternalNode* node, size_t child_index, int child_depth);
    void rebalance_rec(Node* node, const K& range_min, const K& range_max, int depth);

    // Attempts to reduce the height of the tree, checking whether the root has only one child.
    bool reduce_tree();

    // It removes the elements with keys in the interval [range_min, range_max] for the subtree rooted
    // at the given `node'. It returns `true' if any of the nodes in the given subtree does not
    // respect the constraint of their size in [A, B], false otherwise.
    // The parameter `min' is an output variable to return the minimum value in the subtree.
    bool remove_keys(Node* node, const K& range_min, const K& range_max, int depth, bool* is_min_set, K* min);

    // It removes the children of the given node in the interval [index, index + length).
    void remove_subtrees(InternalNode* node, size_t index, size_t length, int children_depth);

    // Helper method, it performs the recursion of remove_subtrees
    void remove_subtrees_rec0(Node* node, int depth);

    // Remove the given interval from the sub-tree starting at provided node
    void remove(Node* node, const K& keymin, const K& keymax, int depth);

    // Remove a single element from the tree
    bool remove(Node* node, const K& key, int depth, V* out_value_removed, K* omin);

    // Check whether the nodes at the given height are leaves or internal nodes
    bool is_leaf(int depth) const;

    // Validate the parameters a, b (lower and upper bound respectively)
    void validate_bounds() const;

    // Create a Java-like iterator
    std::unique_ptr<ABTree::Iterator> create_iterator(const K& max, Leaf* block, int64_t pos) const;
    std::unique_ptr<ABTree::Iterator> leaf_scan(Leaf* leaf, const K& min, const K& max) const;

    // Recursively dump the content of the given subtree
    void dump_data(std::ostream&, Node* node, int depth) const;

public:
    /**
     * Create a new (a,b)-tree with the given capacities for the internal nodes and leaves
     */
    ABTree(size_t inode_capacity = 64, size_t leaf_capacity = 128);

    /**
     * Create a new (a,b)-tree with the bounds [iA, iB] for the inner nodes and [lA, lB] for the leaves
     */
    ABTree(size_t iA, size_t iB, size_t lA, size_t lB);

    /**
     * Destructor
     */
    ~ABTree();

    /**
     * Insert the given key/value into the (a,b)-tree
     */
    void insert(const K& key, const V& value);

    /**
     * Search the given key in the tree and returns true if found, false otherwise. The parameter
     * out_value, if not null, will be the value associated to the given when the key is found.
     */
    bool find(const K& key, V* out_value) const noexcept;

    /**
     * Invoke the given function for all elements in the range [min, max]. The function must have the following
     * signature: bool fn(const K& key, const V& value). It must return true to continue the scan to the next
     * element, otherwise false to stop the iteration.
     * The elements in the tree must not be modified while the iterator is in use.
     */
    template<typename Callback>
    void scan(const K& min, const K& max, Callback fn) const;

    /**
     * Create a java-like iterator to scan all elements in the range [min, max]. Use the iterator as:
     *
     * iterator = tree.iterator(min, max);
     * K key; V value;
     * while( iterator.next(key, value) ) {
     *    // process key, value ...
     * }
     *
     * The elements in the tree must not be modified while the iterator is in use.
     */
    std::unique_ptr<typename ABTree<K, V>::Iterator> iterator(const K& min, const K& max) const;

    /**
     * Search and remove the given key from the tree. It returns true if the key has been found and
     * removed, false otherwise. The parameter out_value, if not null, will be the old value associated
     * to the key, if the key was found. In case of multiple matches (duplicates), it removes only one
     * of the matching keys in an unspecified manner.
     */
    bool remove(const K& key, V* out_value);

    /**
     * Remove all elements in the interval [min, max]
     */
    void remove(const K& min, const K& max);

    /**
     * Retrieve the number of elements contained in the (a,b)-tree
     */
    size_t size() const;

    /**
     * Check whether the tree is empty
     */
     bool empty() const;

    /**
    * Get the minimum key currently stored in the (a,b)-tree. If the tree is empty, it throws a std::range_error
    */
    K key_min() const;

    /**
    * Get the maximum key currently stored in the (a,b)-tree. If the tree is empty, it throws a std::range_error
    */
    K key_max() const;

    /**
     * Dump the content of the tree to the given output stream
     */
    void dump(std::ostream& out = std::cout) const;

    /**
    * Report the approximate memory footprint (in bytes) of the whole data structure
    */
    size_t memory_footprint() const;
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/

#define _ABTREE_PREFETCH(ptr) __builtin_prefetch(ptr, /* 0 = read only, 1 = read/write */ 0 /*,  temporal locality 3 */)

template<typename K, typename V>
ABTree<K, V>::ABTree(size_t inode_capacity, size_t leaf_capacity) : ABTree(inode_capacity /2, inode_capacity, leaf_capacity/ 2, leaf_capacity) { }

template<typename K, typename V>
ABTree<K, V>::ABTree(size_t iA, size_t iB, size_t lA, size_t lB) :
    m_intnode_a(iA), m_intnode_b(iB), m_leaf_a(lA), m_leaf_b(lB),
    m_min_sizeof_inode(init_memsize_internal_node()), m_min_sizeof_leaf(init_memsize_leaf()),
    m_root(create_leaf()), m_cardinality(0), m_num_nodes_allocated(0), m_num_leaves_allocated(1) {
    validate_bounds();
}

template<typename K, typename V>
ABTree<K, V>::~ABTree(){
    delete_node(m_root, 0);
    m_root = nullptr;
}

template<typename K, typename V>
void ABTree<K, V>::validate_bounds() const {
    if(m_intnode_a <= 1) throw std::invalid_argument("The minimum capacity for an internal node must be greater than 1");
    if(2*m_intnode_a -1 > m_intnode_b) throw std::invalid_argument("The capacity of internal nodes must respect the constraint: A < B/2, with A = minimum capacity, B = maximum capacity of the node");
    if(m_leaf_a <= 1) throw std::invalid_argument("The minimum capacity for a leaf must be greater than 1");
    if(2*m_leaf_a -1 > m_leaf_b) throw std::invalid_argument("The capacity of leaves must respect the constraint: A < B/2, with A = minimum capacity, B = maximum capacity of the leaf");
}

template<typename K, typename V>
bool ABTree<K, V>::Node::empty() const {
    return N == 0;
}

template<typename K, typename V>
K* ABTree<K,V>::KEYS(const InternalNode* inode) const{
    InternalNode* instance = const_cast<InternalNode*>(inode);
    return reinterpret_cast<K*>(reinterpret_cast<uint8_t*>(instance) + sizeof(InternalNode));
}

template<typename K, typename V>
typename ABTree<K, V>::Node** ABTree<K, V>::CHILDREN(const InternalNode* inode) const {
    return reinterpret_cast<Node**>(KEYS(inode) + m_intnode_b);
}

template<typename K, typename V>
K* ABTree<K, V>::KEYS(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<K*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}

template<typename K, typename V>
V* ABTree<K,V>::VALUES(const Leaf* leaf) const {
    return reinterpret_cast<V*>(KEYS(leaf) + m_leaf_b);
}

template<typename K, typename V>
size_t ABTree<K, V>::size() const{
    return m_cardinality;
}

template<typename K, typename V>
bool ABTree<K, V>::empty() const{
    return size() == 0;
}

template<typename K, typename V>
bool ABTree<K, V>::is_leaf(int depth) const {
    assert(depth < m_height);
    return (depth == m_height -1);
}

template<typename K, typename V>
typename ABTree<K, V>::InternalNode* ABTree<K, V>::create_internal_node() const {
    static_assert(!std::is_polymorphic<InternalNode>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(InternalNode) == 8, "Expected only 8 bytes for the cardinality");

    // (cardinality) 1 + (keys=) intnode_b + (pointers) intnode_b +1 == 2 * intnode_b +2;
    InternalNode* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_internal_node());
    if(rc != 0) throw std::bad_alloc();
    ptr->N = 0;

    m_num_nodes_allocated++;
    return ptr;
}

template<typename K, typename V>
size_t ABTree<K, V>::init_memsize_internal_node() const {
    return sizeof(InternalNode) + /* separator keys */ sizeof(K) * m_intnode_b + /* children */ sizeof(Node*) * (m_intnode_b +1);
}

template<typename K, typename V>
size_t ABTree<K, V>::memsize_internal_node() const {
    return m_min_sizeof_inode;
}

template<typename K, typename V>
typename ABTree<K, V>::Leaf* ABTree<K, V>::create_leaf() const {
    static_assert(!std::is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + ptr previous + ptr next");

    // (cardinality) 1 + (ptr left/right) 2 + (keys=) leaf_b + (values) leaf_b == 2 * leaf_b + 1;
    Leaf* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_leaf());
    if(rc != 0) throw std::bad_alloc();
    ptr->N = 0;
    ptr->next = ptr->previous = nullptr;

    m_num_leaves_allocated++;
    return ptr;
}

template<typename K, typename V>
size_t ABTree<K, V>::init_memsize_leaf() const {
    return sizeof(Leaf) + /* keys */ sizeof(K) * m_leaf_b + /* values */ sizeof(V) * m_leaf_b;
}

template<typename K, typename V>
size_t ABTree<K, V>::memsize_leaf() const {
    return m_min_sizeof_leaf;
}

template<typename K, typename V>
size_t ABTree<K, V>::get_lowerbound(int depth) const {
    bool is_leaf = (depth == m_height -1);
    return is_leaf ? m_leaf_a : m_intnode_a;
}

template<typename K, typename V>
size_t ABTree<K, V>::get_upperbound(int depth) const {
    bool is_leaf = (depth == m_height -1);
    return is_leaf ? m_leaf_b : m_intnode_b;
}

template<typename K, typename V>
void ABTree<K, V>::delete_node(Node* node, int depth) const {
    assert(node != nullptr);
    bool is_leaf = (depth == m_height -1);

    if(!is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);

        for(size_t i = 0; i < inode->N; i++){
            delete_node(children[i], depth +1);
        }

        m_num_nodes_allocated--;
    } else {
        m_num_leaves_allocated--;
    }

    free(node);
}

template<typename K, typename V>
size_t ABTree<K, V>::memory_footprint() const {
    return sizeof(ABTree<K, V>) + m_num_nodes_allocated * memsize_internal_node() + m_num_leaves_allocated * memsize_leaf();
}

template<typename K, typename V>
K ABTree<K, V>::key_min() const {
    if(empty()) throw std::range_error("The tree is empty");
    Node* node = m_root;
    assert(node != nullptr);

    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        node = CHILDREN(inode)[0];
    }

    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0 && "Empty leaf");
    return KEYS(leaf)[0];
}

template<typename K, typename V>
K ABTree<K, V>::key_max() const {
    if(empty()) throw std::range_error("The tree is empty");
    Node* node = m_root;
    assert(node != nullptr);

    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        node = CHILDREN(inode)[inode->N -1];
    }

    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0 && "Empty leaf");
    return KEYS(leaf)[leaf->N -1];
}

template<typename K, typename V>
void ABTree<K, V>::split_root(){
    InternalNode* root0 = create_internal_node();
    CHILDREN(root0)[0] = m_root;
    root0->N = 1;
    m_height++;
    split(root0, 0, 1);
    m_root = root0;
}

template<typename K, typename V>
void ABTree<K, V>::split(InternalNode* inode, size_t child_index, int child_depth){
    assert(inode != nullptr);
    assert(child_index <= inode->N);

    bool child_is_leaf = child_depth >= m_height -1;
    K pivot;
    Node* ptr = nullptr; // the new child

    if(child_is_leaf){
        // split a leaf in half
        Leaf* l1 = reinterpret_cast<Leaf*>(CHILDREN(inode)[child_index]);
        Leaf* l2 = create_leaf();

        assert(l1->N <= m_leaf_b);

        size_t thres = (l1->N +1) /2;
        l2->N = l1->N - thres;
        assert(l2->N >= m_leaf_a);
        l1->N = thres;
        assert(l1->N >= m_leaf_a);

        // move the elements from l1 to l2
        ::memcpy(KEYS(l2), KEYS(l1) + thres, l2->N * sizeof(KEYS(l2)[0]));
        ::memcpy(VALUES(l2), VALUES(l1) + thres, l2->N * sizeof(VALUES(l2)[0]));

        // adjust the links
        l2->next = l1->next;
        if( l2->next != nullptr ) { l2->next->previous = l2; }
        l2->previous = l1;
        l1->next = l2;

        // threshold derives the new pivot
        pivot = KEYS(l2)[0]; // == l1->keys[thres]
        ptr = l2;
    }

        // split an internal node
    else {
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(inode)[child_index]);
        InternalNode* n2 = create_internal_node();

        size_t thres = n1->N /2;
        n2->N = n1->N - (thres +1);
        assert(n2->N >= m_intnode_a);
        n1->N = thres +1;
        assert(n1->N >= m_intnode_a);

        // move the elements from n1 to n2
        assert(n2->N > 0);
        memcpy(KEYS(n2), KEYS(n1) + thres + 1, (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(CHILDREN(n2), CHILDREN(n1) + thres + 1, n2->N * sizeof(CHILDREN(n2)[0]));

        // derive the new pivot
        pivot = KEYS(n1)[thres];
        ptr = n2;
    }

    // finally, add the pivot to the parent (current node)
    assert(inode->N <= m_intnode_b); // when inserting, the parent is allowed to become b+1
    K* keys = KEYS(inode);
    Node** children = CHILDREN(inode);

    for(int64_t i = static_cast<int64_t>(inode->N) -1, child_index_signed = child_index; i > child_index_signed; i--){
        keys[i] = keys[i-1];
        children[i +1] = children[i];
    }

    keys[child_index] = pivot;
    children[child_index +1] = ptr;
    inode->N++;
}

template<typename K, typename V>
void ABTree<K, V>::insert(Node* node, const K& key, const V& value, int depth){
    assert(node != nullptr);

    // tail recursion on the internal nodes
    while(depth < (m_height -1)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        assert(inode->N > 0);
        size_t i = 0, last_key = inode->N -1;
        K* __restrict keys = KEYS(inode);
        while(i < last_key && key > keys[i]) i++;
        node = CHILDREN(inode)[i];

        // before moving to its child, check whether it is full. If this is the case
        // we need to make a recursive call to check again whether we need to split the
        // node after an element has been inserted
        bool child_is_leaf = (depth + 1) >= m_height -1;
        if(child_is_leaf && node->N == m_leaf_b){
            split(inode, i, depth +1); // we already know we are going to insert an element
            if(key > KEYS(inode)[i]) node = CHILDREN(inode)[++i];
        } else if (!child_is_leaf && node->N == m_intnode_b){
            insert(node, key, value, depth+1);
            if(node->N > m_intnode_b){ split(inode, i, depth+1); }
            return; // stop the loop
        }

        depth++;
    }

    // finally, shift the elements & insert into the leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N < m_leaf_b);
    size_t i = leaf->N;
    K* __restrict keys = KEYS(leaf);
    V* __restrict values = VALUES(leaf);
    while(i > 0 && keys[i-1] > key){
        keys[i] = keys[i-1];
        values[i] = values[i-1];
        i--;
    }
    keys[i] = key;
    values[i] = value;
    leaf->N++;

    m_cardinality += 1;
}

template<typename K, typename V>
void ABTree<K, V>::insert(const K& key, const V& value){
    // split the root when it is a leaf
    if(m_height == 1 && m_root->N == m_leaf_b){
        split_root();
    }

    insert(m_root, key, value, 0);

    // split the root when it is an internal node
    if(m_height > 1 && m_root->N > m_intnode_b){
        split_root();
    }
}

template<typename K, typename V>
void ABTree<K, V>::merge(InternalNode* node, size_t child_index, int child_depth){
    assert(node != nullptr);
    assert(child_index +1 <= node->N);

    // merge two adjacent leaves
    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];
        assert(l1->N + l2->N <= m_leaf_b);

        // move all elements from l2 to l1
        ::memcpy(KEYS(l1) + l1->N, KEYS(l2), l2->N * sizeof(KEYS(l2)[0]));
        ::memcpy(VALUES(l1) + l1->N, VALUES(l2), l2->N * sizeof(VALUES(l2)[0]));

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
        assert(n1->N + n2->N + 1 <= m_intnode_b);

        // move the pivot into n1
        KEYS(n1)[n1->N -1] = KEYS(node)[child_index];
        CHILDREN(n1)[n1->N] = CHILDREN(n2)[0];

        // move all elements from n2 to n1 (except the first pointer from n2)
        assert(n2->N > 0);
        memcpy(KEYS(n1) + n1->N, KEYS(n2), (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(CHILDREN(n1) + n1->N +1, CHILDREN(n2) +1, (n2->N -1) * sizeof(CHILDREN(n2)[0]));

        // update the sizes of the two nodes
        n1->N += n2->N;
        n2->N = 0;

        // deallocate the intermediate node
        delete_node(n2, child_depth); n2 = nullptr;
    }

    // finally, remove the pivot from the parent (current node)
    assert(node->N >= get_lowerbound(child_depth -1) || node == m_root);
    // node->N might become |a-1|, this is still okay in a remove operation as we are
    // going to rebalance this node in post-order
    K* keys = KEYS(node);
    Node** children = CHILDREN(node);
    for(size_t i = child_index +1, last = node->N -1; i < last; i++){
        keys[i -1] = keys[i];
        children[i] = children[i+1];
    }
    node->N--;
}

template<typename K, typename V>
void ABTree<K, V>::rotate_right(InternalNode* node, size_t child_index, int child_depth, size_t need){
    assert(node != nullptr);
    assert(0 < child_index && child_index < node->N);
    assert(need > 0);
    assert(CHILDREN(node)[child_index-1]->N >= need);
    assert(CHILDREN(node)[child_index]->N + need <= get_upperbound(child_depth));

    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index -1];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index];

        K* __restrict l1_keys = KEYS(l1);
        V* __restrict l1_values = VALUES(l1);
        K* __restrict l2_keys = KEYS(l2);
        V* __restrict l2_values = VALUES(l2);

        // shift elements in l2 by `need'
        for(size_t i = l2->N -1 + need; i > 0; i--){
            l2_keys[i] = l2_keys[i - need];
            l2_values[i] = l2_values[i - need];
        }

        // copy `need' elements from l1 to l2
        for(size_t i = 0; i < need; i++){
            l2_keys[i] = l1_keys[l1->N - need +i];
            l2_values[i] = l1_values[l1->N - need +i];
        }
        // update the split point
        KEYS(node)[child_index -1] = l2_keys[0];

        // update the cardinalities
        l2->N += need;
        l1->N -= need;
    } else { // the children are internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index -1];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index];

        K* __restrict n2_keys = KEYS(n2);
        Node** __restrict n2_children = CHILDREN(n2);
        K* __restrict n1_keys = KEYS(n1);
        Node** __restrict n1_children = CHILDREN(n1);

        // shift elements in n2 by `need'
        if(n2->N > 0){
            n2_children[n2->N + need -1] = n2_children[n2->N -1];
            for(size_t i = n2->N + need -2; i >= need; i--){
                n2_keys[i] = n2_keys[i - need];
                n2_children[i] = n2_children[i - need];
            }
        }
        // move the pivot from node to n2
        n2_keys[need -1] = KEYS(node)[child_index-1];
        n2_children[need -1] = CHILDREN(n1)[n1->N -1];

        // copy the remaining elements from n1 to n2
        size_t idx = n1->N - need;
        for(size_t i = 0; i < need -1; i--){
            n2_keys[i] = n1_keys[idx];
            n2_children[i] = n1_children[idx];
            idx++;
        }

        // update the pivot
        KEYS(node)[child_index-1] = KEYS(n1)[n1->N - need -1];

        n2->N += need;
        n1->N -= need;
    }
}

template<typename K, typename V>
void ABTree<K, V>::rotate_left(InternalNode* node, size_t child_index, int child_depth, size_t need){
    assert(node != nullptr);
    assert(0 <= child_index && child_index < node->N);
    assert(CHILDREN(node)[child_index]->N + need <= get_upperbound(child_depth));
    assert(CHILDREN(node)[child_index+1]->N >= need);

    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];

        K* __restrict l1_keys = KEYS(l1);
        V* __restrict l1_values = VALUES(l1);
        K* __restrict l2_keys = KEYS(l2);
        V* __restrict l2_values = VALUES(l2);

        // move `need' elements of l2 in l1
        for(size_t i = 0; i < need; i++){
            l1_keys[l1->N + i] = l2_keys[i];
            l1_values[l1->N + i] = l2_values[i];
        }

        // left shift elements by `need' in l2
        for(int64_t i = 0, sz = l2->N -need; i < sz; i++){
            l2_keys[i] = l2_keys[i+need];
            l2_values[i] = l2_values[i+need];
        }

        // update the pivot
        KEYS(node)[child_index] = l2_keys[0];

        // adjust the sizes
        l1->N += need;
        l2->N -= need;
    } else { // internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index +1];

        K* __restrict n1_keys = KEYS(n1);
        Node** __restrict n1_children = CHILDREN(n1);
        K* __restrict n2_keys = KEYS(n2);
        Node** __restrict n2_children = CHILDREN(n2);

        // add the pivot to n1
        assert(n1->N > 0);
        n1_keys[n1->N -1] = KEYS(node)[child_index];
        n1_children[n1->N] = n2_children[0];

        // move 'need -1' elements from n2 to n1
        size_t idx = n1->N;
        for(size_t i = 0; i < need -1; i++){
            n1_keys[idx] = n2_keys[i];
            n1_children[idx +1] = n2_children[i +1];
        }

        // update the pivot
        KEYS(node)[child_index] = n2_keys[need -1];

        // left shift elements by `need' in n2
        for(size_t i = 0, sz = n2->N -need -1; i < sz; i++){
            n2_keys[i] = n2_keys[i+need];
            n2_children[i] = n2_children[i+need];
        }
        n2_children[n2->N -need -1] = n2_children[n2->N -1];

        // adjust the sizes
        n1->N += need;
        n2->N -= need;
    }

}

template<typename K, typename V>
void ABTree<K, V>::rebalance_lb(InternalNode* node, size_t child_index, int child_depth){
    assert(node != nullptr);
    assert(node->N > 1 || node == m_root);
    assert(child_index < node->N);

    // the child already contains more than a elements => nop
    size_t child_sz = CHILDREN(node)[child_index]->N;
    const size_t lowerbound = get_lowerbound(child_depth);
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

template<typename K, typename V>
bool ABTree<K, V>::reduce_tree(){
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

template<typename K, typename V>
void ABTree<K, V>::remove_subtrees_rec0(Node* node, int depth){
    if(node == nullptr) return;

    if(!is_leaf(depth)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);
        for(size_t i = 0; i < inode->N; i++){
            remove_subtrees_rec0(children[i], depth +1);
            delete_node(children[i], depth +1);
            children[i] = nullptr;
        }
    } else {
        m_cardinality -= node->N;
    }

    node->N = 0;
}

template<typename K, typename V>
void ABTree<K, V>::remove_subtrees(InternalNode* node, size_t index, size_t length, int children_depth){
    assert(node != nullptr);
    assert(index + length <= node->N);

    K* keys = KEYS(node);
    Node** children = CHILDREN(node);

    for(size_t i = index, last = index + length; i < last; i++){
        remove_subtrees_rec0(children[i], children_depth);
        delete_node(children[i], children_depth);
        children[i] = nullptr;
    }

    // if the length == node->N, then we are removing all elements
    assert(length < node->N || (index == 0 && node->N == length));
    if(length < node->N){
        // shift the pointers
        for(size_t i = index, last = node->N - length; i < last; i++){
            children[i] = children[i + length];
        }
        // shift the keys
        for(size_t i = (index > 0) ? index -1 : 0, last = node->N -1 - length; i < last; i++){
            keys[i] = keys[i + length];
        }
    }

    node->N -= length;
}

template<typename K, typename V>
bool ABTree<K, V>::remove_keys(Node* node, const K& range_min, const K& range_max, int depth, bool* is_min_set, K* min){
    if(!is_leaf(depth)){
        bool retrebalance = false;
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t start = 0, N = inode->N;
        while(start < N -1 && KEYS(inode)[start] < range_min) start++;
        size_t end = start;
        while(end < N -1 && KEYS(inode)[end] <= range_max) end++;

        size_t remove_trees_start = start +1;
        size_t remove_trees_length = (end > start +1) ? end - start -1 : 0;

        // remove the keys at the head
        retrebalance |= remove_keys(CHILDREN(inode)[start], range_min, range_max, depth +1, is_min_set, min);
        if(CHILDREN(inode)[start]->empty()){
            remove_trees_start--;
            remove_trees_length++;
        }

        // remove the keys at the tail
        if(end > start){
            bool is_tmp_set = false; K tmp;
            retrebalance |= remove_keys(CHILDREN(inode)[end], range_min, range_max, depth +1, &is_tmp_set, &tmp);
            if(!is_tmp_set){ // empty block
                assert(CHILDREN(inode)[end]->empty());
                remove_trees_length++;
            } else {
                KEYS(inode)[end -1] = tmp;
            }
        }

        // remove whole trees
        if(remove_trees_length > 0){
            // before shifting the key containing the minimum for the next available block,
            // record into the variable *min
            if(min && start == 0){
                if(remove_trees_length < inode->N){
                    *is_min_set = true;
                    *min = KEYS(inode)[remove_trees_length -1];
                } else {
                    *is_min_set = false;
                }
            }

            remove_subtrees(inode, remove_trees_start, remove_trees_length, depth +1);
        }

        // when we remove multiple children (i.e. start < end), we can have a situation like this:
        // inode->children: U | U | start = PR | FR | FR | FR | end = PR | U | U
        // where U = untouched, PR: subtree partially removed, FR: subtree wholly removed
        // then after remove_subtrees(...) is invoked, all FR branches are deleted from the array and the two PR
        // subtrees are next to each other. The next invocation ensures that only one of the two PRs has less than |a|
        // elements, otherwise they are merged together.
        if(start < end && start+1 < inode->N && CHILDREN(inode)[start]->N + CHILDREN(inode)[start+1]->N <= 2* get_lowerbound(depth +1) -1){
            merge(inode, start, depth+1);
        }

        return retrebalance || inode->N < m_intnode_a;
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->N == 0) { // mmh
            if(min) *is_min_set = false;
            return true;
        }

        K* keys = KEYS(leaf);
        V* values = VALUES(leaf);

        if((keys[0] <= range_max) && (keys[leaf->N -1] >= range_min)) {
            size_t start = 0;
            while(keys[start] < range_min) start++;
            size_t end = start;
            while(end < leaf->N && keys[end] <= range_max) end++;
            size_t length = end - start;
            size_t last = start + leaf->N - end;
            for(size_t i = start; i < last; i++){
                keys[i] = keys[i + length];
                values[i] = values[i + length];
            }
            leaf->N -= length;
            m_cardinality -= length;
        }

        if(min != nullptr){
            *is_min_set = (leaf->N > 0);
            if(is_min_set) *min = keys[0];
        }

        return leaf->N < m_leaf_a;
    }
}

template<typename K, typename V>
void ABTree<K, V>::rebalance_rec(Node* node, const K& range_min, const K& range_max, int depth){
    // base case
    if(is_leaf(depth)){
        assert(node->N >= m_leaf_a || node == m_root);
        return;
    }

    // rebalance the internal nodes
    InternalNode* inode = reinterpret_cast<InternalNode*>(node);
    K* keys = KEYS(inode);
    Node** children = CHILDREN(inode);
    assert(inode->N > 0);
    size_t i = 0, inode_num_keys = inode->N -1;
    while(i < inode_num_keys && keys[i] < range_min) i++;

    rebalance_lb(inode, i, depth +1); // the first call ensures inode[i] >= |a+1| if possible, otherwise inode[i] >= |a|

    // if this is the root, check whether we need to reduce the tree if it has only one child
    if(node == m_root){
        bool reduced = reduce_tree(); // reduced is the same as checking root != node
        if(reduced){ return rebalance_rec(m_root, range_min, range_max, 0); }
    }

    rebalance_rec(children[i], range_min, range_max, depth +1);

    rebalance_lb(inode, i, depth +1); // the second time, it brings inode[i] from |a-1| to at least |a|

    // if this is the root, check whether we need to reduce the tree if it has only one child
    if(node == m_root && reduce_tree()){ return rebalance_rec(m_root, range_min, range_max, 0); }

    if(inode->N > 1 && i < inode->N -2 && keys[i] < range_max){
        rebalance_lb(inode, i +1, depth +1);
        if(node == m_root && reduce_tree()){ return rebalance_rec(m_root, range_min, range_max, 0); }
        rebalance_rec(children[i+1], range_min, range_max, depth +1);
        rebalance_lb(inode, i +1, depth +1); // ensure inode[i+1] >= |a|
        if(node == m_root && reduce_tree()){ return rebalance_rec(m_root, range_min, range_max, 0); }
    }
}

template<typename K, typename V>
void ABTree<K, V>::remove(Node* node, const K& keymin, const K& keymax, int depth){
    // first pass, remove the elements
    bool rebalance = remove_keys(node, keymin, keymax, depth, nullptr, nullptr);

    if(rebalance){
        // quite an edge case, everything was removed from the tree, also the starting leaf, due to the subtree removal
        if(node == m_root && node->N == 0 && m_height > 1){
            delete_node(m_root, 0);
            m_height =1;
            m_root = create_leaf();
        } else {
            // standard case
            rebalance_rec(node, keymin, keymax, 0);
        }
    }
}

template<typename K, typename V>
void ABTree<K, V>::remove(const K& min, const K& max){
    remove(m_root, min, max, 0);
}

template<typename K, typename V>
bool ABTree<K, V>::remove(Node* node, const K& key, int depth, V* out_removed_value, K* omin){
    assert(node != nullptr);

    while(depth < m_height -1){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = node->N;
        assert(N > 0);
        while(i < N -1 && KEYS(inode)[i] < key) i++;
        if(omin == nullptr && i < N -1 && KEYS(inode)[i] == key){
            // in case omin != nullptr, then i = 0 and we are already following the min from another internal node. This
            // tree has many duplicates. The resulting omin has to be inode->keys[i] = inode->keys[0] and all keys from
            // block 0 are equal to inode->keys[0]. Nevertheless we keep traversing the tree to remove the key from the leaf.
            K newkey;
            bool removed = remove(CHILDREN(inode)[i+1], key, depth +1, out_removed_value, &newkey);
            KEYS(inode)[i] = newkey;
            rebalance_lb(inode, i+1, depth+1);
            return removed; // stop the tail recursion
        } else if (CHILDREN(inode)[i]->N <= get_lowerbound(depth+1)){
            bool removed = remove(CHILDREN(inode)[i], key, depth+1, out_removed_value, omin); // it might bring inode->pointers[i]->N == |a-1|
            rebalance_lb(inode, i, depth +1);
            return removed; // stop the tail recursion
        } else { // the node has already |a+1| children, no need to rebalance
            node = CHILDREN(inode)[i];
        }

        depth++;
    }

    { // base case, this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        size_t N = leaf->N;
        K* keys = KEYS(leaf);
        V* values = VALUES(leaf);
        bool removed = false;

        if(N > 0){
            if(keys[N-1] == key){
                removed = true;
                if(out_removed_value != nullptr) *out_removed_value = values[N-1];
                leaf->N -= 1;
            } else if(keys[N-1] > key){
                size_t i = 0;
                while(i < N && keys[i] < key) i++;
                if(i < N && keys[i] == key){
                    removed = true;
                    if(out_removed_value != nullptr) *out_removed_value = values[i];
                    for(size_t j = i; j < leaf->N -1; j++){
                        keys[j] = keys[j+1];
                        values[j] = values[j+1];
                    }
                    leaf->N -= 1;
                }
            }
        }

        if(omin && leaf->N > 0){
            *omin = keys[0];
        }

        return removed;
    }
}

template<typename K, typename V>
bool ABTree<K, V>::remove(const K& key, V* out_value){
    bool removed = remove(m_root, key, 0, out_value, nullptr);
    if(removed){
        m_cardinality--;

        // shorten the tree when the root contains only one child
        reduce_tree();
    }

    return removed;
}

template<typename K, typename V>
bool ABTree<K, V>::find(const K& key, V* out_value) const noexcept {
    Node* node = m_root; // start from the root
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N -1;
        assert(N > 0 && N <= m_intnode_b);
        K* __restrict keys = KEYS(inode);
        while(i < N && keys[i] <= key) i++;
        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    size_t i = 0, N = leaf->N;
    K* __restrict keys = KEYS(leaf);
    while(i < N && keys[i] < key) i++;
    bool match = (i < N && keys[i] == key);
    if(match && out_value != nullptr) *out_value = VALUES(leaf)[i];
    return match;
}

template<typename K, typename V>
template<typename Callback>
void ABTree<K, V>::scan(const K& min, const K& max, Callback user_callback) const {
    if(min > max || size() == 0){ return; } // edge case

    // Find the first leaf for the key `min'
    Node* node = m_root;
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N;
        assert(N > 0);
        K* __restrict keys = KEYS(inode);
        while(i < N -1 && keys[i] < min) i++;
        node = CHILDREN(inode)[i];
    }

    // Find the first entry in the current leaf such that key >= min
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0 && "Empty leaf");

    // edge case, the interval starts at the sibling leaf
    if(KEYS(leaf)[leaf->N -1] < min){
        leaf = leaf->next;
        assert((leaf == nullptr || leaf->N > 0) && "Empty leaf");
        if(leaf == nullptr || KEYS(leaf)[0] < min){
            return;
        }
    }

    // edge case, the interval should have started before this leaf
    if (KEYS(leaf)[0] > max){ return; }

    // Find the last leaf for the key `max'
    node = m_root;
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        assert(inode->N  > 0);
        int64_t i = inode->N -1;
        K* __restrict keys = KEYS(inode);
        while(i > 0 && keys[i -1] > max) i--;
        node = CHILDREN(inode)[i];
    }
    Leaf* leaf_max = reinterpret_cast<Leaf*>(node);
    assert(leaf_max->N > 0 && "Empty leaf");
    if(KEYS(leaf_max)[leaf_max->N -1] <= max && leaf_max->next != nullptr){
        leaf_max = leaf_max->next;
    }

    // standard case, find the first key that satisfies the interval
    K* __restrict keys = KEYS(leaf);
    V* __restrict values = VALUES(leaf);
    int64_t i = 0;
    while(/*i < leaf->N && */ keys[i] < min) i++;

    int64_t N = leaf->N;

    while(leaf != leaf_max){
        while(i < N){
            assert(keys[i] <= max && "Key outside the search range [min, max]");

            // invoke the callback
            if( ! user_callback( keys[i], values[i]) ) return;

            i++;
        }

        // move to the next leaf
        assert(leaf->next != nullptr && "The last leaf should leaf_max");
        leaf = leaf->next;
        keys = KEYS(leaf);
        values = VALUES(leaf);
        i = 0;
        N = leaf->N;

        // prefetch the next next leaf :!)
        _ABTREE_PREFETCH(leaf->next);
        // prefetch the first two blocks for the keys
        _ABTREE_PREFETCH(KEYS(leaf->next));
        _ABTREE_PREFETCH(KEYS(leaf->next) + 8);
        // prefetch the first two blocks for the values
        _ABTREE_PREFETCH(VALUES(leaf->next));
        _ABTREE_PREFETCH(VALUES(leaf->next) + 8);
    }

    // last leaf
    assert(leaf == leaf_max);
    while(i < N && keys[i] <= max){
        // invoke the callback
        if( ! user_callback( keys[i], values[i]) ) return;

        i++;
    }
}

template<typename K, typename V>
ABTree<K, V>::Iterator::Iterator(const ABTree<K, V>* tree, const K& max, Leaf* block, int64_t pos):
    m_tree(tree), m_max(max), m_block(block), m_pos(pos) { }

template<typename K, typename V>
bool ABTree<K, V>::Iterator::has_next() const { return m_block != nullptr; }

template<typename K, typename V>
bool ABTree<K, V>::Iterator::next(K* out_key, V* out_value) {
    if(!has_next()) return false;

    // fetch the key and value for the current position
    if(out_key != nullptr)
        *out_key = m_tree->KEYS(m_block)[m_pos];
    if(out_value != nullptr)
        *out_value = m_tree->VALUES(m_block)[m_pos];


    // move to the next position
    if(m_pos >= m_block->N - 1){
        m_block = m_block->next;
        m_pos = 0;
    } else {
        m_pos++;
    }

    // is the next item satisfy the interval [min, max]?
    if(m_block != nullptr && m_tree->KEYS(m_block)[m_pos] > m_max){
        m_block = nullptr;
    }

    return true;
}

template<typename K, typename V>
std::unique_ptr<typename ABTree<K, V>::Iterator> ABTree<K, V>::create_iterator(const K& max, Leaf* leaf, int64_t pos) const {
    using iterator_t = typename ABTree<K, V>::Iterator;

    if(leaf == nullptr || KEYS(leaf)[pos] > max){
        return std::unique_ptr<iterator_t>(new iterator_t(this, max, nullptr, 0));
    } else {
        return std::unique_ptr<iterator_t>(new iterator_t(this, max, leaf, pos));
    }
}

template<typename K, typename V>
std::unique_ptr<typename ABTree<K, V>::Iterator> ABTree<K, V>::leaf_scan(Leaf* leaf, const K& min, const K& max) const {
    assert(leaf != nullptr);

    if(leaf->N == 0){ return create_iterator(max, nullptr, 0); }

    // edge case, the interval starts at the sibling leaf
    if(KEYS(leaf)[leaf->N -1] < min){
        leaf = leaf->next;
        if(leaf == nullptr) {
            return create_iterator(max, nullptr, 0);
        } else if (KEYS(leaf)[0] >= min){
            return create_iterator(max, leaf, 0);
        } else {
            return create_iterator(max, nullptr, 0);
        }

        // edge case, the interval should have started before this leaf
    } else if (KEYS(leaf)[0] > max){
        return create_iterator(max, nullptr, 0);

        // standard case, find the first key that satisfies the interval
    } else {
        size_t i = 0;
        while(i < leaf->N && KEYS(leaf)[i] < min) i++;
        return create_iterator(max, leaf, i);
    }
}

template<typename K, typename V>
std::unique_ptr<typename ABTree<K, V>::Iterator> ABTree<K, V>::iterator(const K& min, const K& max) const{
    if(min > max) return create_iterator(max, nullptr, 0); // imp

    Node* node = m_root;

    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N;
        assert(N > 0);
        K* __restrict keys = KEYS(inode);
        while(i < N -1 && keys[i] < min) i++;
        node = CHILDREN(inode)[i];
    }

    return leaf_scan(reinterpret_cast<Leaf*>(node), min, max);
}

template<typename K, typename V>
void ABTree<K, V>::dump_data(std::ostream& out, Node* node, int depth) const {
    using namespace std;
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
            for(size_t i = 0; i < inode->N -1; i++){
                if(i > 0) out << ", ";
                out << i << ": " << KEYS(inode)[i];
            }
        }
        out << '\n';
        dump_tabs();
        out << "Ptrs: " << hex;
        for(size_t i = 0; i < inode->N; i++){
            if(i > 0) out << ", ";
            out << i << ": " << CHILDREN(inode)[i];
        }
        out << dec << endl;
        out.setf(flags);

        // recurse
        for(int i = 0, sz = inode->N; i < sz; i++){
            dump_data(out, CHILDREN(inode)[i], depth+1);
        }
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->empty()) return;

        auto flags = out.flags();

        dump_tabs();
        for(size_t i = 0; i < leaf->N; i++){
            if(i > 0) out << ", ";
            out << "<" << KEYS(leaf)[i] << ", " << VALUES(leaf)[i] << ">";
        }
        out << "\n";

        dump_tabs();
        out << "Prev: " <<  leaf->previous << ", Next: " << leaf->next;

        out << endl;

        out.setf(flags);
    }
}

template<typename K, typename V>
void ABTree<K, V>::dump(std::ostream& out) const {
    out << "B-Tree, capacity inodes : [" << m_intnode_a << ", " << m_intnode_b << "], capacity leaves: [" << m_leaf_a << ", " << m_leaf_b << "]"
         ", memory usage: " << memory_footprint() << " bytes, size: " << size() << std::endl;
    dump_data(out, m_root, 0);
}

#undef _ABTREE_PREFETCH

} // namespace

