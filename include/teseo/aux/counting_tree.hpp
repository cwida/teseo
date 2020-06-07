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

#include <cinttypes>
#include <ostream>

#include "teseo/aux/item.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/util/latch.hpp"

namespace teseo::gc { class GarbageCollector; } // forward declaration

namespace teseo::aux {

/**
 * A counting B+ tree, where vertices can be retrieved by either their real ID or their rank in the
 * sorted order. Duplicates are not allowed.
 *
 * This class is not thread-safe.
 */
class CountingTree {
    CountingTree(const CountingTree&) = delete;
    CountingTree& operator= (CountingTree&) = delete;

    // Superclass of a node in the tree, either a leaf or an internal node
    struct Node {
        // remove the ctors
        Node() = delete;
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

        uint64_t N; // number of elements in the node
        bool empty() const; // is the node empty?
    };

    // An internal node of the tree, containing the separator keys
    struct InternalNode : public Node {
        // remove the ctors
        InternalNode() = delete;
        InternalNode(const InternalNode&) = delete;
        InternalNode& operator=(const InternalNode&) = delete;
    };

    uint64_t* KEYS(const InternalNode* inode) const;
    uint64_t* RANKS(const InternalNode* inode) const;
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
    ItemUndirected* ELEMENTS(const Leaf* leaf) const;

private:
    constexpr static uint64_t m_inode_B = context::StaticConfiguration::aux_counting_tree_capacity_inodes; // max number of separator keys per internal node
    constexpr static uint64_t m_leaf_B = context::StaticConfiguration::aux_counting_tree_capacity_leaves; // max number of elements per leaf
    Node* m_root = nullptr; // the root node of the B+ tree
    uint64_t m_cardinality; // total number of elements present in the B+ tree
    int m_height; // the height of the B+ tree

    // Create a new node / leaf
    static InternalNode* create_internal_node();
    static Leaf* create_leaf();

    // Get the memory size of an internal node / leaf
    constexpr static uint64_t memsize_internal_node();
    constexpr static uint64_t memsize_leaf();

    // Delete an existing internal node or leaf
    void delete_node(Node* node, int depth) const;
    void close_rec(gc::GarbageCollector* gc, Node* node, int depth);

    // Check whether the nodes at the given height are leaves or internal nodes
    bool is_leaf(int depth) const;

    // Retrieve the cumulative sum of a node
    uint64_t compute_cumulative_sum(Node* node, int depth);

    // Split the child of `node' at index `child' in half and adds the new node as a new child of `node'.
    void split(InternalNode* inode, uint64_t child_index, int child_depth);

    // Increase the height of the tree by 1, by splitting the current root in half and introducing a new root.
    void split_root();

    // Insert the given element in the subtree rooted at the given `node'.
    void do_insert(Node* node, const ItemUndirected& element, int depth);

    // Try to reduce the height of the tree, checking whether the root has only one child.
    bool reduce_tree();

    // Merge two adjacent nodes together
    void merge(InternalNode* node, uint64_t child_index, int child_depth);

    // Move `num_nodes' elements from the node `child_index +1' to its sibling `child_index'
    void rotate_left(InternalNode* node, uint64_t child_index, int child_depth, uint64_t num_nodes);

    // Move `num_nodes' elements from the node `child_index -1' to its sibling `child_index'
    void rotate_right(InternalNode* node, uint64_t child_index, int child_depth, uint64_t num_nodes);

    // Check that the threshold of the node is greater than its lower bound. In case merge or rotate to satisfy the lower bound
    void rebalance(InternalNode* node, uint64_t child_index, int child_depth);

    // Remove a single element from the tree
    bool do_remove(Node* node, uint64_t vertex_id, int depth, uint64_t* omin);

    // Recursively dump the content of the given subtree
    void do_dump(std::ostream&, Node* node, uint64_t cumulative_sum, int depth) const;

public:
    /**
     * Create a new empty tree
     */
    CountingTree();

    /**
     * Move constructor
     */
    CountingTree(CountingTree&& ct);

    /**
     * Destructor
     */
    ~CountingTree();

    /**
     * Insert the given vertex in the tree
     */
    void insert(const ItemUndirected& item);

    /**
     * Remove the vertex from the tree. The identifier must be the real vertex id.
     * @return true if the vertex has been indeed removed, false if not found.
     */
    bool remove(uint64_t vertex_id);

    /**
     * Retrieve the element associated to the given vertex
     * Precondition: the caller accesses the data structure in mutual exclusion
     * @return the first item is a pointer to the stored element in the leaf, the second item is its rank/logical id. The result for the first
     *         item is nullptr if the element has not been found.
     */
    std::pair<ItemUndirected*, uint64_t> get_by_vertex_id(uint64_t vertex_id);
    std::pair<const ItemUndirected*, uint64_t> get_by_vertex_id(uint64_t vertex_id) const;


    /**
     * Retrieve the element associated to the given vertex.
     * @throw Abort if the version becomes outdated while traversing the tree
     * @return true if the element was found, false otherwise.
     */
    bool get_by_vertex_id_optimistic(uint64_t vertex_id, util::OptimisticLatch<0>& latch, uint64_t version, ItemUndirected* output_item, uint64_t* output_rank = nullptr) const;

    /**
     * Retrieve the element associated to the given rank
     * Precondition: the caller accesses the data structure in mutual exclusion
     */
    ItemUndirected* get_by_rank(uint64_t rank);
    const ItemUndirected* get_by_rank(uint64_t rank) const;

    /**
     * Retrieve the element associated to the given rank.
     * @throw Abort if the version becomes outdated while traversing the tree
     * @return true if the element was found, false otherwise.
     */
    bool get_by_rank_optimistic(uint64_t rank, util::OptimisticLatch<0>& latch, uint64_t version, ItemUndirected* output) const;

    /**
     * Retrieve the total number of elements stored in the tree
     */
    uint64_t size() const;

    /**
     * Check whether the tree is empty
     */
    bool empty() const;

    /**
     * Remove all nodes in the tree. No new items can be further added.
     */
    void close(gc::GarbageCollector* gc = nullptr);

    /**
     * Dump the content of the tree to stdout, for debugging purposes
     */
    void dump() const;
};


} // namespace

