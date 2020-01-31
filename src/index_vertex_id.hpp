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
#include <utility>

#include "context.hpp"
#include "latch.hpp"

namespace teseo::internal {

class IndexVertexID {
    // forward declarations;
    class Node;
    class N4;
    class N16;
    class N48;
    class N256;

    /**
     * An encoded key in the tree
     */
    struct Key {
        static constexpr int MAX_LENGTH = 8; // all vertex IDs are 8 bytes
        uint8_t m_data[MAX_LENGTH];

    public:
        // Create a new encoded key, stored in the tree
        Key(uint64_t vertex_id);

        // Access the single bytes of the encoded key
        uint8_t &operator[](uint32_t i);
        const uint8_t &operator[](uint32_t i) const;

        // The length of the key
        uint32_t length() const;
    };


    /**
     * The type of inner node in the tree
     */
    enum class NodeType : uint8_t {
        N4 = 0,
        N16 = 1,
        N48 = 2,
        N256 = 3
    };

    /**
     * A single entry associated to a key
     */
    struct NodeEntry {
        Node* m_child { nullptr };
        int64_t m_vertex_count { 0 };
        UndoEntry* m_vertex_undo { nullptr };
    };

    // A generic node in the
    class Node {
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

    public:
        static constexpr int MAX_PREFIX_LEN = 8;

    protected:
        NodeType m_type; // the type of the node
        uint8_t m_children_count; // number of children in the node
        uint8_t m_prefix_count; // number of bytes in the prefix
        uint8_t m_prefix[MAX_PREFIX_LEN]; // prefix shared by all keys

        Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length);

        void set_type(NodeType type);

    public:
        // Destructor
        virtual ~Node();

        NodeType get_type() const;

        // Number of children in the node
        int num_children() const;

        // Read the whole prefix
        uint8_t* get_prefix() const;

        // Read the length of the prefix
        int get_prefix_length() const;

        // Set the prefix
        void set_prefix(const uint8_t* prefix, uint32_t length);

        // Check whether the node contains a prefix
        bool has_prefix() const;

        //
        bool prefix_match(const Key& key, int prefix_start, int* out_prefix_end, uint8_t** out_non_matching_prefix, int* out_non_matching_length);

        // Get the corresponding node for the given byte in the trie, or null if no node has been associated
        virtual NodeEntry* get_child(uint8_t byte) const = 0;

        // Update the entry and the count for the given key
        void update(uint8_t byte, Node* child, int64_t count_diff);

        // Check whether the given node is full, that is, no new children can be inserted
        virtual bool is_overfilled() const = 0;

        // Check whether the given node should be shrank to a smaller node type, due to a deletion
        virtual bool is_underfilled() const = 0;

        // Insert the given child in the node
        virtual void insert(uint8_t key, const NodeEntry& child, bool undo_tx) = 0;
        void insert(uint8_t key, const NodeEntry* child, bool undo_tx);
    };


    class N4 : public Node {
    public:
        uint8_t m_keys[4];
        NodeEntry m_children[4];

    public:
        N4(const uint8_t *prefix, uint32_t prefix_length);
        void insert(uint8_t key, const NodeEntry& child, bool undo_tx);
        NodeEntry* get_child(uint8_t byte) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N16* to_N16() const;

    };

    class N16 : public Node {
        uint8_t m_keys[16];
        NodeEntry m_children[16];

        // Flip the sign bit, enables signed SSE comparison of unsigned values
        static uint8_t flip_sign(uint8_t byte);

        // Count trailing zeros, only defined for x>0
        static unsigned ctz(uint16_t x);

    public:
        N16(const uint8_t* prefix, uint32_t prefix_length);
        void insert(uint8_t key, const NodeEntry& child, bool undo_tx);
        NodeEntry* get_child(uint8_t byte) const;
        void update(uint8_t byte, Node* child, int64_t count_diff);
        bool is_overfilled() const;
        bool is_underfilled() const;
        N4* to_N4() const; // create a new node with the same content (due to shrinking)
        N48* to_N48() const; // create a new node with the same content (due to expansion)
    };


    class N48 : public Node {
        uint8_t m_child_index[256];
        NodeEntry m_children[48];

        // flag to keep track if an entry in child_index is empty or not. The value 48 is the number of slots in the node.
        static const uint8_t EMPTY_MARKER = 48;

    public:
        N48(const uint8_t* prefix, uint32_t prefix_length);
        void insert(uint8_t key, const NodeEntry& child, bool undo_tx);
        NodeEntry* get_child(uint8_t byte) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N16* to_N16() const; // create a new node with the same content (due to shrinking)
        N256* to_N256() const; // create a new node with the same content (due to expansion)
    };

    class N256 : public Node {
        NodeEntry m_children[256];

    public:
        N256(const uint8_t* prefix, uint32_t prefix_length);
        void insert(uint8_t key, const NodeEntry& child, bool undo_tx);
        NodeEntry* get_child(uint8_t byte) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N48* to_N48() const; // create a new node with the same content (due to shrinking)
    };


    Node* m_root;
    OptimisticLatch<0> m_latch;

    // Insert the node `child' in the node `node_current' under the key `key_current'. The "node_current" may need
    // to be expanded (or split) if there is not enough space to insert a new child, in this case we also need
    // the parent node to replace the node_current with the new expanded node
    void insert_and_grow(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, const NodeEntry& child);

    // check whether the current ptr to node is actually a leaf
    static bool is_leaf(Node* node);

    // Assuming that the given node is a leaf, get the underlying payload
    static void* get_leaf_address(Node* leaf);

    // Retrieve the vertex_id associated to the given leaf
    static uint64_t get_leaf_vertex_id(Node* leaf);

    // Mark the given node for the garbage collector
    static void mark_node_for_gc(Node* node);

    // Create a new leaf for the given value
    static Node* create_leaf(uint64_t vertex_id, void* value);

    static void update_entry(NodeEntry& old_entry, const NodeEntry& new_entry, bool log_txn);

    // Recursive delete all nodes and their children, freeing the memory associated
    static void delete_nodes_rec(Node* node);

public:

    // Destructor
    ~IndexVertexID();

    // Insert a new element in the index with the given
    // @param vertex_id the new key to insert
    // @param count the number of items associated to the given vertex_id
    // @param value the pointer to the leaf in the underlying B-Tree
    void insert(uint64_t vertex_id, int64_t count, void* value);

    void update_key(uint64_t vertex_id_old, uint64_t vertex_id_new, int64_t count_diff);

    void update_count(uint64_t vertex_id, int64_t count_diff);

    bool remove(uint64_t vertex_id);

    void* get_value_by_logical_id(uint64_t logical_id) const;

    void* get_value_by_real_id(uint64_t vertex_id) const;

    /**
     * Get the total count stored in the tree
     */
    uint64_t get_total_count() const;

    /**
     * Dump the whole content of the tree to the stdout, for debugging purposes
     * This method is not thread-safe.
     */
    void dump() const;
};


std::ostream& operator<<(std::ostream& out, const IndexVertexID::Key& key);

} // namespace

