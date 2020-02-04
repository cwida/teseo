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
    IndexVertexID(const IndexVertexID&) = delete;
    IndexVertexID& operator=(const IndexVertexID&) = delete;

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

        // Retrieve the original vertex ID
        uint64_t get_vertex_id() const;
    };

    friend std::ostream& operator<<(std::ostream& out, const IndexVertexID::Key& key);


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

    /**
     * A leaf of the trie
     */
    struct Leaf {
        uint64_t m_vertex_id;
        void* m_btree_leaf_address;
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
        uint8_t* get_prefix();
        const uint8_t* get_prefix() const;

        // Read the length of the prefix
        int get_prefix_length() const;

        // Set the prefix
        void set_prefix(const uint8_t* prefix, uint32_t length);

        // Check whether the node contains a prefix
        bool has_prefix() const;

        //
        bool prefix_match(const Key& key, int prefix_start, int* out_prefix_end, uint8_t** out_non_matching_prefix, int* out_non_matching_length);

        // -1 if the current prefix is less than the key, 0 if equals, +1 if the current prefix is larger than the key
        int prefix_compare(const Key& key, int& /* in/out */ key_level) const;

        // Prepend to the current prefix the prefix of the node first_part and the byte from second_part
        void prepend_prefix(Node* first_part, uint8_t second_part);

        // Get the corresponding node for the given byte in the trie, or null if no node has been associated
        virtual NodeEntry* get_child(uint8_t byte) = 0;

        // Update the node pointed by the given node
        void change(uint8_t byte, Node* node, int64_t count_diff = 0);

        // Check whether the given node is full, that is, no new children can be inserted
        virtual bool is_overfilled() const = 0;

        // Check whether the given node should be shrank to a smaller node type, due to a deletion
        virtual bool is_underfilled() const = 0;

        // Insert the given child in the node
        virtual void insert(uint8_t key, const NodeEntry& child) = 0;

        // Remove the given key from the node, return true if the key has been actually removed, false otherwise
        virtual bool remove(uint8_t key, NodeEntry* out_old_entry) = 0;

        // Get the node with the highest key among the children
        virtual Node* max() const = 0;

        // Retrieve the child whose key is the maximum among the keys less or equal than the given key
        virtual std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const = 0;

        Node* get_predecessor(uint8_t key) const;

        // Dump the content of the node to given output stream
        static void dump(std::ostream& out, Node* node, int key_level, int depth);
    };


    class N4 : public Node {
    public:
        uint8_t m_keys[4];
        NodeEntry m_children[4];

    public:
        N4(const uint8_t *prefix, uint32_t prefix_length);
        void insert(uint8_t key, const NodeEntry& entry);
        bool remove(uint8_t byte, NodeEntry* out_old_entry);
        NodeEntry* get_child(uint8_t byte);
        std::tuple</* key */ uint8_t, /* entry */ NodeEntry*> get_first_child();
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* max() const;
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
        void insert(uint8_t key, const NodeEntry& entry);
        bool remove(uint8_t byte, NodeEntry* out_old_entry);
        NodeEntry* get_child(uint8_t byte);
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* max() const;
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
        void insert(uint8_t key, const NodeEntry& entry);
        bool remove(uint8_t byte, NodeEntry* out_old_entry);
        NodeEntry* get_child(uint8_t byte);
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* max() const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N16* to_N16() const; // create a new node with the same content (due to shrinking)
        N256* to_N256() const; // create a new node with the same content (due to expansion)
    };

    class N256 : public Node {
        NodeEntry m_children[256];

    public:
        N256(const uint8_t* prefix, uint32_t prefix_length);
        void insert(uint8_t key, const NodeEntry& entry);
        bool remove(uint8_t byte, NodeEntry* out_old_entry);
        NodeEntry* get_child(uint8_t byte);
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* max() const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N48* to_N48() const; // create a new node with the same content (due to shrinking)
    };

    Node* m_root; // the root of the trie
    OptimisticLatch<0> m_latch; // latch to protect read/write access

    // Insert the node `child' in the node `node_current' under the key `key_current'. The "node_current" may need
    // to be expanded (or split) if there is not enough space to insert a new child, in this case we also need
    // the parent node to replace the node_current with the new expanded node
    void do_insert_and_grow(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, const NodeEntry& child);

    // Recursive procedure to remove an entry from the trie
    bool do_remove(Node* node_parent, uint8_t byte_parent, Node* node_current, const Key& key, int key_level_start, NodeEntry* out_entry_removed);

    // Remove the child in node_current[key_current], and in case shrink node_current to a smaller node variaty if underfilled. The param
    // out_entry_removed reports the entry removed from node_current[key_current]
    // @return true if the entry with key_current has been removed from node_current, false otherwise
    bool do_remove_and_shrink(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, NodeEntry* out_entry_removed);

    // Create a new leaf for the given value
    static Node* create_leaf(uint64_t vertex_id, void* value);

    // check whether the current ptr to node is actually a leaf
    static bool is_leaf(Node* node);

    // retrieve the leaf content of the given node
    static Leaf* get_leaf(Node* node);

    // Mark the given node for the garbage collector
    static void mark_node_for_gc(Node* node);

    static void update_entry(NodeEntry& old_entry, const NodeEntry& new_entry, bool log_txn);

    // Recursive delete all nodes and their children, freeing the memory associated
    static void delete_nodes_rec(Node* node);

    void do_insert(Node* node_parent, uint8_t byte_parent, Node* node_current, const Key& key, int key_level_start, NodeEntry& new_element);

    static void create_txn_undo(uint64_t vertex_id, NodeEntry& entry, int64_t difference);
    static void create_txn_undo(uint64_t vertex_id, NodeEntry* entry, int64_t difference); // alias

    // Find the first entry that is less or equal than the given key
    void* find_btree_leaf_by_vertex_id_leq(uint64_t latch_version, const Key& key, Node* node, int level) const;

    void* get_max_leaf_address(uint64_t latch_version, Node* node) const;


public:
    // Constructor
    IndexVertexID();

    // Destructor
    ~IndexVertexID();

    // Insert a new element in the index with the given
    // @param vertex_id the new key to insert
    // @param count the number of items associated to the given vertex_id
    // @param value the pointer to the leaf in the underlying B-Tree
    void insert(uint64_t vertex_id, int64_t count, void* btree_leaf_address);

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

