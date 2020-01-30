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
     * A single entry associated in a node
     */
    struct NodeEntry {
        Node* m_child;
        uint64_t m_vertex_count;
        UndoEntry* m_vertex_undo;
    };

    // A generic node in the
    class Node {
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

    public:
        static constexpr int MAX_PREFIX_LEN = 6;

    protected:
        OptimisticLatch<2> m_version;
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

        // Read lock
        uint64_t read_lock() const;
        void read_validate(uint64_t version) const;

        // Write lock
        void write_lock();
        void write_lock(uint64_t version);
        void write_unlock();

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
        virtual Node* get_child(uint8_t byte) const = 0;

        // Get the corresponding entry(child/version/undo log) for the given byte
        virtual NodeEntry get_entry(uint8_t byte) const = 0;
    };


    class N4 : public Node {
    public:
        uint8_t m_keys[4];
        Node* m_children[4] = {nullptr, nullptr, nullptr, nullptr};
        uint64_t m_vertex_count[4] = {0, 0, 0, 0};
        UndoEntry* m_vertex_version[4] = {nullptr, nullptr, nullptr, nullptr};

    public:
        N4(const uint8_t *prefix, uint32_t prefix_length);

        void insert(uint8_t key, NodeEntry value);

        // Get the child node with the corresponding byte
        Node* get_child(uint8_t byte) const;

        NodeEntry get_entry(uint8_t byte) const;

        template<class NODE>
        void copyTo(NODE *n) const;

        bool change(uint8_t key, N *val);

        N *getChild(const uint8_t k) const;

        void remove(uint8_t k);

        N *getAnyChild() const;

        N* getMaxChild() const;

        N* getChildLessOrEqual(uint8_t key, bool& out_exact_match) const;

        bool isFull() const;

        bool isUnderfull() const;

        std::tuple<N *, uint8_t> getSecondChild(const uint8_t key) const;

        void deleteChildren();

        uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                         uint32_t &childrenCount) const;
    };


    Node* m_root;


    // check whether the current ptr to node is actually a leaf
    static bool is_leaf(Node* node);

    // Assuming that the given node is a leaf, get the underlying payload
    static void* get_leaf_address(Node* leaf);

    // Create a new leaf for the given value
    static Node* create_leaf(void* value);

public:
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

