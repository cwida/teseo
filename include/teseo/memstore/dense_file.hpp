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
#include <vector>

#include "data_item.hpp"

namespace teseo::rebalance { class ScratchPad; } // forward declaration

namespace teseo::memstore {

// forward declarations
class RemoveVertex;
class Segment;

/**
 * An sorted file consisting of an unsorted dense area properly indexed
 */
class DenseFile {
    DenseFile(const DenseFile&) = delete;
    DenseFile& operator=(const DenseFile&) = delete;
    friend class Segment;

    // forward declarations;
    class Node;
    class N4;
    class N16;
    class N48;
    class N256;
    class NodeList;

    /**
     * An encoded key in the index
     */
    struct Key {
        static constexpr int MAX_LENGTH = 16; // all
        uint8_t m_data[MAX_LENGTH];

    public:
        // Uninitialised key
        Key();

        // Create a new encoded key, stored in the tree
        Key(uint64_t vertex_id);

        Key(uint64_t src, uint64_t dst);

        // Access the single bytes of the encoded key
        uint8_t &operator[](uint32_t i);
        const uint8_t &operator[](uint32_t i) const;

        // The length of the key
        int length() const;

        // The actual encoded data
        uint8_t* data();
        const uint8_t* data() const;

        // Retrieve the source of the edge
        uint64_t get_source() const;

        // Retrieve the destination of the edge
        uint64_t get_destination() const;

        // Equality operators
        bool operator==(const Key& key) const;
        bool operator!=(const Key& key) const;
        bool operator<=(const Key& key) const;
    };

    friend std::ostream& operator<<(std::ostream& out, const DenseFile::Key& key);

    /**
     * The type of inner node in the tree
     */
    enum class NodeType : uint8_t {
        N4 = 0,
        N16 = 1,
        N48 = 2,
        N256 = 3
    };
    friend std::ostream& operator<<(std::ostream& out, const DenseFile::NodeType& n4);

    /**
     * A leaf of the trie
     */
    struct Leaf { uint64_t m_value; };

    /**
     * A list of nodes
     */
    class NodeList {
    public:
        uint64_t m_size = 0;
        Node* m_nodes[256];
    };

    // A generic node in the
    class Node {
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

    public:
        static constexpr int MAX_PREFIX_LEN = 13;

    protected:
        NodeType m_type; // the type of the node
        uint8_t m_count; // number of children in the node
        uint8_t m_prefix_sz; // number of bytes in the prefix, up to 16
        uint8_t m_prefix[MAX_PREFIX_LEN]; // prefix shared by all keys

        Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length);

        void set_type(NodeType type);

    public:
        // The type of node, in {N4, N16, N48, N256}
        NodeType get_type() const;

        // Number of children in the node
        int count() const;

        // Read the whole prefix
        uint8_t* get_prefix();
        const uint8_t* get_prefix() const;

        // Read the length of the prefix
        int get_prefix_length() const;

        // Set the prefix
        void set_prefix(const uint8_t* prefix, uint32_t length);

        // Check whether the node contains a prefix
        bool has_prefix() const;

        // Check whether the prefix of the current node is equal to the data of the key, starting from offset prefix_start
        bool prefix_match_exact(DenseFile* df, const Key& key, int prefix_start, int* out_prefix_end, uint8_t** out_non_matching_prefix, int* out_non_matching_length) const;

        // -1 => no match, 0 => maybe, 1 => match
        int prefix_match_approximate(const Key& key, int prefix_start, int* out_prefix_end) const;

        // -1 if the current prefix is less than the key, 0 if equals, +1 if the current prefix is larger than the key
        template<bool is_optimistic>
        int prefix_compare(Context& context, const DenseFile* df, const Key& key, int& /* in/out */ key_level) const;

        // Prepend to the current prefix the prefix of the node first_part and the byte from second_part
        void prefix_prepend(Node* first_part, uint8_t second_part);

        // Get the corresponding node for the given byte in the trie, or null if no node has been associated
        Node* get_child(uint8_t key) const;

        // Get any descendant leaf (to compare the prefix)
        Leaf get_any_descendant_leaf() const;
        template<bool is_optimistic>
        Leaf get_any_descendant_leaf(Context& context) const;

        // Update the node pointed by the given key
        bool change(uint8_t key, Node* value);

        // Check whether the given node is full, that is, no new children can be inserted
        bool is_overfilled() const;

        // Check whether the given node should be shrank to a smaller node type, due to a deletion
        bool is_underfilled() const;

        // Insert the given child in the node
        void insert(uint8_t key, Node* child);

        // Remove the given key from the node, return true if the key has been actually removed, false otherwise
        bool remove(uint8_t key);

        // Get the node with the highest key among the children
        static Leaf get_max_leaf(Node* node);

        // Retrieve the child whose key is the maximum among the keys less or equal than the given key
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;

        // Retrieve the predecessor of the given key
        Node* get_predecessor(uint8_t key) const;

        // Retrieve the sorted list of children of this node
        NodeList children() const;

        // Retrieve all children of this node that are greater than the given key
        NodeList children_gt(uint8_t key) const;

        // Dump the content of the node to given output stream
        static void dump(std::ostream& out, const DenseFile* df, Node* node, int key_level, int depth);
    };


    class N4 : public Node {
    public:
        uint8_t m_keys[4];
        Node* m_children[4];

    public:
        N4(const uint8_t *prefix, uint32_t prefix_length);
        void insert(uint8_t key, Node* value);
        bool remove(uint8_t byte);
        Node** get_child_ptr(uint8_t byte);
        Node* get_max_child() const;
        std::tuple</* key */ uint8_t, /* entry */ Node*> get_other_child(uint8_t key) const;
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* get_any_child() const;
        NodeList children() const;
        NodeList children_gt(uint8_t key) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N16* to_N16() const;
    };

    class N16 : public Node {
        uint8_t m_keys[16];
        Node* m_children[16];

        // Flip the sign bit, enables signed SSE comparison of unsigned values
        static uint8_t flip_sign(uint8_t byte);

        // Count trailing zeros, only defined for x>0
        static unsigned ctz(uint16_t x);

    public:
        N16(const uint8_t* prefix, uint32_t prefix_length);
        void insert(uint8_t key, Node* entry);
        bool remove(uint8_t key);
        Node** get_child_ptr(uint8_t byte);
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* get_max_child() const;
        Node* get_any_child() const;
        NodeList children() const;
        NodeList children_gt(uint8_t key) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N4* to_N4() const; // create a new node with the same content (due to shrinking)
        N48* to_N48() const; // create a new node with the same content (due to expansion)
    };


    class N48 : public Node {
        uint8_t m_child_index[256];
        Node* m_children[48];

        // flag to keep track if an entry in child_index is empty or not. The value 48 is the number of slots in the node.
        static const uint8_t EMPTY_MARKER = 48;

    public:
        N48(const uint8_t* prefix, uint32_t prefix_length);
        void insert(uint8_t key, Node* entry);
        bool remove(uint8_t key);
        Node** get_child_ptr(uint8_t byte);
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* get_max_child() const;
        Node* get_any_child() const;
        NodeList children() const;
        NodeList children_gt(uint8_t key) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N16* to_N16() const; // create a new node with the same content (due to shrinking)
        N256* to_N256() const; // create a new node with the same content (due to expansion)
    };

    class N256 : public Node {
        Node* m_children[256];

    public:
        N256(const uint8_t* prefix, uint32_t prefix_length);
        int count() const;
        void insert(uint8_t key, Node* entry);
        bool remove(uint8_t key);
        Node** get_child_ptr(uint8_t byte);
        std::pair<Node*, /* exact match ? */ bool> find_node_leq(uint8_t key) const;
        Node* get_max_child() const;
        Node* get_any_child() const;
        NodeList children() const;
        NodeList children_gt(uint8_t key) const;
        bool is_overfilled() const;
        bool is_underfilled() const;
        N48* to_N48() const; // create a new node with the same content (due to shrinking)
    };

    /**
     * The actual file, containing the elements
     */
    class File {
        File(const File&) = delete;
        File& operator=(const File&) = delete;

        DataItem* m_elements;
        uint32_t m_capacity;
        uint32_t m_size;

    public:
        /**
         * Constructor
         */
        File();

        /**
         * Move constructor
         */
        File(File&& file);

        /**
         * Destructor
         */
        ~File();

        /**
         * Retrieve the element at the given position
         */
        DataItem* operator[](uint64_t index);
        const DataItem* operator[](uint64_t index) const;

        /**
         * Append a new (blank) element in the file. Return it's index.
         */
        DataItem* append();

        /**
         * Retrieve the number of elements stored in the file
         */
        uint64_t cardinality() const;

        /**
         * Retrieve the position in the file of the given data item
         */
        uint64_t position(const DataItem* di) const;

        /**
         * Sort the file in place. Of course, this is going to invalidate the index of the file from now on.
         */
        void sort_in_place();

        /**
         * Deallocate the file in the GC
         */
        void clear();

        /**
         * Dump the content of the list, for debugging purposes
         */
        void dump() const;
    };

    /**
     * A list of vertices that are locked by other transactions, possibly in some other segment, and their edges cannot be modified in the meanwhile
     */
    class TransactionLocks {
        TransactionLocks(const TransactionLocks&) = delete;
        TransactionLocks& operator=(const TransactionLocks&) = delete;

        uint32_t m_capacity;
        uint32_t m_size;
        constexpr static uint64_t m_fixcap_storage_sz = 4;
        uint64_t m_fixcap_storage[m_fixcap_storage_sz];
        uint64_t* m_varcap_storage;

        // Increase the capacity of the underlying buffer
        void resize();

        // Retrieve the internal list of locked vertices
        uint64_t* list();
    public:
        /**
         * Constructor
         */
        TransactionLocks();

        /**
         * Move constructor
         */
        TransactionLocks(TransactionLocks&& other);

        /**
         * Destructor
         */
        ~TransactionLocks();

        /**
         * Lock the given vertex
         * @return true if the vertex has been added to the list, false if it was already present
         */
        bool lock(uint64_t vertex_id);

        /**
         * Check if the given vertex is locked, that is, it's present in the list
         */
        bool is_locked(uint64_t vertex_id) const;

        /**
         * Check if the given vertex is locked, that is, it's present in the list
         * Assume an optimistic lock has been acquired
         */
        bool is_locked_optimistic(Context& context, uint64_t vertex_id) const;

        /**
         * Unlock the given vertex
         * @return true if the vertex has been removed from the list, false if it was not already present
         */
        bool unlock(uint64_t vertex_id);

        /**
         * Retrieve the current list of locked vertices
         */
        const uint64_t* list() const;

        /**
         * Retrieve the number of locked vertices
         */
        uint64_t cardinality() const;

        /**
         * Deallocate the storage to the GC
         */
        void clear();

        /**
         * Retrieve a string representation of the list, for debugging purposes
         */
        std::string to_string() const;

        /**
         * Dump the content of the list, for debugging purposes
         */
        void dump() const;
    };


    Node* m_root; // the root of the trie
    uint64_t m_cardinality; // number of entries stored
    File m_file;
    TransactionLocks m_transaction_locks;

    // Index all entries in the file
    void initialise_index_from_file();

    //  If it doesn't already exist, insert a pair <source, destination> in the file. Return a pointer to either
    //  the existing data item or the newly created one.
    DataItem* index_update(Context& context, const Update& update);

    // Insert the pair <key, data_item_position> in the index
    void index_insert(const memstore::Key& mkey, uint64_t data_item_position);

    // Insert or fetch (if it already exists) an entry in the trie
    uint64_t do_insert(Context* context, const Update* update, const Key& key, Node* node_parent, uint8_t byte_parent, Node* node_current, int key_level_start, Leaf value);

    // Insert the `new_element' in the node `node_current' under the key `key_current'. The "node_current" may need
    // to be expanded (or split) if there is not enough space to insert a new child, in this case we also need
    // the parent node to replace the node_current with the new expanded node
    void do_insert_and_grow(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, Leaf value);

    // Retrieve the item with the given key from the index
    DataItem* index_fetch(const Key& key);

    // Retrieve the data item with the given key from the index. Assume an optimistic latch has been acquired
    const DataItem* index_fetch_optimistic(Context& context, const Key& key) const;

    // Retrieve the max value stored among the descendants of the given node
    Leaf get_max_leaf(Node* node) const;

    // Retrieve the associated key from the leaf
    Key leaf2key(Leaf leaf) const;

    // Retrieve the data item associated to the leaf
    DataItem* leaf2di(Leaf leaf);
    const DataItem* leaf2di(Leaf leaf) const;

    // Retrieve the position in the file stored in the leaf
    static uint64_t leaf2filepos(Leaf leaf);

    // Convert the leaf into a node ptr
    static Node* leaf2node(Leaf leaf);

    // check whether the current ptr to node is actually a leaf
    static bool is_leaf(const Node* node);

    // retrieve the leaf content of the given node
    static Leaf node2leaf(const Node* node);

    // Mark the given node for the garbage collector
    static void mark_node_for_gc(Node* node);

    // Remove (delete) the given node
    static void delete_node(void* node);

    // Recursive delete all nodes and their children, freeing the memory associated
    static void delete_nodes_rec(Node* node, bool use_gc);

    // Dump the content of the index, for debugging purposes
    void dump_index(std::ostream&) const;

    // Scan the data items in the file in sorted order, starting from key
    template<typename Callback>
    void scan(Context& context, const Key& key, Callback cb) const;

    // Recursive procedure to scan the nodes at different levels of the trie
    template<bool is_optimistic, typename Callback>
    bool do_scan_node(Context& context, const Key& key, Node* node, int level, Callback cb) const;

    // Scan all items in the node
    template<bool is_optimistic, typename Callback>
    bool do_scan_everything(Context& context, Node* node, Callback cb) const;

    // Scan a leaf
    template<bool is_optimistic, typename Callback>
    bool do_scan_leaf(Context& context, Leaf leaf, Callback cb) const;

    // Check whether there exists any edge in the current segment, with the given vertex as source, being visible by the current transaction
    bool is_source_visible(Context& context, uint64_t vertex_id) const;

    // Check whether the given update is compatible with the existing data item
    void ccheck(Context* context, const Update* update, const DataItem* data_item);

    // Segment::to_dense_file() ctor
    DenseFile(File&& file, TransactionLocks&& transaction_locks);

public:
    /**
     * Destructor
     */
    ~DenseFile();

    /**
     * Attempt to perform the given update
     * @param context the memstore context
     * @param update the update to perform
     * @param has_source_edge, if the update involves the insertion of an edge, it checks whether
     *        the source vertex exists already in the file. Otherwise it's ignored.
     * @return the additional amount of space needed, in terms of qwords, if this update had been done in a sparse file
     */
    int64_t update(Context& context, const Update& update, bool has_source_vertex);

    /**
     * Rollback the given update
     * @return the additional amount of space needed, in terms of qwords, if this update had been done in a sparse file
     */
    int64_t rollback(Context& context, const Update& update, transaction::Undo* next);

    /**
     * Remove the vertex and all of its attached outgoing edges
     * @return the additional amount of space needed, in terms of qwords, if this update had been done in a sparse file
     */
    int64_t remove_vertex(RemoveVertex& instance);

    /**
     * Unlock a vertex after it an attempt to remove it
     */
    void unlock_vertex(RemoveVertex& instance);

    /**
     * Load all the elements from the file to the given buffer.
     * NB: this operation effectively invalidates the index of this file, which is file as, if we're invoking this method,
     * we're going to rebalance the segment and destroy this file anyway.
     */
    void load(rebalance::ScratchPad& buffer);

    /**
     * Check whether the given key (vertex, edge) exists in the segment and is visible by the current transaction.
     * Assume an optimistic lock has been taken to the context.m_segment.
     * @param context the current context, with the tree traversal memstore -> leaf -> segment
     * @param key the search key
     * @param is_unlocked if true, the search key must be a vertex and its state must be unlocked, to avoid phantom writes.
     */
    bool has_item_optimistic(Context& context, const memstore::Key& key, bool is_unlocked) const;

    /**
     * Retrieve the weight associated to the given edge
     */
    double get_weight_optimistic(Context& context, const memstore::Key& key) const;

    /**
     * Retrieve the number of edges attached to the given vertex.
     * The method works both for locked and optimistic readers.
     */
    void get_degree(Context& context, memstore::Key& next, bool& vertex_found, uint64_t& partial_result) const;

    /**
     * Retrieve the number of elements in the segment
     */
    uint64_t cardinality() const;

    /**
     * Remove all versions from the sparse file
     */
    void clear_versions();

    /**
     * Mark to the GC all allocations made
     */
    void clear();

    /**
     * Dump the content of the file to stdout, for debugging purposes
     */
    void dump() const;

    /**
     * Dump the content of the file to the given output stream, for debugging purposes
     */
    void dump_and_validate(std::ostream& out, Context& context, bool* integrity_check) const;
};

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
uint64_t DenseFile::cardinality() const {
    return m_cardinality;
}

} // namespace
