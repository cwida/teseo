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

#include "index_vertex_id.hpp"

#include <cassert>
#include <cstring>
#include <emmintrin.h> // x86 SSE intrinsics
#include <iomanip>

#include "context.hpp"
#include "garbage_collector.hpp"

using namespace std;

namespace teseo::internal {


/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
extern mutex g_debugging_mutex [[maybe_unused]]; // context.cpp
#define DEBUG
#define COUT_DEBUG_FORCE(msg) { scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[IndexVertexID::" << __FUNCTION__ << "] [" << this_thread::get_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace { // anon namespace
struct Leaf {
    uint64_t m_key;
    void* m_address;
};
} // anon namespace

/*****************************************************************************
 *                                                                           *
 *  IndexVertexID                                                            *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::~IndexVertexID(){
    delete_nodes_rec(m_root);
    delete m_root; m_root = nullptr;
}

void IndexVertexID::insert(uint64_t vertex_id, int64_t count, void* btree_leaf_address){
    Key key(vertex_id);
    NodeEntry new_element{ create_leaf(vertex_id, btree_leaf_address), 0, nullptr };
    create_undo_version(new_element, count); // txn management

    lock_guard<OptimisticLatch<0>> lock(m_latch);
    do_insert(nullptr, 0, m_root, key, 0, new_element);
}

void IndexVertexID::do_insert(Node* node_parent, uint8_t byte_parent, Node* node_current, const Key& key, int key_level_start, NodeEntry& new_element){
    assert(node_current != nullptr && "No starting node given");
    assert((node_parent != nullptr || node_current == m_root) && "Isolated node");
    assert(((node_parent == nullptr) || (node_parent->get_child(byte_parent) == node_current)) && "byte_parent does not match the current node");

    uint8_t non_matching_prefix[Node::MAX_PREFIX_LEN];
    int non_matching_length = 0;

    do {
        // first check whether the prefix matches our key
        int key_level_end = 0; // up to where the current node matches the key, excl
        if( !node_current->prefix_match(key, key_level_start, &key_level_end, &(reinterpret_cast<uint8_t*>(non_matching_prefix)), &non_matching_length) ){
            assert(node_parent != nullptr);
            assert(non_matching_length > 0);

            // create a new node with a common prefix
            N4* node_new = new N4(node_current->get_prefix(), key_level_end - key_level_start);
            node_new->insert(key[key_level_end], new_element); // ok, count is fixed
            node_new->insert(non_matching_prefix[0], node_parent->get_child(key[key_level_start -1])); // ok, copy the count for the counting tree as well

            node_current->set_prefix(non_matching_prefix +1, non_matching_length -1);
            node_parent->change(key[key_level_start -1], node_new); // ok, version count already changed

            return; // done
        }

        // now check the byte at the current node
        key_level_start = key_level_end;
        uint8_t byte_current = key[key_level_start]; // separator key for the current node
        NodeEntry* node_child = node_current->get_child(byte_current);

        if(node_child == nullptr){
            do_insert_and_grow(
                    node_parent, byte_parent,
                    node_current, byte_current,
                    new_element
            ); // ok, count
            return; // done
        } else if(is_leaf(node_child->m_child)){
            Key key_sibling(get_leaf_vertex_id(node_child->m_child));

            key_level_start++;
            int prefix_length = 0;
            while(key[key_level_start + prefix_length] == key_sibling[key_level_start + prefix_length]) prefix_length++;

            N4* node_new = new N4(&key[key_level_start], prefix_length);
            node_new->insert(key[key_level_start + prefix_length], new_element); // FIXME the count
            node_new->insert(key_sibling[key_level_start + prefix_length], node_child);
            node_current->change(byte_current, node_new); // ok, version count already changed

            return; // done
        }
        // keep traversing the trie

        create_txn_undo(node_child, new_element.m_vertex_count);

        // Next iteration
        key_level_start++;
        node_parent = node_current; byte_parent = byte_current;
        node_current = node_child->m_child;
    } while (true);
}

void IndexVertexID::do_insert_and_grow(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, const NodeEntry& entry){
    assert((node_parent == nullptr || !is_leaf(node_parent)) && "It must be an inner node");
    assert((node_parent == nullptr || node_parent->get_child(key_parent) == node_current) && "Invalid key_parent");
    assert(!is_leaf(node_current) && "It must be an inner node");
    assert(node_current->get_child(key_current) == nullptr && "node_current already contains a child for `key_current'");

    if(node_current->is_overfilled()){ // there is no space in the current node for a new child, expand it
        assert((node_current->get_type() != NodeType::N256) && "");

        Node* node_old = node_current;
        switch(node_old->get_type()){ // create a new larger node
        case NodeType::N4:
            node_current = static_cast<N4*>(node_old)->to_N16();
            break;
        case NodeType::N16:
            node_current = static_cast<N16*>(node_old)->to_N48();
            break;
        case NodeType::N48:
            node_current = static_cast<N48*>(node_old)->to_N256();
            break;
        case NodeType::N256:
            assert(0 && "N256 should always have space for all 256 possible keys");
            break;
        }

        node_parent->change(key_parent, node_current); // update the ptr from the old to the new inner node
        mark_node_for_gc(node_old);
    }

    node_current->insert(key_current, entry); // fixme the count
}

bool IndexVertexID::remove(uint64_t vertex_id){
    Key key(vertex_id);

    lock_guard<OptimisticLatch<0>> lock(m_latch);
    return do_remove(nullptr, 0, m_root, key, 0, nullptr);
}

bool IndexVertexID::do_remove(Node* node_parent, uint8_t byte_parent, Node* node_current, const Key& key, int key_level_start, NodeEntry* out_entry_removed){
    do {
        // first check whether the prefix matches our key
        int key_level_end = 0; // up to where the current node matches the key, excl
        if( !node_current->prefix_match(key, key_level_start, &key_level_end, nullptr, nullptr) ){ return false; } // no match on the prefix

        key_level_start = key_level_end;
        uint8_t byte_current = key[key_level_start]; // separator key for the current node
        NodeEntry* node_child = node_current->get_child(byte_current);

        if( node_child == nullptr ) return false; // no match on the indexed byte

        if( is_leaf(node_child->m_child) ){ // our candidate leaf
            if( get_leaf_vertex_id(node_child->m_child) != key.get_vertex_id() ) return false; // not found!



            // if the current node is a N4 with only 1 child, remove
            if(node_current->num_children() == 2 && node_parent != nullptr){
                assert(node_current->get_type() == NodeType::N4);
                node_current->remove(byte_current, out_entry_removed);

                uint8_t byte_second; NodeEntry* node_second {nullptr};
                std::tie(byte_second, node_second) = reinterpret_cast<N4*>(node_current)->get_first_child();

                node_parent->change(byte_parent, node_second->m_child); // FIXME count
                if(!is_leaf(node_current)){ node_second->m_child->prepend_prefix(node_current, byte_second); }

                mark_node_for_gc(node_current);
            } else {
                return do_remove_and_shrink(node_parent, byte_parent, node_current, byte_current, out_entry_removed);
            }

        } else { // keep traversing
            key_level_start++;

            node_parent = node_current; byte_parent = byte_current;
            node_current = node_child->m_child;
        }

    } while( true );
}

bool IndexVertexID::do_remove_and_shrink(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, NodeEntry* out_entry_removed){
    assert((node_parent == nullptr || !is_leaf(node_parent)) && "node_parent must be an inner node");
    assert((node_parent == nullptr || node_parent->get_child(key_parent) == node_current) && "Invalid key_parent");

    bool removed = node_current->remove(key_current, out_entry_removed);

    if( removed && node_current->is_underfilled() ) {
        Node* node_new { nullptr };

        switch(node_current->get_type()){
        case NodeType::N4:
            assert(0 && "N4 cannot be underfilled");
            break;
        case NodeType::N16:
            node_new = static_cast<N16*>(node_current)->to_N4();
            break;
        case NodeType::N48:
            node_new = static_cast<N48*>(node_current)->to_N16();
            break;
        case NodeType::N256:
            node_new = static_cast<N256*>(node_current)->to_N48();
            break;
        }

        assert(node_new != nullptr);
        node_parent->change(key_parent, node_new);
        mark_node_for_gc(node_current);
    }

    return removed;
}

void* IndexVertexID::get_value_by_real_id(uint64_t vertex_id) const {
    Key key(vertex_id);
    do {
        try {
            return find_btree_leaf_by_vertex_id_leq(m_latch.read_version(), key, m_root, 0);
        } catch (Abort) { /* retry */ }
    } while (true);
}


void* IndexVertexID::find_btree_leaf_by_vertex_id_leq(uint64_t latch_version, const Key& key, Node* node, int level) const {
    assert(node != nullptr);

    // first check the damn prefix
    auto prefix_result = node->prefix_compare(key, level);
    m_latch.validate_version(latch_version);
    switch(prefix_result){
    case -1: {
        // counterintuitively, it means that the prefix of the node is lesser than the key
        // i.e. the key is bigger than any element in this node
        return get_max_leaf_address(latch_version, node);
    } break;
    case 0: {
        /* nop */
    } break;
    case +1: {
        // counterintuitively, it means that the prefix of the node is greater than the key
        // ask the parent to return the max for the sibling that precedes this node
        return nullptr;
    } break;
    } // end switch

    // second, find the next node to traverse in the tree
    bool exact_match; // set by node->find_node_leq as side effect
    Node* child = node->find_node_leq(key[level], &exact_match);
    m_latch.validate_version(latch_version);

    if(child == nullptr){
        return nullptr; // again, ask the parent to return the maximum of the previous sibling
    } else if (exact_match || is_leaf(child) ){
        if(is_leaf(child)){
            uint64_t vertex_id = get_leaf_vertex_id(child);
            void* value = get_leaf_address(child);
            m_latch.validate_version(latch_version);
            if(vertex_id <= key.get_vertex_id()) return value;

            // otherwise, check the sibling ...
        } else {
            // the other case is the current byte is equal to the byte indexing this node, we need to traverse the tree
            // and see whether further down they find a suitable leaf. If not, again we need to check the sibling
            void* result = find_btree_leaf_by_vertex_id_leq(latch_version, key, child, level +1);
            if (/* item found */ result != nullptr) return result;

            // otherwise check the left sibling ...
        }

        // then the correct node is the maximum of the previous sibling
        Node* sibling = node->get_predecessor(key[level]);

        // is the information we read still valid ?
        m_latch.validate_version(latch_version);

        if(sibling != nullptr){
            if(is_leaf(sibling)){
                void* value = get_leaf_address(child);

                // last check
                m_latch.validate_version(latch_version);

                return value;
            } else {

                return get_max_leaf_address(latch_version, sibling);
            }
        } else {
            // ask the parent
            return nullptr;
        }

    } else { // key[level] > child[level], but it is lower than all other children => return the max from the given child
        return get_max_leaf_address(latch_version, child);
    }
}

void* IndexVertexID::get_max_leaf_address(uint64_t latch_version, Node* node) const {
    m_latch.validate_version(latch_version);
    while(!is_leaf(node)){
        Node* child = node->max();

        // validate what we read is correct
        m_latch.validate_version(latch_version);

        // next iteration
        node = child;
    }

    m_latch.validate_version(latch_version);
    return get_leaf_address(node);
}

IndexVertexID::Node* IndexVertexID::create_leaf(uint64_t vertex_id, void* value){
    return reinterpret_cast<Node*>(reinterpret_cast<uint64_t>(new Leaf{vertex_id, value}) | (1ull<<63));
}

bool IndexVertexID::is_leaf(Node* node){
    return reinterpret_cast<uint64_t>(node) & (1ull<<63);
}

void* IndexVertexID::get_leaf_address(Node* leaf){
    assert(is_leaf(leaf) && "The given node is not a leaf");
    return reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(leaf) & (~(1ull<<63)))->m_address;
}

// Retrieve the vertex_id associated to the given leaf
uint64_t IndexVertexID::get_leaf_vertex_id(Node* leaf){
    assert(is_leaf(leaf) && "The given node is not a leaf");
    return reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(leaf) & (~(1ull<<63)))->m_key;
}

void IndexVertexID::mark_node_for_gc(Node* node){
    if(!is_leaf(node)){
        GlobalContext::context()->gc()->mark(node);
    } else { // the node is a leaf
        GlobalContext::context()->gc()->mark(reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63))));
    }
}

void IndexVertexID::delete_nodes_rec(Node* node){
    if(is_leaf(node)){
        delete reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63)));

    } else { // inner node

        for(int i = 0; i < 256; i++){
            NodeEntry* entry = node->get_child((uint8_t) i);
            if(entry == nullptr) continue; // move on

            if(entry->m_vertex_undo != nullptr){
                RAISE_EXCEPTION(LogicalError, "Cannot free memory for the given node in the IndexVertexID: a transaction undo log is still in place");
            }

            delete_nodes_rec( entry->m_child );
            delete entry->m_child;
        }
    }
}

void IndexVertexID::dump() const {
    Node::dump(cout, m_root, 0, 0);
}

/*****************************************************************************
 *                                                                           *
 *  Encoded keys (vertex ids)                                                *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::Key::Key(uint64_t key){
    // in Intel x86, 64 bit integers are stored reversed in memory due to little endiannes
    uint8_t* key_le = reinterpret_cast<uint8_t*>(key);
    for(int i = 0; i < 8; i++){ m_data[i] = key_le[7 - i]; }
}

uint8_t& IndexVertexID::Key::operator[](uint32_t i){
    assert(i < length() && "Overflow");
    return m_data[i];
}

const uint8_t& IndexVertexID::Key::operator[](uint32_t i) const{
    assert(i < length() && "Overflow");
    return m_data[i];
}

// The length of the key
uint32_t IndexVertexID::Key::length() const {
    static_assert(MAX_LENGTH == 8);
    return MAX_LENGTH;
}

uint64_t IndexVertexID::Key::get_vertex_id() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 0; i < 8; i++){ key_be[i] = m_data[7 - i]; }
    return vertex_id;
}

std::ostream& operator<<(std::ostream& out, const IndexVertexID::Key& key){
    assert(key.length() == 8 && "Expected a fixed value of 8 bytes");

    // Decode the key
    union {
        uint64_t m_value;
        uint8_t m_bytes[8];
    } key_le;

    key_le.m_value = 0; // init
    for(uint32_t i = 0; i < key.length(); i++){
        key_le.m_bytes[i] = key[7 - i];
    }

    out << "{KEY: " << key_le.m_value << ", bytes={";
    for(int i = 0; i < key.length(); i++){
        if(i > 0) out << ", ";
        out << i << ": " << (int) key[i];
    }
    out << "}}";

    return out;
}

/*****************************************************************************
 *                                                                           *
 *  Generic Node                                                             *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::NodeType IndexVertexID::Node::get_type() const{
    return m_type;
}

IndexVertexID::Node::Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length) : m_children_count(0) {
    set_type(type);
    set_prefix(prefix, prefix_length);
}

IndexVertexID::Node::~Node() {

}

void IndexVertexID::Node::set_type(NodeType type){
    m_type = type;
}

int IndexVertexID::Node::num_children() const {
    return m_children_count;
}

uint8_t* IndexVertexID::Node::get_prefix() const {
    return m_prefix;
}

int IndexVertexID::Node::get_prefix_length() const {
    return m_prefix_count;
}

bool IndexVertexID::Node::has_prefix() const {
    return get_prefix_length() > 0;
}

void IndexVertexID::Node::set_prefix(const uint8_t* prefix, uint32_t length) {
    assert(length <= MAX_PREFIX_LEN && "Overflow");
    memcpy(m_prefix, prefix, length);
    m_prefix_count = static_cast<uint8_t>(length);
}


bool IndexVertexID::Node::prefix_match(const Key& key, int prefix_start, int* out_prefix_end, uint8_t** out_non_matching_prefix, int* out_non_matching_length){
    int i, j, prefix_length = get_prefix_length();
    int max_length = std::min<int>(prefix_length, key.length() - prefix_start);
    for(i = 0, j = prefix_start; i < max_length && m_prefix[i] == key[j]; i++, j++) /* nop */;
    if(out_prefix_end != nullptr){ *out_prefix_end = j; }
    if(out_non_matching_prefix != nullptr){ memcpy(*out_non_matching_prefix, m_prefix + i, prefix_length - i); }
    if(out_non_matching_length != nullptr){ *out_non_matching_length = prefix_length - i; }
    return (i == prefix_length);
}

int IndexVertexID::Node::prefix_compare(const Key& search_key, int& /* in/out */ search_key_level) const {
    if(!has_prefix()) return 0; // the current doesn't have a prefix => technically it matches

    for(int i = 0, prefix_length = get_prefix_length(); i < prefix_length; i++){
//        if (i == maxStoredPrefixLength) { // THIS CANNOT OCCUR IN THIS IMPL
//            Leaf* leaf = N::getAnyChildTid(n, needRestart);
//            if (needRestart) return PCCompareResults::Equal;
//            kt = Key::encode(leaf->m_key);
//        }

        uint8_t byte_search_key = (search_key.length() > search_key_level) ? search_key[search_key_level] : 0; // also this cannot occur
        uint8_t byte_prefix = /* i >= maxStoredPrefixLength ? kt[level] : */ get_prefix()[i];
        if (byte_prefix < byte_search_key) {
            return -1;
        } else if (byte_prefix > byte_search_key) {
            return +1;
        }

        search_key_level++;
    }

    return 0;
}

void IndexVertexID::Node::prepend_prefix(Node* first_part, uint8_t second_part){
    assert(first_part != nullptr && !is_leaf(first_part));
    int num_bytes_to_prepend = std::min(MAX_PREFIX_LEN, first_part->get_prefix_length() + 1);
    memmove(/* to */ m_prefix + num_bytes_to_prepend, /* from */ m_prefix,  std::min(get_prefix_length(), MAX_PREFIX_LEN - num_bytes_to_prepend));
    memcpy(/* to */ m_prefix, first_part->get_prefix(), std::min(num_bytes_to_prepend, first_part->get_prefix_length()));
    if (first_part->get_prefix_length() < MAX_PREFIX_LEN) { m_prefix[num_bytes_to_prepend - 1] = second_part; }
    this->m_prefix_count += first_part->get_prefix_length() + 1;
}

void IndexVertexID::Node::insert(uint8_t key, const NodeEntry* entry) {
    assert(entry != nullptr);
    insert(key, *entry);
}

void IndexVertexID::Node::change(uint8_t byte, Node* node, int64_t count_diff){
    NodeEntry* entry = get_child(byte);
    assert(entry != nullptr && "The entry does not exist");
    entry->m_child = node;
    if(count_diff != 0) { create_txn_undo(entry, count_diff); }
}

IndexVertexID::Node* IndexVertexID::Node::get_predecessor(uint8_t key) const {
    return (key > 0) ? find_node_leq(key -1, nullptr) : nullptr;
}

static void print_tabs(std::ostream& out, int depth){
    auto flags = out.flags();
    out << setw(depth * 4) << setfill(' ') << ' ';
    out.setf(flags);
}

void IndexVertexID::Node::dump(std::ostream& out, Node* node, int level, int depth) {
    assert(node != nullptr);

    print_tabs(out, depth);

    if(is_leaf(node)){
        out << "Leaf: " << node << ", vertex_id: " << get_leaf_vertex_id(node) << ", value: " << (uint64_t) get_leaf_address(node) << " (" << get_leaf_address(node) << ")\n";
    } else {
        out << "Node: " << node << ", key level: " << level << ", type: ";
        switch(node->get_type()){
        case NodeType::N4: out << "N4"; break;
        case NodeType::N16: out << "N16"; break;
        case NodeType::N48: out << "N48"; break;
        case NodeType::N256: out << "N256"; break;
        }
        out << " (" << static_cast<int>(node->get_type()) << ")\n";

        // prefix
        print_tabs(out, depth);
        out << "Prefix, length: " << node->get_prefix_length();
        for(int i = 0, sz = node->get_prefix_length(); i < sz; i++){
            out << ", " << i << ": 0x" << std::hex << static_cast<int64_t>(node->get_prefix()[i]) << std::dec;
        }

        out << "\n";

        // children
        out << "Children: " << num_children();
        for(uint8_t i = 0; i <= 255; i++){
            NodeEntry* entry = node->get_child(i);
            if(entry == nullptr) continue;
            out << ", {byte:" << static_cast<int>(i) << ", pointer:" << entry->m_child << "}";
        }
        out << "\n";

        // recursively dump the children
        for(int i = 0; i < num_children(); i++){
            NodeEntry* entry = node->get_child(i);
            if(entry == nullptr) continue;
            dump(out, entry->m_child, level + 1 + node->get_prefix_length(), depth + 1);
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *  Node4                                                                    *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::N4::N4(const uint8_t *prefix, uint32_t prefix_length) : Node(NodeType::N4, prefix, prefix_length){

}


void IndexVertexID::N4::insert(uint8_t key, const NodeEntry& entry) {
    int pos;
    for(pos = num_children(); pos > 0 && m_keys[pos -1] > key; pos--){ // shift larger keys to the right
        m_keys[pos] = m_keys[pos -1];
        m_children[pos] = m_children[pos -1];
    }

    m_keys[pos] = key;
    m_children[pos] = entry;
    m_children_count++;
}

bool IndexVertexID::N4::remove(uint8_t byte, NodeEntry* out_old_entry){
    for (int i = 0, count = num_children(); i < count; i++) {
        if (m_keys[i] == byte) {
            mark_node_for_gc(m_children[i].m_child); m_children[i].m_child = nullptr;
            if(out_old_entry != nullptr){ *out_old_entry = m_children[i]; }

            memmove(m_keys + i, m_keys + i + 1, count - i - 1);
            memmove(m_children + i, m_children + i + 1, (count - i - 1) * sizeof(NodeEntry));
            m_children_count--;
            return true;
        }
    }

    return false;
}

IndexVertexID::NodeEntry* IndexVertexID::N4::get_child(uint8_t byte) const {
    for (int i = 0, end = m_children_count; i < end; i++) {
        if(m_keys[i] == byte)
            return m_children[i];
    }
    return nullptr;
}

tuple</* key */ uint8_t, /* entry */ IndexVertexID::NodeEntry> IndexVertexID::N4::get_first_child() const {
    assert(num_children() > 0 && "Node emtpy");
    return std::make_tuple(m_keys[0], m_children[0]);
}

pair<IndexVertexID::Node*, /* exact match ? */ bool> IndexVertexID::N4::find_node_leq(uint8_t key) const {

}

IndexVertexID::Node* IndexVertexID::N4::max() const {
    assert(num_children() > 0 && "empty node?");
    return m_children[num_children() -1].m_child;
}

bool IndexVertexID::N4::is_overfilled() const {
    return num_children() == 4;
}

bool IndexVertexID::N4::is_underfilled() const {
    return false;
}

IndexVertexID::N16* IndexVertexID::N4::to_N16() const {
    N16* new_node = new N16(get_prefix(), get_prefix_length());
    for(int i = 0; i < num_children(); i++){
        new_node->insert(m_keys[i], m_children[i]);
    }
    return new_node;
}


/*****************************************************************************
 *                                                                           *
 *  Node16                                                                   *
 *                                                                           *
 *****************************************************************************/
IndexVertexID::N16::N16(const uint8_t* prefix, uint32_t prefix_length):
           Node(NodeType::N16, prefix, prefix_length) {
    memset(m_keys, 0, sizeof(m_keys));
    memset(m_children, 0, sizeof(m_children));
}

void IndexVertexID::N16::insert(uint8_t key, const NodeEntry& entry) {
    uint8_t keyByteFlipped = flip_sign(key);
    __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped), _mm_loadu_si128(reinterpret_cast<__m128i *>(m_keys)));
    uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - num_children()));
    unsigned pos = bitfield ? ctz(bitfield) : num_children();
    memmove(m_keys + pos + 1, m_keys + pos, num_children() - pos);
    memmove(m_children + pos + 1, m_children + pos, (num_children() - pos) * sizeof(NodeEntry));
    m_keys[pos] = keyByteFlipped;
    m_children[pos] = entry;
    m_children_count++;
}

bool IndexVertexID::N16::remove(uint8_t byte, NodeEntry* out_old_entry){
    NodeEntry* entry = get_child(byte);
    if(entry == nullptr) return false;
    mark_node_for_gc(entry->m_child); entry->m_child = nullptr;
    if(out_old_entry != nullptr) { *out_old_entry = *entry; }

    std::size_t pos = entry - m_children;
    memmove(m_keys + pos, m_keys + pos + 1, num_children() - pos - 1);
    memmove(m_children + pos, m_children + pos + 1, (num_children() - pos - 1) * sizeof(NodeEntry));
    m_children_count--;

    assert(get_child(byte) == nullptr);
    return true;
}

IndexVertexID::NodeEntry* IndexVertexID::N16::get_child(uint8_t k) const {
    __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flip_sign(k)),
                                 _mm_loadu_si128(reinterpret_cast<const __m128i *>(m_keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << num_children()) - 1);

    if(bitfield){
        return &m_children[ctz(bitfield)];
    } else {
        return nullptr;
    }
}

pair<IndexVertexID::Node*, /* exact match ? */ bool> IndexVertexID::N16::find_node_leq(uint8_t key) const {

}

IndexVertexID::Node* IndexVertexID::N16::max() const {
    assert(num_children() > 0 && "empty node?");
    return m_children[num_children() -1].m_child;
}

bool IndexVertexID::N16::is_overfilled() const {
    return num_children() == 16;
}

bool IndexVertexID::N16::is_underfilled() const {
    return num_children() <= 3;
}

uint8_t IndexVertexID::N16::flip_sign(uint8_t byte) {
    return byte ^ 128;
}

unsigned IndexVertexID::N16::ctz(uint16_t value){
    return __builtin_ctz(value);
}

IndexVertexID::N4* IndexVertexID::N16::to_N4() const {
    if(num_children() > 4) RAISE(InternalError, "N16 cannot shrink to N4, the number of children is : " << num_children());

    N4* new_node = new N4(get_prefix(), get_prefix_length());
    for(int i = 0; i < num_children(); i++){
        new_node->insert(m_keys[i], m_children[i]);
    }
    return new_node;
}

IndexVertexID::N48* IndexVertexID::N16::to_N48() const {
    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0; i < num_children(); i++){
        new_node->insert(m_keys[i], m_children[i]);
    }
    return new_node;
}

/*****************************************************************************
 *                                                                           *
 *  Node48                                                                   *
 *                                                                           *
 *****************************************************************************/
IndexVertexID::N48::N48(const uint8_t* prefix, uint32_t prefix_length) :
        Node(NodeType::N48, prefix, prefix_length) {
    memset(m_child_index, EMPTY_MARKER, sizeof(m_child_index));
    memset(m_children, 0, sizeof(m_children));
}

void IndexVertexID::N48::insert(uint8_t key, const NodeEntry& entry){
    assert(!is_overfilled() && "This node is full");

    int pos = num_children();
    if (m_children[pos].m_child == nullptr) {
        for (pos = 0; m_children[pos].m_child != nullptr; pos++);
    }
    m_children[pos] = entry;
    m_child_index[key] = (uint8_t) pos;

    m_children_count++;
}

bool IndexVertexID::N48::remove(uint8_t byte, NodeEntry* out_old_entry){
    if(m_child_index[byte] == EMPTY_MARKER) return false;
    auto& entry = m_children[m_child_index[byte]];
    mark_node_for_gc(entry.m_child); entry.m_child = nullptr;
    if(out_old_entry != nullptr) { *out_old_entry = entry; }
    entry = NodeEntry{};
    m_child_index[byte] = EMPTY_MARKER;
    m_children_count--;

    assert(get_child(byte) == nullptr);
    return true;
}

IndexVertexID::NodeEntry* IndexVertexID::N48::get_child(const uint8_t k) const {
    if (m_child_index[k] == EMPTY_MARKER) {
        return nullptr;
    } else {
        return &m_children[m_child_index[k]];
    }
}

pair<IndexVertexID::Node*, /* exact match ? */ bool> IndexVertexID::N48::find_node_leq(uint8_t key) const {

}

IndexVertexID::Node* IndexVertexID::N48::max() const {
    for (int i = 255; i >= 0; i--) {
        if(m_child_index[i] != EMPTY_MARKER){
            return m_children[m_child_index[i]].m_child;
        }
    }

    assert(0 && "This code should be unreachable!");
    return nullptr;
}

bool IndexVertexID::N48::is_overfilled() const {
    return num_children() == 48;
}

bool IndexVertexID::N48::is_underfilled() const {
    return num_children() <= 12;
}

IndexVertexID::N16* IndexVertexID::N48::to_N16() const {
    if(num_children() > 16) RAISE(InternalError, "N48 cannot shrink to N16, the number of children is : " << num_children());

    N16* new_node = new N16(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_child_index[i] != EMPTY_MARKER){
            new_node->insert((uint8_t) i, m_children[m_child_index[i]]);
        }
    }
    return new_node;
}

IndexVertexID::N256* IndexVertexID::N48::to_N256() const {
    N256* new_node = new N256(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_child_index[i] != EMPTY_MARKER){
            new_node->insert((uint8_t) i, m_children[m_child_index[i]]);
        }
    }
    return new_node;
}

/*****************************************************************************
 *                                                                           *
 *  Node256                                                                  *
 *                                                                           *
 *****************************************************************************/
IndexVertexID::N256::N256(const uint8_t* prefix, uint32_t prefix_length){
    memset(m_children, '\0', sizeof(m_children));
}

void IndexVertexID::N256::insert(uint8_t byte, const NodeEntry& entry){
    assert(m_children[byte].m_child == nullptr && "Slot already occupied");
    m_children[byte] = entry;
    m_children_count++;
}

bool IndexVertexID::N256::remove(uint8_t byte, NodeEntry* out_old_entry){
    if(m_children[byte].m_child == nullptr) return false;
    mark_node_for_gc( m_children[byte].m_child );
    if(out_old_entry != nullptr) {
        m_children[byte].m_child = nullptr;
        *out_old_entry = m_children[byte].m_child;
    }
    m_children[byte] = NodeEntry{};
    m_children_count--;
    return true;
}

IndexVertexID::NodeEntry* IndexVertexID::N256::get_child(uint8_t byte) const{
    NodeEntry& entry = m_children[byte];
    return (entry.m_child != nullptr) ? &entry : nullptr;
}

pair<IndexVertexID::Node*, /* exact match ? */ bool> IndexVertexID::N256::find_node_leq(uint8_t key) const {

}

IndexVertexID::Node* IndexVertexID::N256::max() const {
    for (int i = 255; i >= 0; i--) {
        if(m_children[i].m_child != nullptr){
            return m_children[i].m_child;
        }
    }

    assert(0 && "This code should be unreachable!");
    return nullptr;
}

bool IndexVertexID::N256::is_overfilled() const{
    return false;
}

bool IndexVertexID::N256::is_underfilled() const {
    return num_children() <= 37;
}

IndexVertexID::N48* IndexVertexID::N256::to_N48() const {
    if(num_children() > 48) RAISE(InternalError, "N256 cannot shrink to N48, the number of children is : " << num_children());

    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_children[i].m_child != nullptr){
            new_node->insert((uint8_t) i, m_children[i]);
        }
    }

    return new_node;
}

} // namespace
