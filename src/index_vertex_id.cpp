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

#include <cstring>

using namespace std;

namespace teseo::internal {


/*****************************************************************************
 *                                                                           *
 *  IndexVertexID                                                            *
 *                                                                           *
 *****************************************************************************/

bool IndexVertexID::is_leaf(Node* node){
    return reinterpret_cast<uint64_t>(node) & (1ull<<63);
}

void* IndexVertexID::get_leaf_address(Node* leaf){
    assert(is_leaf(leaf) && "The given node is not a leaf");
    return reinterpret_cast<void*>(reinterpret_cast<uint64_t>(leaf) & (~(1ull<<63)));
}

IndexVertexID::Node* IndexVertexID::create_leaf(void* value){
    return reinterpret_cast<Node*>(reinterpret_cast<uint64_t>(value) | (1ull<<63));
}

void IndexVertexID::insert(uint64_t vertex_id, int64_t count, void* value){
    Key key(vertex_id);
    int level_start = 0; // how many bytes of the key we have already indexed
    int level_end = 0; // up to where the current node matches the key
    uint8_t non_matching_prefix[Node::MAX_PREFIX_LEN];
    int non_matching_length = 0;

    Node* node_parent { nullptr };
    Node* node_current { nullptr };
    Node* node_child = m_root; node_child->write_lock();

    do {
        // Move to the next link in the chain
        node_parent = node_current;
        node_current = node_child;
        node_child = nullptr; // tbd

        // first check whether the prefix matches our key
        if( !node_current->prefix_match(key, level_start, &level_end, &(reinterpret_cast<uint8_t*>(non_matching_prefix)), &non_matching_length) ){
            assert(node_parent != nullptr);
            assert(non_matching_length > 0);

            // create a new node with a common prefix
            N4* node_new = new N4(node_current->get_prefix(), level_end - level_start);
            node_new->insert(key[level_end], NodeEntry{ create_leaf(value), count, nullptr });
            node_new->insert(non_matching_prefix[0], node_parent->get_entry(key[level_start -1]));

            node_current->set_prefix(non_matching_prefix +1, non_matching_length -1);
            node_current->write_unlock();

            node_parent->update(key[level_start -1], node_new, count);
            node_parent->write_unlock();

            return; // done
        }

        // now check the byte at the current node
        level_start = level_end;
        node_child = node_current->get_child(key[level_start]);

        if(node_child == nullptr){
            node_current->insert(...);
            node_current->write_unlock();
            return; // done
        } else if(is_leaf(node_child)){

        } else { // keep traversing the trie
            if(node_parent != nullptr) node_parent->write_unlock();
            node_current->update_vertex_count(key[level_start], count);
            node_child->write_lock();

            level_start++;
        }
    } while (true);
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
    return reinterpret_cast<NodeType>(m_version.get_payload());
}

IndexVertexID::Node::Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length) : m_children_count(0) {
    set_type(type);
    set_prefix(prefix, prefix_length);
}

IndexVertexID::Node::~Node() {

}

void IndexVertexID::Node::set_type(NodeType type){
    m_version.set_payload((int) type);
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

uint64_t IndexVertexID::Node::read_lock() const {
    return m_version.read_version();
}

void IndexVertexID::Node::read_validate(uint64_t version) const {
    m_version.validate_version(version);
}

void IndexVertexID::Node::write_lock() {
    m_version.lock();
}

void IndexVertexID::Node::write_lock(uint64_t version) {
    m_version.update(version);
}

void IndexVertexID::Node::write_unlock(){
    m_version.unlock();
}

std::pair<uint64_t, UndoEntry*> IndexVertexID::Node::get_vertex_undo()


/*****************************************************************************
 *                                                                           *
 *  Node4                                                                    *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::N4::N4(const uint8_t *prefix, uint32_t prefix_length) : Node(NodeType::N4, prefix, prefix_length){

}


void IndexVertexID::N4::insert(uint8_t key, NodeEntry value) {
    int pos;
    for(pos = num_children(); pos > 0 && m_keys[pos -1] > key; pos--){ // shift larger keys to the right
        m_keys[pos] = m_keys[pos -1];
        m_children[pos] = m_children[pos -1];
        m_vertex_count[pos] = m_vertex_count[pos -1];
        m_vertex_version[pos] = m_vertex_version[pos -1];
    }

    m_keys[pos] = key;
    m_children[pos] = value.m_child;
    m_vertex_count[pos] = value.m_vertex_count;
    UndoEntry::set_entry_vertex_count(&(m_vertex_version[pos]), value.m_vertex_undo);

    m_children_count++;
}

IndexVertexID::Node* IndexVertexID::N4::get_child(uint8_t byte) const {
    for (int i = 0, end = m_children_count; i < end; i++) {
        if(m_keys[i] == byte)
            return m_children[i];
    }
    return nullptr;
}

IndexVertexID::NodeEntry IndexVertexID::N4::get_entry(uint8_t byte) const{
    NodeEntry result;
    result.m_child = nullptr;

    for (int i = 0, end = m_children_count; i < end; i++) {
        if(m_keys[i] == byte){
            result.m_child = m_children[i];
            result.m_vertex_count = m_vertex_count[i];
            result.m_vertex_undo = m_vertex_version[i];
        }
    }

    return result;
}


    // A generic node in the
    class Node {
        OptimisticLatch<2> m_version;
        uint8_t m_count; // number of children in the node
        uint8_t m_prefix_count; // number of bytes in the prefix
        uint8_t m_prefix[11]; // prefix shared by all keys

    protected:
        void set_type(NodeType type);

    public:
        NodeType get_type() const;

        // Number of children in the node
        uint32_t num_children() const;

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
        uint32_t get_prefix_length() const;

        // Set the prefix
        void set_prefix(const uint8_t* prefix, uint32_t length);
    };
}
