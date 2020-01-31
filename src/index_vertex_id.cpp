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

// if defined, replace the leaf pointer with a Leaf node, not linked to the underlying B-Tree, for debugging purposes
#define DEBUG_INDEX_VERTEX_ID_TEST_LEAVES
#if defined(DEBUG_INDEX_VERTEX_ID_TEST_LEAVES)
namespace { // anon namespace
struct Leaf {
    uint64_t m_key;
    void* m_payload;
};
} // anon namespace
#endif /* DEBUG_INDEX_VERTEX_ID_TEST_LEAVES */

/*****************************************************************************
 *                                                                           *
 *  IndexVertexID                                                            *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::~IndexVertexID(){
    delete_nodes_rec(m_root);
    delete m_root; m_root = nullptr;
}

void IndexVertexID::insert(uint64_t vertex_id, int64_t count, void* value){
    Key key(vertex_id);
    NodeEntry new_element{ create_leaf(vertex_id, value), (uint64_t) count, nullptr };
    int level_start = 0; // how many bytes of the key we have already indexed, incl
    int level_end = 0; // up to where the current node matches the key, excl
    uint8_t key_current {0}; // the separator key for the current node
    uint8_t key_parent {0}; // the separator key for the parent node
    uint8_t non_matching_prefix[Node::MAX_PREFIX_LEN];
    int non_matching_length = 0;

    lock_guard<OptimisticLatch<0>> lock(m_latch);

    Node* node_parent { nullptr };
    Node* node_current { nullptr };
    Node* node_child = m_root;

    do {
        // Move to the next node in the path
        node_parent = node_current; key_parent = key_current;
        node_current = node_child;
        node_child = nullptr; // tbd

        // first check whether the prefix matches our key
        if( !node_current->prefix_match(key, level_start, &level_end, &(reinterpret_cast<uint8_t*>(non_matching_prefix)), &non_matching_length) ){
            assert(node_parent != nullptr);
            assert(non_matching_length > 0);

            // create a new node with a common prefix
            N4* node_new = new N4(node_current->get_prefix(), level_end - level_start);
            node_new->insert(key[level_end], new_element, true);
            node_new->insert(non_matching_prefix[0], node_parent->get_child(key[level_start -1]), false);

            node_current->set_prefix(non_matching_prefix +1, non_matching_length -1);

            node_parent->update(key[level_start -1], node_new, count);

            return; // done
        }

        // now check the byte at the current node
        level_start = level_end;
        key_current = key[level_start];
        node_child = node_current->get_child(key_current);

        if(node_child == nullptr){
            insert_and_grow(
                    node_parent, key_parent,
                    node_current, key_current,
                    new_element
            );
            return; // done
        } else if(is_leaf(node_child)){
            Key key_sibling(get_leaf_vertex_id(node_child));

            level_start++;
            int prefix_length = 0;
            while(key[level_start + prefix_length] == key_sibling[level_start + prefix_length]) prefix_length++;

            N4* node_new = new N4(&key[level_start], prefix_length);
            node_new->insert(key[level_start + prefix_length], new_element, true);
            node_new->insert(key_sibling[level_start + prefix_length], node_current->get_child(key_current), false);
            node_current->update(key_current, node_new, count);

            return; // done
        } else { // keep traversing the trie
            node_current->update(key[level_start], node_child, count);

            level_start++;
        }
    } while (true);
}

void IndexVertexID::insert_and_grow(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, const NodeEntry& child){
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

        node_parent->update(key_parent, node_current, 0); // update the ptr from the old to the new inner node
        mark_node_for_gc(node_old);
    }

    node_current->insert(key_current, child, /* create a new entry in the undo log ? */ true);
}

IndexVertexID::Node* IndexVertexID::create_leaf(uint64_t vertex_id, void* value){
#if defined(DEBUG_INDEX_VERTEX_ID_TEST_LEAVES) /* debug only */
    return reinterpret_cast<Node*>(reinterpret_cast<uint64_t>(new Leaf{vertex_id, value}) | (1ull<<63));
#else
    return reinterpret_cast<Node*>(reinterpret_cast<uint64_t>(value) | (1ull<<63));
#endif
}

bool IndexVertexID::is_leaf(Node* node){
    return reinterpret_cast<uint64_t>(node) & (1ull<<63);
}

void* IndexVertexID::get_leaf_address(Node* leaf){
    assert(is_leaf(leaf) && "The given node is not a leaf");
#if defined(DEBUG_INDEX_VERTEX_ID_TEST_LEAVES) /* debug only */
    return reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(leaf) & (~(1ull<<63)))->m_payload;
#else
    return reinterpret_cast<void*>(reinterpret_cast<uint64_t>(leaf) & (~(1ull<<63)));
#endif
}

// Retrieve the vertex_id associated to the given leaf
uint64_t IndexVertexID::get_leaf_vertex_id(Node* leaf){
    assert(is_leaf(leaf) && "The given node is not a leaf");
#if defined(DEBUG_INDEX_VERTEX_ID_TEST_LEAVES) /* debug only */
    return reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(leaf) & (~(1ull<<63)))->m_key;
#else
    static_assert(false, "Not implemented yet");
#endif
}

void IndexVertexID::mark_node_for_gc(Node* node){
    if(!is_leaf(node)){
        GlobalContext::context()->gc()->mark(node);
    }
#if defined(DEBUG_INDEX_VERTEX_ID_TEST_LEAVES) /* debug only */
    else { // the node is a leaf
        GlobalContext::context()->gc()->mark(reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63))));
    }
#endif
}

static void IndexVertexID::delete_nodes_rec(Node* node){
    if(is_leaf(node)){
#if defined(DEBUG_INDEX_VERTEX_ID_TEST_LEAVES)
        delete reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63)));
#endif
        return;
    }

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

void IndexVertexID::Node::insert(uint8_t key, const NodeEntry* child, bool undo_tx) {
    assert(child != nullptr);
    insert(key, *child, undo_tx);
}

/*****************************************************************************
 *                                                                           *
 *  Node4                                                                    *
 *                                                                           *
 *****************************************************************************/

IndexVertexID::N4::N4(const uint8_t *prefix, uint32_t prefix_length) : Node(NodeType::N4, prefix, prefix_length){

}


void IndexVertexID::N4::insert(uint8_t key, const NodeEntry& value, bool undo_tx) {
    int pos;
    for(pos = num_children(); pos > 0 && m_keys[pos -1] > key; pos--){ // shift larger keys to the right
        m_keys[pos] = m_keys[pos -1];
        m_children[pos] = m_children[pos -1];
    }

    m_keys[pos] = key;
    update_entry(m_children[pos], value, undo_tx);
    m_children_count++;
}

IndexVertexID::NodeEntry* IndexVertexID::N4::get_child(uint8_t byte) const {
    for (int i = 0, end = m_children_count; i < end; i++) {
        if(m_keys[i] == byte)
            return m_children[i];
    }
    return nullptr;
}

bool IndexVertexID::N4::is_overfilled() const {
    return num_children() == 4;
}

bool IndexVertexID::N4::is_underfilled() const {
    return false;
}

//void IndexVertexID::N4::update(uint8_t byte, Node* child, int64_t count_diff){
//    for(int i = 0, end = m_children_count; i < end; i++){
//        if(m_keys[i] == byte){
//            NodeEntry new_value { child, count_diff, nullptr };
//            update_entry(m_children[i], new_value);
//            return;
//        }
//    }
//
//    RAISE_EXCEPTION(InternalError, "The entry for byte '" << (int) byte << "' does not exist");
//}

IndexVertexID::N16* IndexVertexID::N4::to_N16() const {
    N16* new_node = new N16(get_prefix(), get_prefix_length());
    for(int i = 0; i < num_children(); i++){
        new_node->insert(m_keys[i], m_children[i], false);
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

void IndexVertexID::N16::insert(uint8_t key, const NodeEntry& value, bool undo_tx) {
    uint8_t keyByteFlipped = flip_sign(key);
    __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped), _mm_loadu_si128(reinterpret_cast<__m128i *>(m_keys)));
    uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - num_children()));
    unsigned pos = bitfield ? ctz(bitfield) : num_children();
    memmove(m_keys + pos + 1, m_keys + pos, num_children() - pos);
    memmove(m_children + pos + 1, m_children + pos, (num_children() - pos) * sizeof(NodeEntry));
    m_keys[pos] = keyByteFlipped;
    update_entry(m_children[pos], value, undo_tx);
    m_children_count++;
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
        new_node->insert(m_keys[i], m_children[i], false);
    }
    return new_node;
}

IndexVertexID::N48* IndexVertexID::N16::to_N48() const {
    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0; i < num_children(); i++){
        new_node->insert(m_keys[i], m_children[i], false);
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

void IndexVertexID::N48::insert(uint8_t key, const NodeEntry& value, bool undo_tx){
    assert(!is_overfilled() && "This node is full");

    int pos = num_children();
    if (m_children[pos].m_child == nullptr) {
        for (pos = 0; m_children[pos].m_child != nullptr; pos++);
    }
    update_entry(m_children[pos], value, undo_tx);

    m_child_index[key] = (uint8_t) pos;

    m_children_count++;
}

IndexVertexID::NodeEntry* IndexVertexID::N48::get_child(const uint8_t k) const {
    if (m_child_index[k] == EMPTY_MARKER) {
        return nullptr;
    } else {
        return &m_children[m_child_index[k]];
    }
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
            new_node->insert((uint8_t) i, m_children[m_child_index[i]], false);
        }
    }
    return new_node;
}

IndexVertexID::N256* IndexVertexID::N48::to_N256() const {
    N256* new_node = new N256(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_child_index[i] != EMPTY_MARKER){
            new_node->insert((uint8_t) i, m_children[m_child_index[i]], false);
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

void IndexVertexID::N256::insert(uint8_t byte, const NodeEntry& entry, bool undo_tx){
    assert(m_children[byte].m_child == nullptr && "Slot already occupied");
    update_entry(m_children[byte], entry, undo_tx);
    m_children_count++;
}

IndexVertexID::NodeEntry* IndexVertexID::N256::get_child(uint8_t byte) const{
    NodeEntry& entry = m_children[byte];
    return (entry.m_child != nullptr) ? &entry : nullptr;
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
            new_node->insert((uint8_t) i, m_children[i], false);
        }
    }

    return new_node;
}

} // namespace
