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

#include <cassert>
#include <cstring>
#include <emmintrin.h> // x86 SSE intrinsics
#include <iomanip>

#include "context.hpp"
#include "garbage_collector.hpp"
#include "index.hpp"

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

/*****************************************************************************
 *                                                                           *
 *  IndexVertexID                                                            *
 *                                                                           *
 *****************************************************************************/

Index::Index(){
    m_root = new Index::N256(nullptr, 0);
}

Index::~Index(){
    delete_nodes_rec(m_root);
    delete m_root; m_root = nullptr;
}

void Index::insert(uint64_t src, uint64_t dst, void* btree_leaf_address){
    Leaf* element = new Leaf{ Key{src, dst}, btree_leaf_address };
    bool done = false;
    do {
        try {
            do_insert(nullptr, 0, 0, m_root, element, 0);
            done = true;
        } catch (Abort){
             // try again
        }
    } while (!done);
}

void Index::do_insert(Node* node_parent, uint8_t byte_parent, uint64_t version_parent, Node* node_current, Leaf* element, int key_level_start){
    assert(node_current != nullptr && "No starting node given");
    assert((node_parent != nullptr || node_current == m_root) && "Isolated node");
    assert(((node_parent == nullptr) || (node_parent->get_child(byte_parent) == node_current)) && "byte_parent does not match the current node");

    uint8_t non_matching_prefix[Node::MAX_PREFIX_LEN]; uint8_t* ptr_non_matching_prefix = non_matching_prefix;
    int non_matching_length = 0;
    auto& key = element->m_key;

    do {
        uint64_t version_current = node_current->latch_read_lock();

        // first check whether the prefix matches our key
        int key_level_end = 0; // up to where the current node matches the key, excl
        if( !node_current->prefix_match_exact(key, key_level_start, &key_level_end, &ptr_non_matching_prefix, &non_matching_length) ){
            assert(node_parent != nullptr);
            assert(non_matching_length > 0);

            // acquire the latch to both the parent && current node
            node_parent->latch_upgrade_to_write_lock(version_parent);
            try {
                node_current->latch_upgrade_to_write_lock(version_current);
            } catch (Abort){
                node_parent->latch_write_unlock();
                throw;
            }

            // create a new node with a common prefix
            N4* node_new = new N4(node_current->get_prefix(), key_level_end - key_level_start);
            node_new->insert(key[key_level_end], leaf2node(element));
            node_new->insert(non_matching_prefix[0], node_parent->get_child(key[key_level_start -1]));

            node_parent->change(key[key_level_start -1], node_new);
            node_parent->latch_write_unlock();

            node_current->set_prefix(non_matching_prefix +1, non_matching_length -1);
            node_current->latch_write_unlock();

            return; // done
        }

        // now check the byte at the current node
        key_level_start = key_level_end;
        uint8_t byte_current = key[key_level_start]; // separator key for the current node
        Node* node_child = node_current->get_child(byte_current);
        node_current->latch_validate(version_current); // still valid?

        if(node_child == nullptr){ // the slot `byte_current' is empty => insert into node_current
            do_insert_and_grow(
                    node_parent, byte_parent, version_parent,
                    node_current, byte_current, version_current,
                    element
            );
            return; // done
        } else if(is_leaf(node_child)){
            // the slot `byte_current' is occupied by a leaf, we need to create a new inner node to separate between the new element and the leaf already present
            node_current->latch_upgrade_to_write_lock(version_current);

            Key key_sibling = node2leaf(node_child)->m_key;

            key_level_start++;
            int prefix_length = 0;
            while(key[key_level_start + prefix_length] == key_sibling[key_level_start + prefix_length]) prefix_length++;

            N4* node_new = new N4(&key[key_level_start], prefix_length);
            node_new->insert(key[key_level_start + prefix_length], leaf2node(element));
            node_new->insert(key_sibling[key_level_start + prefix_length], node_child);
            node_current->change(byte_current, node_new);
            node_current->latch_write_unlock();
            return; // done
        }

        // keep traversing the trie
        key_level_start++;
        node_parent = node_current; byte_parent = byte_current; version_parent = version_current;
        node_current = node_child;
    } while (true);
}

void Index::do_insert_and_grow(Node* node_parent, uint8_t key_parent, uint64_t version_parent, Node* node_current, uint8_t key_current, uint64_t version_current, Leaf* new_element){
    assert((node_parent == nullptr || !is_leaf(node_parent)) && "It must be an inner node");
    assert(!is_leaf(node_current) && "It must be an inner node");


//    assert((node_parent == nullptr || node_parent->get_child(key_parent) == node_current) && "Invalid key_parent");
//    assert(node_current->get_child(key_current) == nullptr && "node_current already contains a child for `key_current'");

    if(node_current->is_overfilled()){ // there is no space in the current node for a new child, expand it
        assert(node_parent != nullptr && "node_parent can be null only iff node_current is the root of the tree, which will never be overfilled");

        // acquire the latch to both the parent and the child
        node_parent->latch_upgrade_to_write_lock(version_parent);
        try {
            node_current->latch_upgrade_to_write_lock(version_current);
        } catch (Abort) {
            node_parent->latch_write_unlock();
            throw;
        }

        assert(node_current->get_type() != NodeType::N256);


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

        node_current->latch_write_lock(); // always successful, this is the only ptr to node_current

        // update the ptr from the old to the new inner node
        node_parent->change(key_parent, node_current);
        node_parent->latch_write_unlock(); // done with the parent

        node_old->latch_invalidate();
        mark_node_for_gc(node_old);

    } else {
        node_parent->latch_validate(version_parent);
        node_current->latch_upgrade_to_write_lock(version_current);
    }

    node_current->insert(key_current, leaf2node(new_element));
    node_current->latch_write_unlock(); // done
}

bool Index::remove(uint64_t src, uint64_t dst){
    Key key(src, dst);
    bool result { false };
    bool done { false };
    do {
        try {
            result = do_remove(nullptr, 0, 0, m_root, key, 0);
            done = true;
        } catch (Abort) {
            /* try again */
        }
    } while( !done );

    return result;
}

bool Index::do_remove(Node* node_parent, uint8_t byte_parent, uint64_t version_parent, Node* node_current, const Key& key, int key_level_start){
    do {
        uint64_t version_current = node_current->latch_read_lock();

        // first check whether the prefix matches our key
        int key_level_end = 0; // up to where the current node matches the key, excl
        if( !node_current->prefix_match_approximate(key, key_level_start, &key_level_end) == -1 /* no match */){
            node_current->latch_read_unlock(version_current); // still valid?
            return false; // we didn't remove nothing
        }

        key_level_start = key_level_end;
        uint8_t byte_current = key[key_level_start]; // separator key for the current node
        Node* node_child = node_current->get_child(byte_current);
        node_current->latch_validate(version_current); // is what we read still valid?
        if( node_child == nullptr ) return false; // no match on the indexed byte

        if( is_leaf(node_child) ){ // our candidate leaf
            auto leaf = node2leaf(node_child);
            if( leaf->m_key != key ) return false; // not found!

            // if the current node is a N4 with only 1 child, remove it
            if(node_current->count() == 2 && node_parent != nullptr){
                assert(node_current->get_type() == NodeType::N4);

                // acquire the latches to both the parent and the current
                node_parent->latch_upgrade_to_write_lock(version_parent);
                try {
                    node_current->latch_upgrade_to_write_lock(version_current);
                } catch( Abort ){
                    node_parent->latch_write_unlock();
                    throw;
                }



                // move the other sibling
                uint8_t byte_second; Node* node_second {nullptr};
                std::tie(byte_second, node_second) = reinterpret_cast<N4*>(node_current)->get_other_child(byte_second);

                if(is_leaf(node_second)){
                    node_parent->change(byte_parent, node_second);
                    node_parent->latch_write_unlock();
                    node_current->latch_invalidate();

                } else {
                    try {
                        node_second->latch_write_lock();
                    } catch (Abort){
                        node_current->latch_write_unlock();
                        node_parent->latch_write_unlock();
                        throw;
                    }

                    node_parent->change(byte_parent, node_second);
                    node_parent->latch_write_unlock();

                    node_second->prefix_prepend(node_current, byte_second);
                    node_second->latch_write_unlock();
                }

                node_current->latch_invalidate();
                mark_node_for_gc(node_current);
            } else { // standard case
                do_remove_and_shrink(node_parent, byte_parent, version_parent, node_current, byte_current, version_current);
            }


            return true;
        } else { // keep traversing
            key_level_start++;

            node_parent = node_current; byte_parent = byte_current;
            node_current = node_child->m_child;
        }

    } while( true );
}

bool Index::do_remove_and_shrink(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, NodeEntry* out_entry_removed){
    assert((node_parent == nullptr || !is_leaf(node_parent)) && "node_parent must be an inner node");
    assert((node_parent == nullptr || node_parent->get_child(key_parent)->m_child == node_current) && "Invalid key_parent");

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

void* Index::get_value_by_real_id(uint64_t vertex_id) const {
    Key key(vertex_id);
    do {
        try {
            return find_btree_leaf_by_vertex_id_leq(m_latch.read_version(), key, m_root, 0);
        } catch (Abort) { /* retry */ }
    } while (true);
}


void* Index::find_btree_leaf_by_vertex_id_leq(uint64_t latch_version, const Key& key, Node* node, int level) const {
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
    Node* child; bool exact_match;
    std::tie(child, exact_match) = node->find_node_leq(key[level]);
    m_latch.validate_version(latch_version);

    if(child == nullptr){
        return nullptr; // again, ask the parent to return the maximum of the previous sibling
    } else if (exact_match || is_leaf(child) ){
        if(is_leaf(child)){
            auto leaf = node2leaf(child);
            m_latch.validate_version(latch_version);
            if(leaf->m_vertex_id <= key.get_vertex_id()) return leaf->m_btree_leaf_address;

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
                auto leaf = node2leaf(sibling);

                // last check
                m_latch.validate_version(latch_version);

                return leaf->m_btree_leaf_address;
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

void* Index::get_max_leaf_address(uint64_t latch_version, Node* node) const {
    m_latch.validate_version(latch_version);
    while(!is_leaf(node)){
        Node* child = node->max();

        // validate what we read is correct
        m_latch.validate_version(latch_version);

        // next iteration
        node = child;
    }

    auto leaf = node2leaf(node);
    m_latch.validate_version(latch_version);
    return leaf->m_btree_leaf_address;
}

Index::Node* Index::leaf2node(Leaf* leaf){
    return reinterpret_cast<Node*>(reinterpret_cast<uint64_t>(leaf) | (1ull<<63));
}

Index::Leaf* Index::node2leaf(Node* node){
    assert(is_leaf(node));
    return reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63)));;
}

bool Index::is_leaf(Node* node){
    return reinterpret_cast<uint64_t>(node) & (1ull<<63);
}


void Index::mark_node_for_gc(Node* node){
    if(!is_leaf(node)){
        GlobalContext::context()->gc()->mark(node);
    } else { // the node is a leaf
        GlobalContext::context()->gc()->mark(reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63))));
    }
}

void Index::delete_nodes_rec(Node* node){
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

void Index::dump() const {
    Node::dump(cout, m_root, 0, 0);
}

/*****************************************************************************
 *                                                                           *
 *  Encoded keys (vertex ids)                                                *
 *                                                                           *
 *****************************************************************************/

Index::Key::Key(uint64_t key) : Key(key, 0){ }

Index::Key::Key(uint64_t src, uint64_t dst){
    // in Intel x86, 64 bit integers are stored reversed in memory due to little endiannes
    uint8_t* __restrict src_le = reinterpret_cast<uint8_t*>(src);
    for(int i = 0; i < 8; i++){ m_data[i] = src_le[7 - i]; }
    uint8_t* __restrict dst_le = reinterpret_cast<uint8_t*>(dst);
    for(int i = 8; i < 16; i++){ m_data[i] = dst_le[7 - i]; }
}

uint8_t& Index::Key::operator[](uint32_t i){
    assert(i < length() && "Overflow");
    return m_data[i];
}

const uint8_t& Index::Key::operator[](uint32_t i) const{
    assert(i < length() && "Overflow");
    return m_data[i];
}

int Index::Key::length() const {
    static_assert(MAX_LENGTH == 8);
    return MAX_LENGTH;
}

uint64_t Index::Key::get_source() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 0; i < 8; i++){ key_be[i] = m_data[7 - i]; }
    return vertex_id;
}

uint64_t Index::Key::get_destination() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 8; i < 15; i++){ key_be[i] = m_data[7 - i]; }
    return vertex_id;
}

bool Index::Key::operator==(const Key& other) const {
    // all keys should be 16 bytes in this implementation
    assert(length() == 16 && "[this] All keys should have a length of 16 bytes");
    assert(other.length() == 16 && "[other] All keys should have a length of 16 bytes");
    uint64_t this_p1 = *reinterpret_cast<uint64_t*>(m_data);
    uint64_t this_p2 = *reinterpret_cast<uint64_t*>(m_data + 8);
    uint64_t other_p1 = *reinterpret_cast<uint64_t*>(other.m_data);
    uint64_t other_p2 = *reinterpret_cast<uint64_t*>(other.m_data + 8);

    return this_p1 == other_p1 && this_p2 == other_p2;
}

bool Index::Key::operator!=(const Key& other) const {
    return !((*this) == other);
}

std::ostream& operator<<(std::ostream& out, const Index::Key& key){
    out << "{KEY: " << key.get_source() << " -> " << key.get_destination() << ", bytes={";
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

Index::NodeType Index::Node::get_type() const{
    return reinterpret_cast<NodeType>(m_latch.get_payload());
}

Index::Node::Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length) : m_count(0) {
    set_type(type);
    set_prefix(prefix, prefix_length);
}

void Index::Node::set_type(NodeType type){
    m_latch.set_payload((int) type);
}

void Index::Node::latch_validate(uint64_t version) const {
    m_latch.validate_version(version);
}

uint64_t Index::Node::latch_read_lock() const {
    return m_latch.read_version();
}

void Index::Node::latch_read_unlock(uint64_t version) const {
    latch_validate(version);
}

void Index::Node::latch_upgrade_to_write_lock(uint64_t version){
    m_latch.update(version);
}

void Index::Node::latch_write_lock() {
    m_latch.lock();
}

void Index::Node::latch_write_unlock() {
    m_latch.unlock();
}

void Index::Node::latch_invalidate(){
    m_latch.invalidate();
}

int Index::Node::count() const {
    return m_count;
}

uint8_t* Index::Node::get_prefix() {
    return reinterpret_cast<uint8_t*>(m_prefix);
}

const uint8_t* Index::Node::get_prefix() const {
    return reinterpret_cast<const uint8_t*>(m_prefix);
}

int Index::Node::get_prefix_length() const {
    return m_prefix_sz;
}

bool Index::Node::has_prefix() const {
    return get_prefix_length() > 0;
}

void Index::Node::set_prefix(const uint8_t* prefix, uint32_t length) {
    assert(length <= numeric_limits<uint8_t>::max() && "Overflow");
    memcpy(m_prefix, prefix, std::min<int>(length, MAX_PREFIX_LEN));
    m_prefix_sz = static_cast<uint8_t>(length);
}

void Index::Node::prefix_prepend(Node* first_part, uint8_t second_part){
    assert(first_part != nullptr && !is_leaf(first_part));
    int num_bytes_to_prepend = std::min(MAX_PREFIX_LEN, first_part->get_prefix_length() + 1);
    memmove(/* to */ m_prefix + num_bytes_to_prepend, /* from */ m_prefix, std::min(get_prefix_length(), MAX_PREFIX_LEN - num_bytes_to_prepend));
    memcpy(/* to */ m_prefix, first_part->get_prefix(), std::min(num_bytes_to_prepend, first_part->get_prefix_length()));
    if (first_part->get_prefix_length() < MAX_PREFIX_LEN) { m_prefix[num_bytes_to_prepend - 1] = second_part; }
    this->m_prefix_sz += first_part->get_prefix_length() + 1;
}

bool Index::Node::prefix_match_exact(const Key& key, int prefix_start, int* out_prefix_end, uint8_t** out_non_matching_prefix, int* out_non_matching_length) const {
    int i, j, prefix_length = get_prefix_length();
    uint8_t* prefix = get_prefix();
    for(int i = 0, j = prefix_start; i < prefix_length; i++, j++){
        if(i == MAX_PREFIX_LEN){ // we need to retrieve the full prefix from one of the leaves
            prefix = get_any_child()->m_key.data() + prefix_start;
        }

        if(key[j] != prefix[i]){ // the prefix does not match the given key
            if(out_prefix_end != nullptr) { *out_prefix_end = j; }
            if(out_non_matching_prefix != nullptr){
                if(prefix_length > MAX_PREFIX_LEN && i < MAX_PREFIX_LEN){
                    prefix = get_any_child()->m_key.data() + prefix_start;
                }

                memcpy(*out_non_matching_prefix, prefix + i, prefix_length -i);
            }
            if(out_non_matching_length != nullptr){
                *out_non_matching_length = prefix_length - i;
            }
            return false;
        }
    }

    return true;
}


int Index::Node::prefix_match_approximate(const Key& key, int prefix_start, int* out_prefix_end) const {
    // -1 = no match, 0 = maybe, 1 = match
    if(out_prefix_end != nullptr) *out_prefix_end = prefix_start; // init
    int j = prefix_start;

    assert((key.length() >= prefix_start + get_prefix_length()) && "Because all keys have the same length, 16 bytes");

    uint8_t* __restrict prefix = get_prefix();
    for(int i = 0, sz = std::min(get_prefix_length(), MAX_PREFIX_LEN); i < sz; i++){
        if(prefix[i] != key[j]){
            if(out_prefix_end != nullptr) *out_prefix_end = j;
            return -1; // no match
        }
        j++;
    }

    if(out_prefix_end != nullptr){
        *out_prefix_end = prefix_start + get_prefix_length();
    }

    if(get_prefix_length() > MAX_PREFIX_LEN){
        return 0; // maybe
    } else {
        return 1; // they are equal
    }
}


int Index::Node::prefix_compare(const Key& search_key, int& /* in/out */ search_key_level) const {
    if(!has_prefix()) return 0; // the current doesn't have a prefix => technically it matches

    uint8_t* __restrict prefix = get_prefix();
    const int prefix_start = search_key_level;

    for(int i = 0, prefix_length = get_prefix_length(); i < prefix_length; i++){
        if (i == MAX_PREFIX_LEN) {
            Leaf* leaf = get_any_child();
            assert(search_key.length() == leaf->m_key.length() && "All keys should have the same length");
            prefix = leaf->m_key.data() + prefix_start;
        }

        uint8_t byte_prefix = prefix[i];
        uint8_t byte_search_key = search_key[search_key_level];

        if (byte_prefix < byte_search_key) {
            return -1;
        } else if (byte_prefix > byte_search_key) {
            return +1;
        }

        search_key_level++;
    }

    return 0;
}

bool Index::Node::change(uint8_t key, Node* value){
    Node** slot { nullptr };
    switch (get_type()) {
        case NodeType::N4:
            slot = reinterpret_cast<N4 *>(this)->get_child_ptr(key);
            break;
        case NodeType::N16:
            slot = reinterpret_cast<N16 *>(this)->get_child_ptr(key);
            break;
        case NodeType::N48:
            slot = reinterpret_cast<N48 *>(this)->get_child_ptr(key);
            break;
        case NodeType::N256:
            slot = reinterpret_cast<N256 *>(this)->get_child_ptr(key);
            break;
    }

    if (slot == nullptr) return false;

    *slot = value;
    return true;
}

bool Index::Node::is_overfilled() const{
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<N4 *>(this)->is_overfilled();
        case NodeType::N16:
            return reinterpret_cast<N16 *>(this)->is_overfilled();
        case NodeType::N48:
            return reinterpret_cast<N48 *>(this)->is_overfilled();
        case NodeType::N256:
            return reinterpret_cast<N256 *>(this)->is_overfilled();
        default:
            assert(0 && "Invalid case");
            return false;
    }
}

bool Index::Node::is_underfilled() const{
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<N4 *>(this)->is_underfilled();
        case NodeType::N16:
            return reinterpret_cast<N16 *>(this)->is_underfilled();
        case NodeType::N48:
            return reinterpret_cast<N48 *>(this)->is_underfilled();
        case NodeType::N256:
            return reinterpret_cast<N256 *>(this)->is_underfilled();
        default:
            assert(0 && "Invalid case");
            return false;
    }
}

void Index::Node::insert(uint8_t key, const Node* child){
    switch (get_type()) {
        case NodeType::N4:
            reinterpret_cast<N4 *>(this)->insert(key, child); break;
        case NodeType::N16:
            reinterpret_cast<N16 *>(this)->insert(key, child); break;
        case NodeType::N48:
            reinterpret_cast<N48 *>(this)->insert(key, child); break;
        case NodeType::N256:
            reinterpret_cast<N256 *>(this)->insert(key, child); break;
    }
}


Index::Node* Index::Node::get_child(uint8_t key) const {
    Node* ptr_this = const_cast<Node*>(this);
    Node** ptr_node {nullptr};
    switch (get_type()) {
        case NodeType::N4:
            ptr_node = reinterpret_cast<N4 *>(ptr_this)->get_child_ptr(key);
            break;
        case NodeType::N16:
            ptr_node = reinterpret_cast<N16 *>(ptr_this)->get_child_ptr(key);
            break;
        case NodeType::N48:
            ptr_node = reinterpret_cast<N48 *>(ptr_this)->get_child_ptr(key);
            break;
        case NodeType::N256:
            ptr_node = reinterpret_cast<N256 *>(ptr_this)->get_child_ptr(key);
            break;
    }

    return ptr_node != nullptr ? *ptr_node : nullptr;
}
Index::Leaf* Index::Node::get_any_child() const {
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<N4 *>(this)->get_any_child();
        case NodeType::N16:
            return reinterpret_cast<N16 *>(this)->get_any_child();
        case NodeType::N48:
            return reinterpret_cast<N48 *>(this)->get_any_child();
        case NodeType::N256:
            return reinterpret_cast<N256 *>(this)->get_any_child();
    }
}

std::pair<Index::Node*, /* exact match ? */ bool> Index::Node::find_node_leq(uint8_t key) const {
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<N4 *>(this)->find_node_leq(key);
        case NodeType::N16:
            return reinterpret_cast<N16 *>(this)->find_node_leq(key);
        case NodeType::N48:
            return reinterpret_cast<N48 *>(this)->find_node_leq(key);
        case NodeType::N256:
            return reinterpret_cast<N256 *>(this)->find_node_leq(key);
    }
}

Index::Leaf* Index::Node::get_max_leaf(Node* node, uint64_t node_version) {
    assert(node != nullptr);

    while(!is_leaf(node)){
        Node* child = nullptr;
        switch (node->get_type()) {
        case NodeType::N4:
            child = static_cast<N4*>(node)->get_max_child();
            break;
        case NodeType::N16:
            child = static_cast<N16 *>(node)->get_max_child();
            break;
        case NodeType::N48:
            child = static_cast<N48 *>(node)->get_max_child();
            break;
        case NodeType::N256:
            child = static_cast<N256 *>(node)->get_max_child();
            break;
        }

        // validate what we read is still valid
        node->latch_validate(node_version);

        // acquire the lock for the leaf
        assert(child != nullptr);
        uint64_t child_version = 0;
        if(!is_leaf(child)){
            child_version = child->latch_read_lock();
        }

        // finally, release the current node
        node->latch_read_unlock(node_version);

        // next iteration
        node = child;
        node_version = child_version;
    }

    return node2leaf(node);
}

Index::Node* Index::Node::get_predecessor(uint8_t key) const {
    return (key > 0) ? find_node_leq(key -1).first : nullptr;
}

static void print_tabs(std::ostream& out, int depth){
    auto flags = out.flags();
    out << setw(depth * 4) << setfill(' ') << ' ';
    out.setf(flags);
}

void Index::Node::dump(std::ostream& out, Node* node, int level, int depth) {
    assert(node != nullptr);

    print_tabs(out, depth);

    if(is_leaf(node)){
        auto leaf = node2leaf(node);
        out << "Leaf: " << node << ", key: " << leaf->m_key.get_source() << " -> " << leaf->m_key.get_destination() << ", value: " << (uint64_t) leaf->m_btree_leaf_address << " (" << leaf->m_btree_leaf_address << ")\n";
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
        print_tabs(out, depth);
        out << "Children: " << node->count();
        for(int i = 0; i <= 255; i++){
            Node* child = node->get_child(i);
            if(child == nullptr) continue;
            out << ", {byte:" << static_cast<int>(i) << ", pointer:" << child << "}";
        }
        out << "\n";

        // recursively dump the children
        for(int i = 0; i < node->count(); i++){
            Node* child = node->get_child(i);
            if(child == nullptr) continue;
            dump(out, child, level + 1 + node->get_prefix_length(), depth + 1);
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *  Node4                                                                    *
 *                                                                           *
 *****************************************************************************/

Index::N4::N4(const uint8_t *prefix, uint32_t prefix_length) : Node(NodeType::N4, prefix, prefix_length){

}


void Index::N4::insert(uint8_t key, const Node* value) {
    int pos;
    for(pos = count(); pos > 0 && m_keys[pos -1] > key; pos--){ // shift larger keys to the right
        m_keys[pos] = m_keys[pos -1];
        m_children[pos] = m_children[pos -1];
    }

    m_keys[pos] = key;
    m_children[pos] = value;
    m_count++;
}

bool Index::N4::remove(uint8_t key){
    for (int i = 0, count = count(); i < count; i++) {
        if (m_keys[i] == key) {
            mark_node_for_gc(m_children[i]); m_children[i] = nullptr;

            memmove(m_keys + i, m_keys + i + 1, count - i - 1);
            memmove(m_children + i, m_children + i + 1, (count - i - 1) * sizeof(Node*));
            m_count--;
            return true;
        }
    }

    return false;
}

Index::Node** Index::N4::get_child_ptr(uint8_t byte) {
    for (int i = 0, end = m_count; i < end; i++) {
        if(m_keys[i] == byte)
            return m_children + i;
    }

    return nullptr;
}

Index::Node* Index::N4::get_max_child() const {
    assert(count() > 0 && "empty node?");
    return m_children[count() -1];
}

tuple</* key */ uint8_t, /* entry */ Index::Node*> Index::N4::get_other_child(uint8_t key) const {
    for(int i = 0, sz = count(); i < sz; i++){
        if(m_keys[i] == key){
            return std::make_tuple(m_keys[i], m_children[i]);
        }
    }

    return std::make_tuple(0, nullptr);
}

pair<Index::Node*, /* exact match ? */ bool> Index::N4::find_node_leq(uint8_t key) const {
    int i = count() -1;
    while(i >= 0 && m_keys[i] > key) i--;
    if(i < 0){ // no match!
        return pair<Node*, /* exact match ? */ bool>{ nullptr, false };
    } else { // i >= 0, match
        return pair<Node*, /* exact match ? */ bool>{ m_children[i], /* exact match ? */ (key == m_keys[i]) };
    }
}

bool Index::N4::is_overfilled() const {
    return count() == 4;
}

bool Index::N4::is_underfilled() const {
    return false;
}

Index::N16* Index::N4::to_N16() const {
    N16* new_node = new N16(get_prefix(), get_prefix_length());
    for(int i = 0; i < count(); i++){
        new_node->insert(m_keys[i], m_children[i]);
    }
    return new_node;
}


/*****************************************************************************
 *                                                                           *
 *  Node16                                                                   *
 *                                                                           *
 *****************************************************************************/
Index::N16::N16(const uint8_t* prefix, uint32_t prefix_length):
           Node(NodeType::N16, prefix, prefix_length) {
    memset(m_keys, 0, sizeof(m_keys));
    memset(m_children, 0, sizeof(m_children));
}

void Index::N16::insert(uint8_t key, const Node* value) {
    uint8_t keyByteFlipped = flip_sign(key);
    __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped), _mm_loadu_si128(reinterpret_cast<__m128i *>(m_keys)));
    uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - count()));
    unsigned pos = bitfield ? ctz(bitfield) : count();
    memmove(m_keys + pos + 1, m_keys + pos, count() - pos);
    memmove(m_children + pos + 1, m_children + pos, (count() - pos) * sizeof(Node*));
    m_keys[pos] = keyByteFlipped;
    m_children[pos] = value;
    m_count++;
}

bool Index::N16::remove(uint8_t key){
    Node* node = get_child(key);
    if(node == nullptr) return false;
    mark_node_for_gc(node);

    std::size_t pos = node - m_children;
    memmove(m_keys + pos, m_keys + pos + 1, count() - pos - 1);
    memmove(m_children + pos, m_children + pos + 1, (count() - pos - 1) * sizeof(Node*));
    m_count--;

    assert(get_child(key) == nullptr);
    return true;
}

Index::Node** Index::N16::get_child_ptr(uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flip_sign(k)),
                                 _mm_loadu_si128(reinterpret_cast<const __m128i *>(m_keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << count()) - 1);

    if(bitfield){
        return &m_children[ctz(bitfield)];
    } else {
        return nullptr;
    }
}

pair<Index::Node*, /* exact match ? */ bool> Index::N16::find_node_leq(uint8_t key_unsigned) const {
    assert(count() > 0 && "Empty node!");

    uint8_t key_signed = flip_sign(key_unsigned);
    auto lhs = _mm_set1_epi8(key_signed);
    auto rhs = _mm_loadu_si128(reinterpret_cast<const __m128i *>(m_keys));
    auto cmp = _mm_cmplt_epi8(lhs, rhs);
    unsigned mask = (1U << count());
    unsigned bitfield = _mm_movemask_epi8(cmp) | mask;
    assert(bitfield && "Expected due to the OR with (1 << count)");
    unsigned index = __builtin_ctz(bitfield);
    if(index > 0){
        index--;
        return pair<Node*, bool>{m_children[index], m_keys[index] == key_signed};
    } else { // all elements are greater than key_signed
        return pair<Node*, bool>{nullptr, false};
    }
}

Index::Node* Index::N16::get_max_child() const {
    assert(count() > 0 && "empty node?");
    return m_children[count() -1];
}

bool Index::N16::is_overfilled() const {
    return count() == 16;
}

bool Index::N16::is_underfilled() const {
    return count() <= 3;
}

uint8_t Index::N16::flip_sign(uint8_t byte) {
    return byte ^ 128;
}

unsigned Index::N16::ctz(uint16_t value){
    return __builtin_ctz(value);
}

Index::N4* Index::N16::to_N4() const {
    if(count() > 4) RAISE(InternalError, "N16 cannot shrink to N4, the number of children is : " << count());

    N4* new_node = new N4(get_prefix(), get_prefix_length());
    for(int i = 0; i < count(); i++){
        new_node->insert(m_keys[i], m_children[i]);
    }
    return new_node;
}

Index::N48* Index::N16::to_N48() const {
    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0; i < count(); i++){
        new_node->insert(m_keys[i], m_children[i]);
    }
    return new_node;
}

/*****************************************************************************
 *                                                                           *
 *  Node48                                                                   *
 *                                                                           *
 *****************************************************************************/
Index::N48::N48(const uint8_t* prefix, uint32_t prefix_length) :
        Node(NodeType::N48, prefix, prefix_length) {
    memset(m_child_index, EMPTY_MARKER, sizeof(m_child_index));
    memset(m_children, 0, sizeof(m_children));
}

void Index::N48::insert(uint8_t key, const Node* value){
    assert(!is_overfilled() && "This node is full");

    int pos = count();
    if (m_children[pos] == nullptr) {
        for (pos = 0; m_children[pos] != nullptr; pos++);
    }
    m_children[pos] = value;
    m_child_index[key] = (uint8_t) pos;

    m_count++;
}

bool Index::N48::remove(uint8_t byte){
    if(m_child_index[byte] == EMPTY_MARKER) return false;
    auto& entry = m_children[m_child_index[byte]];
    mark_node_for_gc(entry);
    m_child_index[byte] = EMPTY_MARKER;
    m_count--;

    assert(get_child(byte) == nullptr);
    return true;
}

Index::Node** Index::N48::get_child_ptr(const uint8_t k) {
    if (m_child_index[k] == EMPTY_MARKER) {
        return nullptr;
    } else {
        return &m_children[m_child_index[k]];
    }
}

pair<Index::Node*, /* exact match ? */ bool> Index::N48::find_node_leq(uint8_t key) const {
    int index = key; // convert the type
    bool exact_match = m_child_index[index] != EMPTY_MARKER;
    if(exact_match){
        return pair<Node*, bool>{m_children[m_child_index[index]], exact_match};
    } else {
        index--;
        while(index >= 0){
            if(m_child_index[index] != EMPTY_MARKER){
                return pair<Node*, bool>{m_children[m_child_index[index]], exact_match};
            }
            index--;
        }
        return pair<Node*, bool>{nullptr, exact_match};
    }
}

Index::Node* Index::N48::get_max_child() const {
    for (int i = 255; i >= 0; i--) {
        if(m_child_index[i] != EMPTY_MARKER){
            return m_children[m_child_index[i]];
        }
    }

    assert(0 && "This code should be unreachable!");
    return nullptr;
}

bool Index::N48::is_overfilled() const {
    return count() == 48;
}

bool Index::N48::is_underfilled() const {
    return count() <= 12;
}

Index::N16* Index::N48::to_N16() const {
    if(count() > 16) RAISE(InternalError, "N48 cannot shrink to N16, the number of children is : " << count());

    N16* new_node = new N16(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_child_index[i] != EMPTY_MARKER){
            new_node->insert((uint8_t) i, m_children[m_child_index[i]]);
        }
    }
    return new_node;
}

Index::N256* Index::N48::to_N256() const {
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
Index::N256::N256(const uint8_t* prefix, uint32_t prefix_length) : Node(NodeType::N256, prefix, prefix_length){
    memset(m_children, '\0', sizeof(m_children));
}

void Index::N256::insert(uint8_t byte, const Node* value){
    assert(m_children[byte] == nullptr && "Slot already occupied");
    m_children[byte] = value;
    m_count++;
}

bool Index::N256::remove(uint8_t key){
    if(m_children[key] == nullptr) return false;
    mark_node_for_gc( m_children[key] );
    m_children[key] = nullptr;
    m_count--;
    return true;
}

Index::Node** Index::N256::get_child_ptr(uint8_t byte) {
    return (m_children[byte] != nullptr) ? &(m_children[byte]) : nullptr;
}

pair<Index::Node*, /* exact match ? */ bool> Index::N256::find_node_leq(uint8_t key) const {
    int index = key; // convert the type

    bool exact_match = m_children[index] != nullptr;
    if(exact_match){
        return pair<Node*, bool>{m_children[index], exact_match};
    } else {
        index--;
        while(index >= 0){
            if(m_children[index] != nullptr){
                return pair<Node*, bool>{m_children[index], exact_match};
            }
            index--;
        }

        return pair<Node*, bool>{nullptr, exact_match};
    }
}

Index::Node* Index::N256::get_max_child() const {
    for (int i = 255; i >= 0; i--) {
        if(m_children[i] != nullptr){
            return m_children[i];
        }
    }

    assert(0 && "This code should be unreachable!");
    return nullptr;
}

bool Index::N256::is_overfilled() const{
    return false;
}

bool Index::N256::is_underfilled() const {
    return count() <= 37;
}

Index::N48* Index::N256::to_N48() const {
    if(count() > 48) RAISE(InternalError, "N256 cannot shrink to N48, the number of children is : " << count());

    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_children[i] != nullptr){
            new_node->insert((uint8_t) i, m_children[i]);
        }
    }

    return new_node;
}

} // namespace