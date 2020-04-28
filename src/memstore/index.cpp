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

#include "teseo/memstore/index.hpp"

#include <cassert>
#include <cstring>
#include <emmintrin.h> // x86 SSE intrinsics
#include <limits>
#include <iomanip>
#include <mutex>
#include <smmintrin.h> // SSE 4.1

#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/util/error.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/

ostream& operator<<(ostream& out, const Index::NodeType& type){
    switch(type){
    case Index::NodeType::N4:
        out << "N4"; break;
    case Index::NodeType::N16:
        out << "N16"; break;
    case Index::NodeType::N48:
        out << "N48"; break;
    case Index::NodeType::N256:
        out << "N256"; break;
    }
    return out;
}

/*****************************************************************************
 *                                                                           *
 *  Index                                                                    *
 *                                                                           *
 *****************************************************************************/

Index::Index(){
    m_root = new Index::N256(nullptr, 0);
    m_size = 0;
}

Index::~Index(){
    delete_nodes_rec(m_root);
    delete m_root; m_root = nullptr;
}

uint64_t Index::size() const {
    return m_size;
}

bool Index::empty() const {
    return size() == 0;
}

void Index::insert(uint64_t src, uint64_t dst, Value value){

    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "It should have already entered an epoch");
    assert(find(src, dst) != value && "The search key already exists");

    Leaf* element = new Leaf{ Key{src, dst}, value };
    COUT_DEBUG("key: " << src << " -> " << dst << ", value: " << value << ", leaf: " << element);
    bool done = false;
    do {

        try {
            // assume that duplicates are never inserted
            do_insert(nullptr, 0, 0, m_root, element, 0);

            m_size++;
            done = true;
        } catch (Abort){
             // try again
        }
    } while (!done);

    assert(find(src, dst) == value);
}

void Index::do_insert(Node* node_parent, uint8_t byte_parent, uint64_t version_parent, Node* node_current, Leaf* element, int key_level_start){
    assert(node_current != nullptr && "No starting node given");
    assert((node_parent != nullptr || node_current == m_root) && "Isolated node");
    assert(((node_parent == nullptr) || (node_parent->get_child(byte_parent) == node_current)) && "byte_parent does not match the current node");

    uint8_t non_matching_prefix[Key::MAX_LENGTH];
    uint8_t* ptr_non_matching_prefix = non_matching_prefix;
    int non_matching_length = 0;
    auto& key = element->m_key;

    do {
        uint64_t version_current = node_current->latch_read_lock();
        COUT_DEBUG("[iteration start] node_parent: " << node_parent << ", byte_parent: " << (int) byte_parent << ", version_parent: " << version_parent << ", node_current: " << node_current);


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

            COUT_DEBUG("prefix mismatch, create a new N4 node under " << node_parent << " at byte: " << (int) key[key_level_start -1] << ", node_new: " << node_new << ", leaf_new: " << leaf2node(element) << ", sibling: " << node_parent->get_child(key[key_level_start -1]));

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
            COUT_DEBUG("standard case");
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
            assert(key != key_sibling && "Duplicate key");

            key_level_start++;
            int prefix_length = 0;
            while(key[key_level_start + prefix_length] == key_sibling[key_level_start + prefix_length]) prefix_length++;

            N4* node_new = new N4(&key[key_level_start], prefix_length);
            node_new->insert(key[key_level_start + prefix_length], leaf2node(element));
            node_new->insert(key_sibling[key_level_start + prefix_length], node_child);

            COUT_DEBUG("conflict, create a new N4 node under " << node_parent << " at byte: " << (int) key[key_level_start -1] << ", node_new: " << node_new << ", leaf 1 (new element): " << leaf2node(element) << ", leaf 2 (existing element): "  << node_child);
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

        COUT_DEBUG("replace " << node_old << "(" << node_old->get_type() << ") with " << node_current << "(" << node_current->get_type() << "), parent: " << node_parent << " at byte " << (int) key_parent);

        node_current->latch_write_lock(); // always successful, this is the only ptr to node_current

        // update the ptr from the old to the new inner node
        node_parent->change(key_parent, node_current);
        node_parent->latch_write_unlock(); // done with the parent

        node_old->latch_invalidate();
        mark_node_for_gc(node_old);

    } else { // the there is still space in the child
        if(node_parent != nullptr) { node_parent->latch_validate(version_parent); }
        node_current->latch_upgrade_to_write_lock(version_current);
    }

    COUT_DEBUG("insert into " << node_current << " (" << node_current->get_type() << ") at byte " << (int) key_current << " the leaf (new element): " << leaf2node(new_element));
    node_current->insert(key_current, leaf2node(new_element));
    node_current->latch_write_unlock(); // done
}

bool Index::remove(uint64_t src, uint64_t dst){
    COUT_DEBUG(src << " -> " << dst);
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "It should have already entered an epoch");

    Key key(src, dst);
    bool result { false };
    bool done { false };
    do {
        try {
            result = do_remove(nullptr, 0, 0, m_root, key, 0);
            if(result == true) { m_size--; } // key removed?
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
        COUT_DEBUG("[iteration_start] node parent: "  << node_parent << ", byte parent: " << (int) byte_parent << ", version parent: " << version_parent << ", node current: " << node_current);

        // first check whether the prefix matches our key
        int key_level_end = 0; // up to where the current node matches the key, excl
        if( node_current->prefix_match_approximate(key, key_level_start, &key_level_end) == -1 /* no match */){
            node_current->latch_read_unlock(version_current); // still valid?
            return false; // we didn't remove nothing
        }

        key_level_start = key_level_end;
        uint8_t byte_current = key[key_level_start]; // separator key for the current node
        Node* node_child = node_current->get_child(byte_current);
        COUT_DEBUG("byte_current: " << (int) byte_current << ", node child: " << node_child );
        node_current->latch_validate(version_current); // is what we read still valid?
        if( node_child == nullptr ) return false; // no match on the indexed byte


        if( is_leaf(node_child) ){ // our candidate leaf
            auto leaf = node2leaf(node_child);
            if( leaf->m_key != key ) return false; // not found!
            COUT_DEBUG("node child is a leaf");

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
                std::tie(byte_second, node_second) = reinterpret_cast<N4*>(node_current)->get_other_child(byte_current);
                assert(node_second != nullptr);

                if(is_leaf(node_second)){
                    COUT_DEBUG("replace node " <<  node_current << " (" << node_current->get_type() << ") at byte << " << (int) byte_parent << " with leaf " << node_second);

                    node_parent->change(byte_parent, node_second);
                    node_parent->latch_write_unlock();
                    node_current->latch_invalidate();

                } else {
                    try {
                        node_second->latch_write_lock();
                    } catch ( Abort ){
                        node_current->latch_write_unlock();
                        node_parent->latch_write_unlock();
                        throw;
                    }

                    COUT_DEBUG("replace node " <<  node_current << " (" << node_current->get_type() << ") at byte << " << (int) byte_parent << " with node " << node_second);

                    node_parent->change(byte_parent, node_second);
                    node_parent->latch_write_unlock();

                    node_second->prefix_prepend(node_current, byte_second);
                    node_second->latch_write_unlock();
                }

                node_current->latch_invalidate();
                mark_node_for_gc(node_current);
                mark_node_for_gc(node_child);

            } else { // standard case
                COUT_DEBUG("node current is the standard case");
                do_remove_and_shrink(node_parent, byte_parent, version_parent, node_current, byte_current, version_current);
            }


            return true;
        } else { // keep traversing
            key_level_start++;

            node_parent = node_current;
            byte_parent = byte_current;
            version_parent = version_current;

            node_current = node_child;
        }

    } while( true );
}

void Index::do_remove_and_shrink(Node* node_parent, uint8_t key_parent, uint64_t version_parent, Node* node_current, uint8_t key_current, uint64_t version_current){
    COUT_DEBUG("node_parent: " << node_parent << ", key_parent: " << (int) key_parent << ", version_parent: " << version_parent << ", node_current: " << node_current << ", key_current: " << (int) key_current << ", version_current: " << version_current << ", underfilled: " << node_current->is_underfilled());
    assert((node_parent == nullptr || !is_leaf(node_parent)) && "node_parent must be an inner node");

    if( !node_current->is_underfilled() || node_parent == nullptr ){ // standard case
        node_current->latch_upgrade_to_write_lock(version_current); // still valid
        node_current->remove(key_current);
        node_current->latch_write_unlock();
    } else { // shrink the current node
        //COUT_DEBUG("node_current: " << node_current << ", underfilled: " << node_current->is_underfilled() << ", type: " << (int) node_current->get_type() << ", num children: " << node_current->count());

        node_parent->latch_upgrade_to_write_lock(version_parent);
        try {
            node_current->latch_upgrade_to_write_lock(version_current);
        } catch ( Abort ){
            node_parent->latch_write_unlock();
            throw;
        }

        // remove the item
        node_current->remove(key_current);

        // shrink the node
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

        COUT_DEBUG("replace " << node_current << " (" << node_current->get_type() << ") with " << node_new << "(" << node_new->get_type() << ")");

        assert(node_new != nullptr);
        node_parent->change(key_parent, node_new);
        node_parent->latch_write_unlock();

        node_current->latch_invalidate();
        mark_node_for_gc(node_current);
    }
}

auto Index::find(uint64_t vertex_id) const -> Value {
    return find(vertex_id, 0);
}

auto Index::find(uint64_t src, uint64_t dst) const -> Value {
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "It should have already entered an epoch");

    Key key {src, dst};
    Value result;
    bool done = false;
    do {
        try {
            result = do_find(key, m_root, 0);
            done = true;
        } catch (Abort){
            // try again ...
        }
    } while (!done);

    return result;
}


auto Index::do_find(const Key& key, Node* node, int level) const -> Value {
    assert(node != nullptr);
    uint64_t node_version = node->latch_read_lock();

    // first check the damn prefix
    auto prefix_result = node->prefix_compare(key, level);
    node->latch_validate(node_version);
    switch(prefix_result){ // it doesn't matter what it returns, it needs to restart the search again
    case -1: {
        // counterintuitively, it means that the prefix of the node is lesser than the key
        // i.e. the key is bigger than any element in this node
        return get_max_leaf_address(node, node_version);
    } break;
    case 0: {
        /* nop */
    } break;
    case +1: {
        // counterintuitively, it means that the prefix of the node is greater than the key
        // ask the parent to return the max for the sibling that precedes this node
        return Value{};
    } break;
    } // end switch

    // second, find the next node to traverse in the tree
    Node* child; bool exact_match;
    std::tie(child, exact_match) = node->find_node_leq(key[level]);
    node->latch_validate(node_version);

    if(child == nullptr){
        return Value{}; // again, ask the parent to return the maximum of the previous sibling
    } else if (exact_match || is_leaf(child) ){

        // if we picked a leaf, check whether the search key to search is >= the the leaf's key. If not, our
        // target leaf will be the sibling of the current node
        if(is_leaf(child)){
            auto leaf = node2leaf(child);
            node->latch_validate(node_version);
            if(leaf->m_key <= key) return leaf->m_value;

            // otherwise, check the sibling ...
        } else {
            // the other case is the current byte is equal to the byte indexing this node, we need to traverse the tree
            // and see whether further down they find a suitable leaf. If not, again we need to check the sibling
            Value result = do_find(key, child, level +1);
            if (/* item found */ result.leaf() != nullptr) return result;

            // otherwise check the left sibling ...
        }

        // then the correct node is the maximum of the previous sibling
        Node* sibling = node->get_predecessor(key[level]);

        // is the information we read still valid ?
        node->latch_validate(node_version);

        if(sibling != nullptr){
            if(is_leaf(sibling)){
                auto leaf = node2leaf(sibling);

                // last check
                node->latch_validate(node_version);

                return leaf->m_value;
            } else {
                auto sibling_version = sibling->latch_read_lock();
                return get_max_leaf_address(sibling, sibling_version);
            }
        } else {
            // ask the parent
            return Value{};
        }

    } else { // key[level] > child[level], but it is lower than all other children => return the max from the given child

        auto child_version = child->latch_read_lock();
        node->latch_read_unlock(node_version);
        return get_max_leaf_address(child, child_version);

    }
}

auto Index::get_max_leaf_address(Node* current, uint64_t current_version) const -> Value {
    assert(current != nullptr);

    current->latch_validate(current_version);
    while(!is_leaf(current)){
        Node* child { nullptr };
        switch(current->get_type()){
        case Index::NodeType::N4:
            child = reinterpret_cast<N4*>(current)->get_max_child(); break;
        case Index::NodeType::N16:
            child = reinterpret_cast<N16*>(current)->get_max_child(); break;
        case Index::NodeType::N48:
            child = reinterpret_cast<N48*>(current)->get_max_child(); break;
        case Index::NodeType::N256:
            child = reinterpret_cast<N256*>(current)->get_max_child(); break;
        }

        // validate what we read is correct
        current->latch_validate(current_version);

        // acquire the lock for the leaf
        assert(child != nullptr);
        uint64_t child_version = 0;
        if(!is_leaf(child)){ child_version = child->latch_read_lock(); }

        // finally, release the current node
        current->latch_read_unlock(current_version);

        // next iteration
        current = child;
        current_version = child_version;
    }

    auto leaf = node2leaf(current);
    return leaf->m_value;
}

Index::Node* Index::leaf2node(Leaf* leaf){
    return reinterpret_cast<Node*>(reinterpret_cast<uint64_t>(leaf) | (1ull<<63));
}

Index::Leaf* Index::node2leaf(Node* node){
    assert(is_leaf(node));
    return reinterpret_cast<Leaf*>(reinterpret_cast<uint64_t>(node) & (~(1ull<<63)));;
}

bool Index::is_leaf(const Node* node){
    return reinterpret_cast<uint64_t>(node) & (1ull<<63);
}


void Index::mark_node_for_gc(Node* node){
    if(!is_leaf(node)){
        context::global_context()->gc()->mark(node);
    } else { // the node is a leaf
        context::global_context()->gc()->mark(node2leaf(node));
    }
}

void Index::delete_nodes_rec(Node* node){
    assert(node != nullptr && !is_leaf(node));

    for(int i = 0; i < 256; i++){
        Node* entry = node->get_child((uint8_t) i);
        if( entry == nullptr ) continue;

        if (is_leaf(entry)){ // remove a leaf
            delete node2leaf(entry);
        } else { // remove an intermediate node
            delete_nodes_rec( entry );
            delete entry;
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
    // in Intel x86, 64 bit integers are stored reversed in memory due to little endianness
    uint8_t* __restrict src_le = reinterpret_cast<uint8_t*>(&src);
    for(int i = 0; i < 8; i++){ m_data[i] = src_le[7 - i]; }
    uint8_t* __restrict dst_le = reinterpret_cast<uint8_t*>(&dst);
    for(int i = 0; i < 8; i++){ m_data[8 + i] = dst_le[7 - i]; }

    // COUT_DEBUG("src: " << src << ", dst: " << dst << ", encoding: " << *this);
}

uint8_t& Index::Key::operator[](uint32_t i){
    assert((int) i < length() && "Overflow");
    return m_data[i];
}

const uint8_t& Index::Key::operator[](uint32_t i) const{
    assert((int) i < length() && "Overflow");
    return m_data[i];
}

int Index::Key::length() const {
    return MAX_LENGTH;
}

uint8_t* Index::Key::data(){
    return reinterpret_cast<uint8_t*>(m_data);
}

const uint8_t* Index::Key::data() const {
    return reinterpret_cast<const uint8_t*>(m_data);
}

uint64_t Index::Key::get_source() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 0; i < 8; i++){ key_be[i] = m_data[7 - i]; }
    return vertex_id;
}

uint64_t Index::Key::get_destination() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 0; i < 8; i++){ key_be[i] = m_data[15 - i]; }
    return vertex_id;
}

bool Index::Key::operator==(const Key& other) const {
    // all keys should be 16 bytes in this implementation
    assert(length() == 16 && "[this] All keys should have a length of 16 bytes");
    assert(other.length() == 16 && "[other] All keys should have a length of 16 bytes");


#if defined(__SSE4_1__)
    __m128i op1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data()));
    __m128i op2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(other.data()));
    __m128i opxor = _mm_xor_si128(op1, op2);
    return _mm_testz_si128(opxor, opxor);
#else
    const uint8_t* __restrict op1 = reinterpret_cast<const uint8_t*>(data());
    const uint8_t* __restrict op2 = reinterpret_cast<const uint8_t*>(other.data());
    for(int i = 0; i < 16; i++){
        if(op1[i] != op2[i]) return false;
    }

    return true;
#endif
}

bool Index::Key::operator!=(const Key& other) const {
    return !((*this) == other);
}

bool Index::Key::operator<=(const Key& other) const {
    auto src1 = get_source();
    auto src2 = other.get_source();
    if(src1 < src2){
        return true;
    } else if (src1 == src2){
        auto dst1 = get_destination();
        auto dst2 = other.get_destination();
        return dst1 <= dst2;
    } else { // src1 > src2
        return false;
    }
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
Index::Node::Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length) : m_count(0) {
    set_type(type);
    set_prefix(prefix, prefix_length);
}

Index::NodeType Index::Node::get_type() const{
    uint8_t raw_type = (uint8_t) m_latch.get_payload();
    return *reinterpret_cast<NodeType*>(&raw_type);
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
    return ((get_type() != NodeType::N256) ?  m_count : reinterpret_cast<const N256*>(this)->count());
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
    assert(length <= (uint32_t) numeric_limits<uint8_t>::max() && "Overflow");
    if(length > 0){ // avoid the warning: argument 2 null where non-null expected [-Wnonnull]
        memcpy(m_prefix, prefix, std::min<int>(length, MAX_PREFIX_LEN));
    }
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
    int prefix_length = get_prefix_length();
    const uint8_t* prefix = get_prefix();

    if(out_prefix_end != nullptr) { *out_prefix_end = prefix_start + prefix_length; }

    for(int i = 0, j = prefix_start; i < prefix_length; i++, j++){
        if(i == MAX_PREFIX_LEN){ // we need to retrieve the full prefix from one of the leaves
            prefix = get_any_descendant_leaf()->m_key.data() + prefix_start;
        }

        if(key[j] != prefix[i]){ // the prefix does not match the given key
            if(out_prefix_end != nullptr) { *out_prefix_end = j; }
            if(out_non_matching_prefix != nullptr){
                if(prefix_length > MAX_PREFIX_LEN && i < MAX_PREFIX_LEN){
                    prefix = get_any_descendant_leaf()->m_key.data() + prefix_start;
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

    const uint8_t* __restrict prefix = get_prefix();
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

    const uint8_t* __restrict prefix = get_prefix();
    const int prefix_start = search_key_level;

    for(int i = 0, prefix_length = get_prefix_length(); i < prefix_length; i++){
        if (i == MAX_PREFIX_LEN) {
            Leaf* leaf = get_any_descendant_leaf();
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
            return reinterpret_cast<const N4 *>(this)->is_overfilled();
        case NodeType::N16:
            return reinterpret_cast<const N16 *>(this)->is_overfilled();
        case NodeType::N48:
            return reinterpret_cast<const N48 *>(this)->is_overfilled();
        case NodeType::N256:
            return reinterpret_cast<const N256 *>(this)->is_overfilled();
        default:
            assert(0 && "Invalid case");
            return false;
    }
}

bool Index::Node::is_underfilled() const{
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<const N4 *>(this)->is_underfilled();
        case NodeType::N16:
            return reinterpret_cast<const N16 *>(this)->is_underfilled();
        case NodeType::N48:
            return reinterpret_cast<const N48 *>(this)->is_underfilled();
        case NodeType::N256:
            return reinterpret_cast<const N256 *>(this)->is_underfilled();
        default:
            assert(0 && "Invalid case");
            return false;
    }
}

void Index::Node::insert(uint8_t key, Node* child){
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

bool Index::Node::remove(uint8_t key){
    COUT_DEBUG("this: " << this << " (" << get_type() << "), key: " << (int) key);
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<N4 *>(this)->remove(key);
        case NodeType::N16:
            return reinterpret_cast<N16 *>(this)->remove(key);
        case NodeType::N48:
            return reinterpret_cast<N48 *>(this)->remove(key);
        case NodeType::N256:
            return reinterpret_cast<N256 *>(this)->remove(key);
        default:
            assert(0 && "Invalid case");
            return false;
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

Index::Leaf* Index::Node::get_any_descendant_leaf() const{
    const Node* node { this };

    do {
        uint64_t version = node->latch_read_lock();
        const Node* next { nullptr };
        switch (node->get_type()) {
            case NodeType::N4:
                next = reinterpret_cast<const N4 *>(node)->get_any_child(); break;
            case NodeType::N16:
                next = reinterpret_cast<const N16 *>(node)->get_any_child(); break;
            case NodeType::N48:
                next = reinterpret_cast<const N48 *>(node)->get_any_child(); break;
            case NodeType::N256:
                next = reinterpret_cast<const N256 *>(node)->get_any_child(); break;
        }
        assert(next != nullptr);
        node->latch_read_unlock(version);

        // next iteration
        assert(node != next && "Infinite loop");
        node = next;
    } while (!is_leaf(node));

    return node2leaf(const_cast<Node*>(node));
}

std::pair<Index::Node*, /* exact match ? */ bool> Index::Node::find_node_leq(uint8_t key) const {
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<const N4 *>(this)->find_node_leq(key);
        case NodeType::N16:
            return reinterpret_cast<const N16 *>(this)->find_node_leq(key);
        case NodeType::N48:
            return reinterpret_cast<const N48 *>(this)->find_node_leq(key);
        case NodeType::N256:
            return reinterpret_cast<const N256 *>(this)->find_node_leq(key);
        default:
            assert(0 && "Invalid case");
    }

    __builtin_unreachable();
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
        out << "Leaf: " << node << ", key: " << leaf->m_key.get_source() << " -> " << leaf->m_key.get_destination() << ", value: " << leaf->m_value << "\n";
    } else {
        uint64_t version1 = node->m_latch.read_version();

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
        for(int i = 0, sz = std::min(node->get_prefix_length(), MAX_PREFIX_LEN); i < sz; i++){
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
        for(int i = 0; i <= 255; i++){
            Node* child = node->get_child(i);
            if(child == nullptr) continue;
            dump(out, child, level + 1 + node->get_prefix_length(), depth + 1);
        }

        try {
            node->m_latch.validate_version(version1); // treat it like an assertion, it should never fail
        } catch(Abort){
            assert(false && "#validate_version shouldn't abort here");
            throw;
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

void Index::N4::insert(uint8_t key, Node* value) {
//    COUT_DEBUG("key: " << (int) key << ", value: " << value);

    int pos;
    for(pos = count(); pos > 0 && m_keys[pos -1] > key; pos--){ // shift larger keys to the right
        m_keys[pos] = m_keys[pos -1];
        m_children[pos] = m_children[pos -1];
    }

    m_keys[pos] = key;
    m_children[pos] = value;
    m_count++;
    assert(m_count <= 4 && "Overflow");
}

bool Index::N4::remove(uint8_t key){
    for (int i = 0, sz = count(); i < sz; i++) {
        if (m_keys[i] == key) {
            mark_node_for_gc(m_children[i]); m_children[i] = nullptr;

            memmove(m_keys + i, m_keys + i + 1, sz - i - 1);
            memmove(m_children + i, m_children + i + 1, (sz - i - 1) * sizeof(Node*));
            assert(m_count > 0 && "Underflow");
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
        if(m_keys[i] != key){
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

Index::Node* Index::N4::get_any_child() const {
    Node* result { nullptr };
    for(int i = 0, sz = count(); i < sz; i++){
        if(is_leaf(m_children[i])){
            return m_children[i];
        } else {
            result = m_children[i];
        }
    }

    return result;
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

void Index::N16::insert(uint8_t key, Node* value) {
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
    Node** node = get_child_ptr(key);
    if(node == nullptr) return false;
    mark_node_for_gc(*node);

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

Index::Node* Index::N16::get_any_child() const {
    for (int i = 0, sz = count(); i < sz; i++) {
        if(is_leaf(m_children[i]))
            return m_children[i];
    }

    return m_children[0];
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
    for(int i = 0, sz = count(); i < sz; i++){
        new_node->insert(flip_sign(m_keys[i]), m_children[i]);
    }
    return new_node;
}

Index::N48* Index::N16::to_N48() const {
    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0, sz = count(); i < sz; i++){
        new_node->insert(flip_sign(m_keys[i]), m_children[i]);
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

void Index::N48::insert(uint8_t key, Node* value){
    assert(!is_overfilled() && "This node is full");

    // find the first non empty slot
    int pos = count();
    if (m_children[pos] != nullptr) {
        for (pos = 0; m_children[pos] != nullptr; pos++);
    }

    m_children[pos] = value;
    m_child_index[key] = (uint8_t) pos;

    m_count++;
}

bool Index::N48::remove(uint8_t byte){
    auto position = m_child_index[byte];
    if(position == EMPTY_MARKER) return false;
    mark_node_for_gc(m_children[position]);
    m_children[position] = nullptr;
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

Index::Node* Index::N48::get_any_child() const {
    Node* result { nullptr } ;
    for (int i = 0; i < 256; i++) {
        if (m_child_index[i] == EMPTY_MARKER) continue; // empty gap
        Node* candidate = m_children[m_child_index[i]];
        if (is_leaf(candidate)) {
            return candidate;
        } else {
            result = candidate;
        };
    }
    return result;
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

int Index::N256::count() const {
    // m_count is 1 byte, it overflows when the node is full
    if(m_count == 0 && m_children[0] != nullptr){
        return 256;
    } else {
        return m_count;
    }
}

void Index::N256::insert(uint8_t byte, Node* value){
    assert(m_children[byte] == nullptr && "Slot already occupied");
    m_children[byte] = value;
    m_count++;
}

bool Index::N256::remove(uint8_t key){
    if(m_children[key] == nullptr) return false;
    mark_node_for_gc(m_children[key]);
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

Index::Node* Index::N256::get_any_child() const {
    Node* result { nullptr };
    for (int i = 0; i < 256; i++) {
        Node* candidate = m_children[i];
        if(candidate == nullptr)
            continue; // gap
        else if (is_leaf(candidate))
            return candidate;
        else // it's a node
            result = candidate;
    }
    return result;
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
