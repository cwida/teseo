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


#include "teseo/memstore/dense_file.hpp"

#include <cassert>
#include <cstring>
#include <emmintrin.h> // x86 SSE intrinsics
#include <limits>
#include <iomanip>
#include <smmintrin.h> // SSE 4.1

#include "teseo/context/garbage_collector.hpp"
#include "teseo/context/global_context.hpp"
#include "teseo/context/static_configuration.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/memstore/context.hpp"
#include "teseo/memstore/data_item.hpp"
#include "teseo/memstore/error.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/transaction/undo.hpp"

#include "teseo/util/error.hpp"

#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::memstore {

/*****************************************************************************
 *                                                                           *
 *  Initialisation                                                           *
 *                                                                           *
 *****************************************************************************/
DenseFile::~DenseFile() {
    // Remove the index
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "Must be inside an epoch");
    delete_nodes_rec(m_root);
    mark_node_for_gc(m_root); m_root = nullptr;
}


/*****************************************************************************
 *                                                                           *
 *  Updates                                                                  *
 *                                                                           *
 *****************************************************************************/
bool DenseFile::update(Context& context, const Update& update, bool has_source_vertex) {
    if(!has_source_vertex && is_source_visible(context, update.source())) {
        throw NotSureIfItHasSourceVertex{};
    }

    DataItem* item = update_index(context, update);
    transaction::Undo* undo_old = nullptr;

    if(item->has_version()){
        item->m_version.prune_on_write();
        undo_old = item->m_version.get_undo();
    }

    // Update the version chain
    item->m_version.set_type(update);
    transaction::Undo* undo_new = context.m_transaction->mark_last_undo(undo_old);
    item->m_version.set_undo(undo_new);
    if(update.is_edge() && !item->is_empty()){ // save the old value for the weight
        reinterpret_cast<Update*>(undo_new->payload())->set_weight(item->m_update.weight());
    }

    // Copy the record in the dense file
    item->m_update = update;

    // There is always space in a dense file
    return true;
}

bool DenseFile::is_source_visible(Context& context, uint64_t vertex_id) const {
    bool source_exists = false;
    auto cb_visible = [&context, vertex_id, &source_exists](const DataItem* item){
        if(item->m_update.key().source() != vertex_id) return false; // done

        Update update = Update::read_delta(context, item);
        if(update.is_insert()){
            source_exists = true;
            return false; // stop the iterator
        } else {
            return true; // try with the next item
        }

    };

    Key key { vertex_id };
    scan(key, cb_visible);

    return source_exists;
}

void DenseFile::ccheck(Context* context, const Update* update, const DataItem* data_item){
    if(context == nullptr || update == nullptr) return;

    if(data_item == nullptr || data_item->is_empty()){ // new record
        if(update->is_remove()){
            throw Error{ update->key(), update->is_vertex() ? Error::VertexDoesNotExist : Error::EdgeDoesNotExist};
        }
    } else if( data_item->has_version() ){
        transaction::Undo* undo = data_item->m_version.get_undo();
        if(!context->m_transaction->can_write(undo)){
            throw Error{ update->key(), update->is_vertex() ? Error::VertexLocked : Error::EdgeLocked};
        } else if (update->is_insert() && data_item->m_version.is_insert()){
            throw Error{ update->key(), update->is_vertex() ? Error::VertexAlreadyExists : Error::EdgeAlreadyExists };
        } else if (update->is_remove() && data_item->m_version.is_remove()){
            throw Error{ update->key(), update->is_vertex() ? Error::VertexDoesNotExist : Error::EdgeDoesNotExist};
        }
    } else if ( !data_item->has_version() && update->is_insert() ){
        throw Error{ update->key(), update->is_vertex() ? Error::VertexAlreadyExists : Error::EdgeAlreadyExists };
    }
}

/*****************************************************************************
 *                                                                           *
 *  File                                                                     *
 *                                                                           *
 *****************************************************************************/
DenseFile::File::File() : m_capacity(std::max<uint32_t>(1024, context::StaticConfiguration::memstore_segment_size)), m_size(0) {
    m_elements = (DataItem*) calloc(m_capacity, sizeof(DataItem));
    if(m_elements == nullptr) throw std::bad_alloc{};
}

DenseFile::File::~File(){
    auto deleter = [](DataItem* array){ free(array); };
    context::global_context()->gc()->mark(m_elements, deleter);
}

uint64_t DenseFile::File::cardinality() const {
    return m_size;
}

DataItem* DenseFile::File::append(){
    uint64_t filepos = cardinality();

    // resize the array if it became too big
    if(filepos == m_capacity){
        uint64_t capacity = m_capacity *2;
        DataItem* array = (DataItem*) calloc(capacity, sizeof(DataItem));
        if(array == nullptr) throw std::bad_alloc{};
        memcpy(array, m_elements, filepos * sizeof(DataItem));

        auto deleter = [](DataItem* array){ free(array); };
        context::global_context()->gc()->mark(m_elements, deleter);

        m_elements = array;
        m_capacity = capacity;
    }

    m_size++;
    return m_elements + filepos;
}

void DenseFile::File::pop(){
    if(m_size > 0) m_size--;
}

DataItem* DenseFile::File::operator[](uint64_t index){
    assert(index < cardinality() && "Index out of bounds");
    return m_elements + index;
}

const DataItem* DenseFile::File::operator[](uint64_t index) const {
    assert(index < cardinality() && "Index out of bounds");
    return m_elements + index;
}

/*****************************************************************************
 *                                                                           *
 *  Index                                                                    *
 *                                                                           *
 *****************************************************************************/

DataItem* DenseFile::update_index(Context& context, const Update& update){
    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "It should have already entered an epoch");

    Key key { update.key().source(), update.key().destination() };
    Leaf leaf { m_file.cardinality() };
    uint64_t filepos = do_insert(&context, &update, key, nullptr, 0, m_root, 0, leaf);
    assert(filepos <= m_file.cardinality());

    return filepos < m_file.cardinality() ? m_file[filepos] : m_file.append();
}

uint64_t DenseFile::do_insert(Context* context, const Update* update, const Key& key, Node* node_parent, uint8_t byte_parent, Node* node_current, int key_level_start, Leaf element) {
    assert(node_current != nullptr && "No starting node given");
    assert((node_parent != nullptr || node_current == m_root) && "Isolated node");
    assert(((node_parent == nullptr) || (node_parent->get_child(byte_parent) == node_current)) && "byte_parent does not match the current node");

    uint8_t non_matching_prefix[Node::MAX_PREFIX_LEN]; uint8_t* ptr_non_matching_prefix = non_matching_prefix;
    int non_matching_length = 0;

    do {
        COUT_DEBUG("[iteration start] node_parent: " << node_parent << ", byte_parent: " << (int) byte_parent << ", node_current: " << node_current);

        // first check whether the prefix matches our key
        int key_level_end = 0; // up to where the current node matches the key, excl
        if( !node_current->prefix_match_exact(this, key, key_level_start, &key_level_end, &ptr_non_matching_prefix, &non_matching_length) ){
            assert(node_parent != nullptr);
            assert(non_matching_length > 0);
            ccheck(context, update, nullptr);

            // create a new node with a common prefix
            N4* node_new = new N4(node_current->get_prefix(), key_level_end - key_level_start);
            node_new->insert(key[key_level_end], leaf2node(element));
            node_new->insert(non_matching_prefix[0], node_parent->get_child(key[key_level_start -1]));

            COUT_DEBUG("prefix mismatch, create a new N4 node under " << node_parent << " at byte: " << (int) key[key_level_start -1] << ", node_new: " << node_new << ", leaf_new: " << leaf2node(element) << ", sibling: " << node_parent->get_child(key[key_level_start -1]));

            node_parent->change(key[key_level_start -1], node_new);
            node_current->set_prefix(non_matching_prefix +1, non_matching_length -1);

            return element.m_value;
        }

        // now check the byte at the current node
        key_level_start = key_level_end;
        uint8_t byte_current = key[key_level_start]; // separator key for the current node
        Node* node_child = node_current->get_child(byte_current);

        if(node_child == nullptr){ // the slot `byte_current' is empty => insert into node_current
            COUT_DEBUG("standard case");
            ccheck(context, update, nullptr);
            do_insert_and_grow( node_parent, byte_parent, node_current, byte_current, element );
            return leaf2filepos(element);
        } else if(is_leaf(node_child)){
            // the slot `byte_current' is occupied by a leaf, we need to create a new inner node to separate between the new element and the leaf already present

            Key key_sibling = leaf2key( node2leaf(node_child) );
            if( key_sibling == key ){ // same key => the element already exists
                Leaf existing_element = node2leaf(node_child);
                ccheck(context, update, leaf2di(existing_element));
                return leaf2filepos(existing_element);
            }

            key_level_start++;
            int prefix_length = 0;
            while(key[key_level_start + prefix_length] == key_sibling[key_level_start + prefix_length]) prefix_length++;

            N4* node_new = new N4(&key[key_level_start], prefix_length);
            node_new->insert(key[key_level_start + prefix_length], leaf2node(element));
            node_new->insert(key_sibling[key_level_start + prefix_length], node_child);

            COUT_DEBUG("conflict, create a new N4 node under " << node_parent << " at byte: " << (int) key[key_level_start -1] << ", node_new: " << node_new << ", leaf 1 (new element): " << leaf2node(element) << ", leaf 2 (existing element): "  << node_child);
            node_current->change(byte_current, node_new);

            return leaf2filepos(element); // done
        }

        // keep traversing the trie
        key_level_start++;
        node_parent = node_current; byte_parent = byte_current;
        node_current = node_child;

    } while (true);
}

void DenseFile::do_insert_and_grow(Node* node_parent, uint8_t key_parent, Node* node_current, uint8_t key_current, Leaf new_element){
    assert((node_parent == nullptr || !is_leaf(node_parent)) && "It must be an inner node");
    assert(!is_leaf(node_current) && "It must be an inner node");

    if(node_current->is_overfilled()){ // there is no space in the current node for a new child, expand it
        assert(node_parent != nullptr && "node_parent can be null only iff node_current is the root of the tree, which will never be overfilled");
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


        // update the ptr from the old to the new inner node
        node_parent->change(key_parent, node_current);

        mark_node_for_gc(node_old); // to support optimistic readers

    }

    COUT_DEBUG("insert into " << node_current << " (" << node_current->get_type() << ") at byte " << (int) key_current << " the leaf (new element): " << leaf2node(new_element));
    node_current->insert(key_current, leaf2node(new_element));
}

template<typename Callback>
void DenseFile::scan(const Key& key, Callback cb) const {
    do_scan(key, m_root, 0, cb);
}

template<typename Callback>
bool DenseFile::do_scan(const Key& key, Node* node, int level, Callback cb) const {
    auto prefix_result = node->prefix_compare(this, key, level);

    switch(prefix_result){
    case -1: {
        // counterintuitively, it means that the prefix of the node is lesser than the key
        // i.e. the key is bigger than any element in this node
        return true;
    } break;
    case 0: {
        bool keep_going = true;
        Node* child = node->get_child(key[level]);
        if(child != nullptr){
            keep_going = do_scan(key, child, level +1, cb);
        }
        if(keep_going){
            NodeList list = node->children_gt(key[level]);
            uint64_t i = 0;
            while(keep_going && i < list.m_size){
                keep_going = do_scan_everything(list.m_nodes[i], cb);
            }
        }

        return keep_going;
    } break;
    case +1: {
        // counterintuitively, it means that the prefix of the node is greater than the key
        // ask the parent to return the max for the sibling that precedes this node
        return do_scan_everything(node, cb);
    } break;
    default:
        assert(0 && "Invalid case");
        return true;
    } // end switch
}

template<typename Callback>
bool DenseFile::do_scan_everything(Node* node, Callback cb) const {
    if(is_leaf(node)){
        const DataItem* di = leaf2di( node2leaf(node) );
        if(di->m_update.is_empty()){ // ignore this data item
            return true;
        } else {
            return cb(di);
        }
    } else {
        NodeList children = node->children();
        uint64_t i = 0;
        bool keep_going = true;
        while(i < children.m_size && keep_going){
            keep_going = do_scan_everything(children.m_nodes[i], cb);
        }
        return keep_going;
    }
}


//auto DenseFile::find(uint64_t vertex_id) const -> Value {
//    return find(vertex_id, 0);
//}
//
//auto DenseFile::find(uint64_t src, uint64_t dst) const -> Value {
//    assert(context::thread_context()->epoch() != numeric_limits<uint64_t>::max() && "It should have already entered an epoch");
//
//    Key key {src, dst};
//    void* result { nullptr };
//    bool done = false;
//    do {
//        try {
//            result = do_find(key, m_root, 0);
//            done = true;
//        } catch (Abort){
//            // try again ...
//        }
//    } while (!done);
//
//    return result;
//}
//
//
//auto DenseFile::do_find(const Key& key, Node* node, int level) const -> Value {
//    assert(node != nullptr);
//    uint64_t node_version = node->latch_read_lock();
//
//    // first check the damn prefix
//    auto prefix_result = node->prefix_compare(key, level);
//    node->latch_validate(node_version);
//    switch(prefix_result){ // it doesn't matter what it returns, it needs to restart the search again
//    case -1: {
//        // counterintuitively, it means that the prefix of the node is lesser than the key
//        // i.e. the key is bigger than any element in this node
//        return get_max_leaf_address(node, node_version);
//    } break;
//    case 0: {
//        /* nop */
//    } break;
//    case +1: {
//        // counterintuitively, it means that the prefix of the node is greater than the key
//        // ask the parent to return the max for the sibling that precedes this node
//        return nullptr;
//    } break;
//    } // end switch
//
//    // second, find the next node to traverse in the tree
//    Node* child; bool exact_match;
//    std::tie(child, exact_match) = node->find_node_leq(key[level]);
//    node->latch_validate(node_version);
//
//    if(child == nullptr){
//        return nullptr; // again, ask the parent to return the maximum of the previous sibling
//    } else if (exact_match || is_leaf(child) ){
//
//        // if we picked a leaf, check whether the search key to search is >= the the leaf's key. If not, our
//        // target leaf will be the sibling of the current node
//        if(is_leaf(child)){
//            auto leaf = node2leaf(child);
//            node->latch_validate(node_version);
//            if(leaf->m_key <= key) return leaf->m_value;
//
//            // otherwise, check the sibling ...
//        } else {
//            // the other case is the current byte is equal to the byte indexing this node, we need to traverse the tree
//            // and see whether further down they find a suitable leaf. If not, again we need to check the sibling
//            void* result = do_find(key, child, level +1);
//            if (/* item found */ result != nullptr) return result;
//
//            // otherwise check the left sibling ...
//        }
//
//        // then the correct node is the maximum of the previous sibling
//        Node* sibling = node->get_predecessor(key[level]);
//
//        // is the information we read still valid ?
//        node->latch_validate(node_version);
//
//        if(sibling != nullptr){
//            if(is_leaf(sibling)){
//                auto leaf = node2leaf(sibling);
//
//                // last check
//                node->latch_validate(node_version);
//
//                return leaf->m_value;
//            } else {
//                auto sibling_version = sibling->latch_read_lock();
//                return get_max_leaf_address(sibling, sibling_version);
//            }
//        } else {
//            // ask the parent
//            return nullptr;
//        }
//
//    } else { // key[level] > child[level], but it is lower than all other children => return the max from the given child
//
//        auto child_version = child->latch_read_lock();
//        node->latch_read_unlock(node_version);
//        return get_max_leaf_address(child, child_version);
//
//    }
//}

auto DenseFile::get_max_leaf(Node* current) const -> Leaf {
    assert(current != nullptr);

    while(!is_leaf(current)){
        Node* child { nullptr };
        switch(current->get_type()){
        case DenseFile::NodeType::N4:
            child = reinterpret_cast<N4*>(current)->get_max_child(); break;
        case DenseFile::NodeType::N16:
            child = reinterpret_cast<N16*>(current)->get_max_child(); break;
        case DenseFile::NodeType::N48:
            child = reinterpret_cast<N48*>(current)->get_max_child(); break;
        case DenseFile::NodeType::N256:
            child = reinterpret_cast<N256*>(current)->get_max_child(); break;
        }

        // next iteration
        current = child;
    }

    return node2leaf(current);
}

DenseFile::Node* DenseFile::leaf2node(Leaf leaf){
    return reinterpret_cast<Node*>(leaf.m_value | (1ull<<63));
}

DenseFile::Leaf DenseFile::node2leaf(Node* node){
    assert(is_leaf(node));
    return Leaf { reinterpret_cast<uint64_t>(node) & (~(1ull<<63)) };
}

bool DenseFile::is_leaf(const Node* node){
    return reinterpret_cast<uint64_t>(node) & (1ull<<63);
}

uint64_t DenseFile::leaf2filepos(Leaf leaf) {
    return leaf.m_value;
}

auto DenseFile::leaf2key(Leaf leaf) const -> Key {
    const DataItem* di = leaf2di(leaf);
    if(di->m_update.is_vertex()){
        return Key { di->m_update.source() };
    } else {
        return Key { di->m_update.source(), di->m_update.destination() };
    }
}

DataItem* DenseFile::leaf2di(Leaf leaf) {
    return m_file[ leaf2filepos(leaf) ];
}

const DataItem* DenseFile::leaf2di(Leaf leaf) const {
    return m_file[ leaf2filepos(leaf) ];
}

void DenseFile::mark_node_for_gc(Node* node){
    if(node != nullptr && !is_leaf(node)){
        context::global_context()->gc()->mark(node);
    }
}

void DenseFile::delete_nodes_rec(Node* node){
    assert(node != nullptr && !is_leaf(node));

    for(int i = 0; i < 256; i++){
        Node* entry = node->get_child((uint8_t) i);
        if( entry == nullptr ) continue;

        if (!is_leaf(entry)){ // remove an intermediate node
            delete_nodes_rec( entry );
            mark_node_for_gc( entry );
        }
    }
}

void DenseFile::dump_index(std::ostream& out) const {
    Node::dump(out, this, m_root, 0, 0);
}

/*****************************************************************************
 *                                                                           *
 *  Encoded keys (vertex ids)                                                *
 *                                                                           *
 *****************************************************************************/
DenseFile::Key::Key(uint64_t key) : Key(key, 0){ }

DenseFile::Key::Key(uint64_t src, uint64_t dst){
    // in Intel x86, 64 bit integers are stored reversed in memory due to little endianness
    uint8_t* __restrict src_le = reinterpret_cast<uint8_t*>(&src);
    for(int i = 0; i < 8; i++){ m_data[i] = src_le[7 - i]; }
    uint8_t* __restrict dst_le = reinterpret_cast<uint8_t*>(&dst);
    for(int i = 0; i < 8; i++){ m_data[8 + i] = dst_le[7 - i]; }

    // COUT_DEBUG("src: " << src << ", dst: " << dst << ", encoding: " << *this);
}

uint8_t& DenseFile::Key::operator[](uint32_t i){
    assert((int) i < length() && "Overflow");
    return m_data[i];
}

const uint8_t& DenseFile::Key::operator[](uint32_t i) const{
    assert((int) i < length() && "Overflow");
    return m_data[i];
}

int DenseFile::Key::length() const {
    return MAX_LENGTH;
}

uint8_t* DenseFile::Key::data(){
    return reinterpret_cast<uint8_t*>(m_data);
}

const uint8_t* DenseFile::Key::data() const {
    return reinterpret_cast<const uint8_t*>(m_data);
}

uint64_t DenseFile::Key::get_source() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 0; i < 8; i++){ key_be[i] = m_data[7 - i]; }
    return vertex_id;
}

uint64_t DenseFile::Key::get_destination() const {
    union { uint8_t key_be[8]; uint64_t vertex_id; };
    for(int i = 0; i < 8; i++){ key_be[i] = m_data[15 - i]; }
    return vertex_id;
}

bool DenseFile::Key::operator==(const Key& other) const {
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

bool DenseFile::Key::operator!=(const Key& other) const {
    return !((*this) == other);
}

bool DenseFile::Key::operator<=(const Key& other) const {
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

std::ostream& operator<<(std::ostream& out, const DenseFile::Key& key){
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
DenseFile::Node::Node(NodeType type, const uint8_t* prefix, uint32_t prefix_length) : m_count(0) {
    set_type(type);
    set_prefix(prefix, prefix_length);
}

DenseFile::NodeType DenseFile::Node::get_type() const{
    return m_type;
}

void DenseFile::Node::set_type(NodeType type){
    m_type = type;
}

int DenseFile::Node::count() const {
    return ((get_type() != NodeType::N256) ?  m_count : reinterpret_cast<const N256*>(this)->count());
}

uint8_t* DenseFile::Node::get_prefix() {
    return reinterpret_cast<uint8_t*>(m_prefix);
}

const uint8_t* DenseFile::Node::get_prefix() const {
    return reinterpret_cast<const uint8_t*>(m_prefix);
}

int DenseFile::Node::get_prefix_length() const {
    return m_prefix_sz;
}

bool DenseFile::Node::has_prefix() const {
    return get_prefix_length() > 0;
}

void DenseFile::Node::set_prefix(const uint8_t* prefix, uint32_t length) {
    assert(length <= (uint32_t) numeric_limits<uint8_t>::max() && "Overflow");
    memcpy(m_prefix, prefix, std::min<int>(length, MAX_PREFIX_LEN));
    m_prefix_sz = static_cast<uint8_t>(length);
}

void DenseFile::Node::prefix_prepend(Node* first_part, uint8_t second_part){
    assert(first_part != nullptr && !is_leaf(first_part));
    int num_bytes_to_prepend = std::min(MAX_PREFIX_LEN, first_part->get_prefix_length() + 1);
    memmove(/* to */ m_prefix + num_bytes_to_prepend, /* from */ m_prefix, std::min(get_prefix_length(), MAX_PREFIX_LEN - num_bytes_to_prepend));
    memcpy(/* to */ m_prefix, first_part->get_prefix(), std::min(num_bytes_to_prepend, first_part->get_prefix_length()));
    if (first_part->get_prefix_length() < MAX_PREFIX_LEN) { m_prefix[num_bytes_to_prepend - 1] = second_part; }
    this->m_prefix_sz += first_part->get_prefix_length() + 1;
}

bool DenseFile::Node::prefix_match_exact(DenseFile* df, const Key& key, int prefix_start, int* out_prefix_end, uint8_t** out_non_matching_prefix, int* out_non_matching_length) const {
    int prefix_length = get_prefix_length();
    const uint8_t* prefix = get_prefix();

    if(out_prefix_end != nullptr) { *out_prefix_end = prefix_start + prefix_length; }

    for(int i = 0, j = prefix_start; i < prefix_length; i++, j++){
        if(i == MAX_PREFIX_LEN){ // we need to retrieve the full prefix from one of the leaves
            prefix = df->leaf2key(get_any_descendant_leaf()).data() + prefix_start;
        }

        if(key[j] != prefix[i]){ // the prefix does not match the given key
            if(out_prefix_end != nullptr) { *out_prefix_end = j; }
            if(out_non_matching_prefix != nullptr){
                if(prefix_length > MAX_PREFIX_LEN && i < MAX_PREFIX_LEN){
                    prefix = df->leaf2key(get_any_descendant_leaf()).data() + prefix_start;
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


int DenseFile::Node::prefix_match_approximate(const Key& key, int prefix_start, int* out_prefix_end) const {
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


int DenseFile::Node::prefix_compare(const DenseFile* df, const Key& search_key, int& /* in/out */ search_key_level) const {
    if(!has_prefix()) return 0; // the current doesn't have a prefix => technically it matches

    const uint8_t* __restrict prefix = get_prefix();
    const int prefix_start = search_key_level;

    for(int i = 0, prefix_length = get_prefix_length(); i < prefix_length; i++){
        if (i == MAX_PREFIX_LEN) {
            Leaf leaf = get_any_descendant_leaf();
            assert(search_key.length() == df->leaf2key(leaf).length() && "All keys should have the same length");
            prefix = df->leaf2key(leaf).data() + prefix_start;
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

bool DenseFile::Node::change(uint8_t key, Node* value){
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

bool DenseFile::Node::is_overfilled() const{
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

void DenseFile::Node::insert(uint8_t key, Node* child){
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

DenseFile::Node* DenseFile::Node::get_child(uint8_t key) const {
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

DenseFile::Leaf DenseFile::Node::get_any_descendant_leaf() const{
    const Node* node { this };

    do {
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

        // next iteration
        assert(node != next && "Infinite loop");
        node = next;
    } while (!is_leaf(node));

    return node2leaf(const_cast<Node*>(node));
}

std::pair<DenseFile::Node*, /* exact match ? */ bool> DenseFile::Node::find_node_leq(uint8_t key) const {
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

DenseFile::Leaf DenseFile::Node::get_max_leaf(Node* node) {
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

        // next iteration
        node = child;
    }

    return node2leaf(node);
}

DenseFile::NodeList DenseFile::Node::children() const {
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<const N4 *>(this)->children();
        case NodeType::N16:
            return reinterpret_cast<const N16 *>(this)->children();
        case NodeType::N48:
            return reinterpret_cast<const N48 *>(this)->children();
        case NodeType::N256:
            return reinterpret_cast<const N256 *>(this)->children();
        default:
            assert(0 && "Invalid case");
    }

    __builtin_unreachable();
}

DenseFile::NodeList DenseFile::Node::children_gt(uint8_t key) const {
    switch (get_type()) {
        case NodeType::N4:
            return reinterpret_cast<const N4 *>(this)->children_gt(key);
        case NodeType::N16:
            return reinterpret_cast<const N16 *>(this)->children_gt(key);
        case NodeType::N48:
            return reinterpret_cast<const N48 *>(this)->children_gt(key);
        case NodeType::N256:
            return reinterpret_cast<const N256 *>(this)->children_gt(key);
        default:
            assert(0 && "Invalid case");
    }

    __builtin_unreachable();
}

DenseFile::Node* DenseFile::Node::get_predecessor(uint8_t key) const {
    return (key > 0) ? find_node_leq(key -1).first : nullptr;
}

static void print_tabs(std::ostream& out, int depth){
    auto flags = out.flags();
    out << setw(depth * 4) << setfill(' ') << ' ';
    out.setf(flags);
}

void DenseFile::Node::dump(std::ostream& out, const DenseFile* df, Node* node, int level, int depth) {
    assert(node != nullptr);

    print_tabs(out, depth);

    if(is_leaf(node)){
        auto leaf = node2leaf(node);
        auto key = df->leaf2key(leaf);
        out << "Leaf: " << node << ", key: " << key.get_source() << " -> " << key.get_destination() << ", value: " << leaf2filepos(leaf) << "\n";
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
            dump(out, df, child, level + 1 + node->get_prefix_length(), depth + 1);
        }
    }
}

/*****************************************************************************
 *                                                                           *
 *  Node4                                                                    *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME /* debug only */
#define COUT_CLASS_NAME "IndexMemstore::N4"

DenseFile::N4::N4(const uint8_t *prefix, uint32_t prefix_length) : Node(NodeType::N4, prefix, prefix_length){

}

void DenseFile::N4::insert(uint8_t key, Node* value) {
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

bool DenseFile::N4::remove(uint8_t key){
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

DenseFile::Node** DenseFile::N4::get_child_ptr(uint8_t byte) {
    for (int i = 0, end = m_count; i < end; i++) {
        if(m_keys[i] == byte)
            return m_children + i;
    }

    return nullptr;
}

DenseFile::Node* DenseFile::N4::get_max_child() const {
    assert(count() > 0 && "empty node?");
    return m_children[count() -1];
}

tuple</* key */ uint8_t, /* entry */ DenseFile::Node*> DenseFile::N4::get_other_child(uint8_t key) const {
    for(int i = 0, sz = count(); i < sz; i++){
        if(m_keys[i] != key){
            return std::make_tuple(m_keys[i], m_children[i]);
        }
    }

    return std::make_tuple(0, nullptr);
}

pair<DenseFile::Node*, /* exact match ? */ bool> DenseFile::N4::find_node_leq(uint8_t key) const {
    int i = count() -1;
    while(i >= 0 && m_keys[i] > key) i--;
    if(i < 0){ // no match!
        return pair<Node*, /* exact match ? */ bool>{ nullptr, false };
    } else { // i >= 0, match
        return pair<Node*, /* exact match ? */ bool>{ m_children[i], /* exact match ? */ (key == m_keys[i]) };
    }
}

DenseFile::Node* DenseFile::N4::get_any_child() const {
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

DenseFile::NodeList DenseFile::N4::children() const {
    NodeList list; // the list of children is already sorted in N4
    list.m_size = m_count;
    memcpy(list.m_nodes, m_children, m_count * sizeof(Node*));
    return list;
}

DenseFile::NodeList DenseFile::N4::children_gt(uint8_t key) const {
    int pos = 0;
    bool stop = false;
    while(pos < m_count && !stop){
        if(m_keys[pos] <= key){
            pos++;
        } else {
            stop = true;
        }
    }

    NodeList list; // the list of children is already sorted in N4
    list.m_size = m_count -pos;
    memcpy(list.m_nodes, m_children + pos, (m_count - pos) * sizeof(Node*));
    return list;
}


bool DenseFile::N4::is_overfilled() const {
    return count() == 4;
}

bool DenseFile::N4::is_underfilled() const {
    return false;
}

DenseFile::N16* DenseFile::N4::to_N16() const {
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
DenseFile::N16::N16(const uint8_t* prefix, uint32_t prefix_length):
           Node(NodeType::N16, prefix, prefix_length) {
    memset(m_keys, 0, sizeof(m_keys));
    memset(m_children, 0, sizeof(m_children));
}

void DenseFile::N16::insert(uint8_t key, Node* value) {
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

bool DenseFile::N16::remove(uint8_t key){
    Node** node = get_child_ptr(key);
    if(node == nullptr) return false;

    std::size_t pos = node - m_children;
    memmove(m_keys + pos, m_keys + pos + 1, count() - pos - 1);
    memmove(m_children + pos, m_children + pos + 1, (count() - pos - 1) * sizeof(Node*));
    m_count--;

    assert(get_child(key) == nullptr);
    return true;
}

DenseFile::Node** DenseFile::N16::get_child_ptr(uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flip_sign(k)),
                                 _mm_loadu_si128(reinterpret_cast<const __m128i *>(m_keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << count()) - 1);

    if(bitfield){
        return &m_children[ctz(bitfield)];
    } else {
        return nullptr;
    }
}

DenseFile::Node* DenseFile::N16::get_any_child() const {
    for (int i = 0, sz = count(); i < sz; i++) {
        if(is_leaf(m_children[i]))
            return m_children[i];
    }

    return m_children[0];
}

pair<DenseFile::Node*, /* exact match ? */ bool> DenseFile::N16::find_node_leq(uint8_t key_unsigned) const {
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

DenseFile::Node* DenseFile::N16::get_max_child() const {
    assert(count() > 0 && "empty node?");
    return m_children[count() -1];
}

DenseFile::NodeList DenseFile::N16::children() const {
    NodeList list; // the list of children is already sorted in N16
    list.m_size = m_count;
    memcpy(list.m_nodes, m_children, m_count * sizeof(Node*));
    return list;
}

DenseFile::NodeList DenseFile::N16::children_gt(uint8_t key_unsigned) const {
    uint8_t key_signed = flip_sign(key_unsigned);
    auto lhs = _mm_loadu_si128(reinterpret_cast<const __m128i *>(m_keys));
    auto rhs = _mm_set1_epi8(key_signed);
    auto cmp = _mm_cmpgt_epi8(lhs, rhs);
    unsigned mask = ((1U << count()) -1);
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
    int pos = (bitfield == 0) ? count() : __builtin_ctz(bitfield);

    NodeList list; // the list of children is already sorted in N16
    list.m_size = m_count -pos;
    memcpy(list.m_nodes, m_children + pos, (m_count - pos) * sizeof(Node*));
    return list;
}

bool DenseFile::N16::is_overfilled() const {
    return count() == 16;
}

bool DenseFile::N16::is_underfilled() const {
    return count() <= 3;
}

uint8_t DenseFile::N16::flip_sign(uint8_t byte) {
    return byte ^ 128;
}

unsigned DenseFile::N16::ctz(uint16_t value){
    return __builtin_ctz(value);
}

DenseFile::N4* DenseFile::N16::to_N4() const {
    if(count() > 4) RAISE(InternalError, "N16 cannot shrink to N4, the number of children is : " << count());

    N4* new_node = new N4(get_prefix(), get_prefix_length());
    for(int i = 0, sz = count(); i < sz; i++){
        new_node->insert(flip_sign(m_keys[i]), m_children[i]);
    }
    return new_node;
}

DenseFile::N48* DenseFile::N16::to_N48() const {
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
#undef COUT_CLASS_NAME /* debug only */
#define COUT_CLASS_NAME "IndexMemstore::N48"


DenseFile::N48::N48(const uint8_t* prefix, uint32_t prefix_length) :
        Node(NodeType::N48, prefix, prefix_length) {
    memset(m_child_index, EMPTY_MARKER, sizeof(m_child_index));
    memset(m_children, 0, sizeof(m_children));
}

void DenseFile::N48::insert(uint8_t key, Node* value){
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

bool DenseFile::N48::remove(uint8_t byte){
    if(m_child_index[byte] == EMPTY_MARKER) return false;
    m_children[m_child_index[byte]] = nullptr;
    m_child_index[byte] = EMPTY_MARKER;
    m_count--;

    assert(get_child(byte) == nullptr);
    return true;
}

DenseFile::Node** DenseFile::N48::get_child_ptr(const uint8_t k) {
    if (m_child_index[k] == EMPTY_MARKER) {
        return nullptr;
    } else {
        return &m_children[m_child_index[k]];
    }
}

pair<DenseFile::Node*, /* exact match ? */ bool> DenseFile::N48::find_node_leq(uint8_t key) const {
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

DenseFile::Node* DenseFile::N48::get_max_child() const {
    for (int i = 255; i >= 0; i--) {
        if(m_child_index[i] != EMPTY_MARKER){
            return m_children[m_child_index[i]];
        }
    }

    assert(0 && "This code should be unreachable!");
    return nullptr;
}

DenseFile::Node* DenseFile::N48::get_any_child() const {
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

DenseFile::NodeList DenseFile::N48::children() const {
    NodeList list;

    int j = 0;
    for (int i = 0; i < 256; i++) {
        if (m_child_index[i] == EMPTY_MARKER) continue; // empty gap
        list.m_nodes[j] = m_children[m_child_index[i]];
        j++;
    }

    list.m_size = j;
    return list;
}

DenseFile::NodeList DenseFile::N48::children_gt(uint8_t key) const {
    NodeList list;

    int j = 0;
    for (int i = static_cast<int>(key) +1; i < 256; i++) {
        if (m_child_index[i] == EMPTY_MARKER) continue; // empty gap
        list.m_nodes[j] = m_children[m_child_index[i]];
        j++;
    }

    list.m_size = j;
    return list;
}

bool DenseFile::N48::is_overfilled() const {
    return count() == 48;
}

bool DenseFile::N48::is_underfilled() const {
    return count() <= 12;
}

DenseFile::N16* DenseFile::N48::to_N16() const {
    if(count() > 16) RAISE(InternalError, "N48 cannot shrink to N16, the number of children is : " << count());

    N16* new_node = new N16(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_child_index[i] != EMPTY_MARKER){
            new_node->insert((uint8_t) i, m_children[m_child_index[i]]);
        }
    }
    return new_node;
}

DenseFile::N256* DenseFile::N48::to_N256() const {
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
DenseFile::N256::N256(const uint8_t* prefix, uint32_t prefix_length) : Node(NodeType::N256, prefix, prefix_length){
    memset(m_children, '\0', sizeof(m_children));
}

int DenseFile::N256::count() const {
    // m_count is 1 byte, it overflows when the node is full
    if(m_count == 0 && m_children[0] != nullptr){
        return 256;
    } else {
        return m_count;
    }
}

void DenseFile::N256::insert(uint8_t byte, Node* value){
    assert(m_children[byte] == nullptr && "Slot already occupied");
    m_children[byte] = value;
    m_count++;
}

bool DenseFile::N256::remove(uint8_t key){
    if(m_children[key] == nullptr) return false;
    m_children[key] = nullptr;
    m_count--;
    return true;
}

DenseFile::Node** DenseFile::N256::get_child_ptr(uint8_t byte) {
    return (m_children[byte] != nullptr) ? &(m_children[byte]) : nullptr;
}

pair<DenseFile::Node*, /* exact match ? */ bool> DenseFile::N256::find_node_leq(uint8_t key) const {
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

DenseFile::Node* DenseFile::N256::get_max_child() const {
    for (int i = 255; i >= 0; i--) {
        if(m_children[i] != nullptr){
            return m_children[i];
        }
    }

    assert(0 && "This code should be unreachable!");
    return nullptr;
}

DenseFile::Node* DenseFile::N256::get_any_child() const {
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

DenseFile::NodeList DenseFile::N256::children() const {
    NodeList list;

    int j = 0;
    for (int i = 0; i < 256; i++) {
        if (m_children[i] == nullptr) continue; // empty gap
        list.m_nodes[j] = m_children[i];
        j++;
    }

    list.m_size = j;
    return list;
}

DenseFile::NodeList DenseFile::N256::children_gt(uint8_t key) const {
    NodeList list;

    int j = 0;
    for (int i = static_cast<int>(key) +1; i < 256; i++) {
        if (m_children[i] == nullptr) continue; // empty gap
        list.m_nodes[j] = m_children[i];
        j++;
    }

    list.m_size = j;
    return list;
}

bool DenseFile::N256::is_overfilled() const{
    return false;
}

bool DenseFile::N256::is_underfilled() const {
    return count() <= 37;
}

DenseFile::N48* DenseFile::N256::to_N48() const {
    if(count() > 48) RAISE(InternalError, "N256 cannot shrink to N48, the number of children is : " << count());

    N48* new_node = new N48(get_prefix(), get_prefix_length());
    for(int i = 0; i < 256; i++){
        if(m_children[i] != nullptr){
            new_node->insert((uint8_t) i, m_children[i]);
        }
    }

    return new_node;
}

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/

ostream& operator<<(ostream& out, const DenseFile::NodeType& type){
    switch(type){
    case DenseFile::NodeType::N4:
        out << "N4"; break;
    case DenseFile::NodeType::N16:
        out << "N16"; break;
    case DenseFile::NodeType::N48:
        out << "N48"; break;
    case DenseFile::NodeType::N256:
        out << "N256"; break;
    }
    return out;
}

} // namespace
