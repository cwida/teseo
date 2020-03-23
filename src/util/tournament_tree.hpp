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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <climits>
#include <cmath>
#include <cstring> // memset
#include <functional>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include "bitset.hpp"

//#define DEBUG

namespace teseo::internal::util {

/**
 * An implementation of a loser tournament tree to merge and extract in sorted
 * order from multiple input sorted sequences/queues.
 *
 * Sample usage:
 *
 * TournamentTree<Key, Queue*> tree(num_queues);
 *
 * // initialise the tree with the first elements of all input sequences/queues
 * for(uint64_t i = 0; i < num_queues; i++){
 *   tree.set(i, queues[i].pop(), &queues[i]);
 * }
 * tree.rebuild(); // find the first minimum in the tree
 *
 * // merge & extract the elements from the tournament tree
 * while (!tree.done){
 *   std::pair<Key, Queue*> item = tree.top();
 *
 *   // ... process the extracted item.first ...
 *
 *   // refill the tournament tree with the next item from the winner's queue
 *   Queue* Q = item.second;
 *   if(Q->empty()){
 *     tree.pop_and_unset(); // queue exhausted
 *   } else {
 *     tree.pop_and_replace(Q->pop());
 *   }
 * }
 *
 *
 */
template<typename K, typename V, typename Comparator = std::less<K>, int Rightmost_Array_Sz = 8>
class TournamentTree {
    TournamentTree(const TournamentTree&) = delete;
    TournamentTree& operator=(const TournamentTree&) = delete;

    const uint16_t m_fanout; // max number of children indexed by each node
    const int16_t m_height; // the height of this tree
    const int32_t m_capacity; // the number of items/leaves in the tree

    /**
     * Keep track of the cardinality and the height of the rightmost subtrees
     */
    struct RightmostSubtreeInfo {
        uint16_t m_root_sz; // the number of elements in the root
        uint16_t m_right_height; // the height of the rightmost subtree
    };
    constexpr static uint64_t m_rightmost_sz = Rightmost_Array_Sz;
    RightmostSubtreeInfo m_rightmost[m_rightmost_sz];

    /**
     * The function used to compare the keys. It must be default constructible.
     */
    Comparator m_comparator;

    struct Leaf { K m_key; V m_value; }; // An item set by the set user
    Leaf* m_leaves; // array of items set by the user, of capacity m_capacity. These are the leaves of the tournament trees.
    Bitset m_active; // keep track which items have been set.

    struct Node { K m_key; uint32_t m_index:31; uint32_t m_active:1; };
    Node* m_inodes; // array with the internal nodes of the tournament tree

    // The winner of the tournament tree
    Node m_winner;

    /**
     * Initialise the tournament tree
     */
    struct InitInfo {
        uint16_t m_fanout; // number of nodes per node
        int16_t m_height; // the height of the tournament tree
        int32_t m_leaves_sz; // the number of items/leaves in the tree
        int32_t m_inodes_sz; // the number of inodes in the tree
        RightmostSubtreeInfo m_rightmost[m_rightmost_sz]; // the size of each subtree
    };
    static InitInfo initialise(uint64_t capacity, uint64_t fanout);
    TournamentTree(InitInfo init);

    /**
     * Get the number of internal nodes in a subtree of the given height
     */
    static uint64_t get_subtree_sz(int fanout, int height);

    /**
     * Completely rebuild the given subtree, from the ground up
     */
    Node rebuild(uint64_t base_inodes, uint64_t base_leaves, int height, bool rightmost);

    /**
     * Extract the next winner from the given subtree
     */
    Node sift(uint32_t index_previous_winner, uint64_t base_inodes, uint64_t base_leaves, int height, bool rightmost);

    /**
     * Extract the next winner from the whole tournament
     */
    void sift();

    /**
     * Dump the content of the tree
     */
    void dump_subtree(std::ostream& out, const Node* parent, uint64_t offset_root, uint64_t offset_leaves, int height, bool rightmost) const;
    static void dump_tabs(std::ostream& out, size_t depth);
    std::string node2str(const Node* node) const;
    std::string node2str(const Node& node, bool print_all = false) const;
    std::string leaf2str(uint64_t offset) const;

public:

    /**
     * Create an empty tournament tree
     * @capacity the maximum number of input sequences/queues that can feed the tree
     * @fanout
     */
    TournamentTree(uint64_t capacity, uint64_t fanout = 32);

    /**
     * Destructor
     */
    ~TournamentTree();

    /**
     * Retrieve the fanout of each node in the tree
     */
    uint64_t fanout() const noexcept;

    /**
     * Retrieve the height of the tournament tree
     */
    int height() const noexcept;

    /**
     * Retrieve the capacity of the tournament tree
     */
    uint64_t capacity() const noexcept;

    /**
     * Set the leaf at the given position, but it doesn't build the tournament tree
     */
    void set(uint64_t position, const K& key, const V& value);

    /**
     * Unset the leaf at the given position, but it doesn't build the tournament tree
     */
    void unset(uint64_t position);

    /**
     * Completely rebuild the tree from the bottom-up
     */
    void rebuild();

    /**
     * Check whether the tournament tree is exhausted. That is, there are no more elements
     * in the feeding queues.
     */
    bool done() const;

    /**
     * Retrieve the current winner of the tournament tree
     */
    std::pair<const K&, const V&> top() const;

    /**
     * Replace the key for the current winner and extract the next winner from the tree.
     */
    void pop_and_replace(const K& key);
    void pop_and_replace(const K& key, const V& value);

    /**
     * Mark as exhausted the queue associated to the current winner and extract the next winner
     */
    void pop_and_unset();

    /**
     * Dump to stdout the content of the tournament tree, for debugging purposes
     * ! N.B. Invoke #rebuild() at least once before to obtain a proper dump !
     */
    void dump(std::ostream& out = std::cout) const;
};


/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
#if defined(DEBUG)
    #define TOURNAMENT_TREE_DEBUG(msg) { std::cout << "[TournamentTree::" << __FUNCTION__ << "] " << msg << std::endl; }
#else
    #define TOURNAMENT_TREE_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
template<typename K, typename V, typename Comparator, int RASz>
TournamentTree<K, V, Comparator, RASz>::TournamentTree(uint64_t capacity, uint64_t node_sz) : TournamentTree(initialise(capacity, node_sz)){ }

template<typename K, typename V, typename Comparator, int RASz>
TournamentTree<K, V, Comparator, RASz>::TournamentTree(InitInfo init) :
    m_fanout(init.m_fanout), m_height(init.m_height), m_capacity(init.m_leaves_sz),
    m_leaves(nullptr), m_active(init.m_leaves_sz), m_inodes(nullptr) {
    memcpy(&m_rightmost, &init.m_rightmost, sizeof(m_rightmost));
    m_leaves = (Leaf*) calloc(init.m_leaves_sz, sizeof(Leaf));
    if(m_leaves == nullptr) throw std::bad_alloc{};
    m_inodes = (Node*) calloc(init.m_inodes_sz, sizeof(Node));
    if(m_inodes == nullptr) { free(m_leaves); m_leaves = nullptr; throw std::bad_alloc{}; }

    m_winner.m_index = 0;
    m_winner.m_active = 0;
}

template<typename K, typename V, typename Comparator, int RASz>
auto TournamentTree<K, V, Comparator, RASz>::initialise(uint64_t capacity, uint64_t fanout) -> InitInfo {
    TOURNAMENT_TREE_DEBUG("capacity: " << capacity << ", fan out: " << fanout);

    if(capacity == 0){ throw std::invalid_argument("The capacity given is zero"); }
    if(capacity > (uint64_t) std::numeric_limits<int32_t>::max()){ throw std::invalid_argument("Invalid capacity: too big"); }
    if(fanout < 2) { throw std::invalid_argument("The tree fanout must be greater or equal than 2"); }
    if(fanout > (uint64_t) std::numeric_limits<uint16_t>::max()){ throw std::invalid_argument("Invalid node size: too big"); }

    InitInfo init;
    init.m_leaves_sz = (int32_t) capacity;
    init.m_inodes_sz = 0;
    init.m_fanout = (uint16_t) fanout;
    memset(&init.m_rightmost, 0, sizeof(init.m_rightmost));

    int height = (capacity > 1) ? floor( log2(capacity -1) / log2(fanout) ) : 0;
    if(height >= static_cast<int>(m_rightmost_sz)){ throw std::invalid_argument("Invalid number of items: too big"); }
    init.m_height = (int16_t) height;

    uint64_t N = capacity;
    do {
        TOURNAMENT_TREE_DEBUG("N: " << N << ", height: " << height);

        // cardinality in root
        uint64_t subtree_sz = pow(fanout, height);
        init.m_rightmost[height].m_root_sz = ceil( static_cast<double>(N) / static_cast<double>(subtree_sz) );
        if(height >= 1) init.m_rightmost[height].m_root_sz--; // because we don't store the winner in the intermediate nodes
        assert(init.m_rightmost[height].m_root_sz > 0);

        if(height >= 1) {
            // find the height of the rightmost subtree
            uint64_t rightmost_subtree_sz = N % subtree_sz;
            if(rightmost_subtree_sz == 0){ rightmost_subtree_sz = subtree_sz; } // the rightmost subtree is actually full
            int rightmost_subtree_height = (rightmost_subtree_sz > 1) ? floor( log2(rightmost_subtree_sz -1) / log2(fanout) ) : 0;
            init.m_rightmost[height].m_right_height = rightmost_subtree_height;

            // memory size of the tree, except the rightmost subtree
            init.m_inodes_sz += init.m_rightmost[height].m_root_sz; // root
            // plus the space used by the full subtrees
            uint64_t full_subtree_sz = get_subtree_sz(fanout, height -1);
            init.m_inodes_sz += full_subtree_sz * (init.m_rightmost[height].m_root_sz); // -1 ignore the rightmost, +1 include the winner subtree

            // next iteration
            N = rightmost_subtree_sz;
            height = rightmost_subtree_height;
        } else {
            N = 0; // done
        }
    } while(N > 0);


#if defined(DEBUG)
    TOURNAMENT_TREE_DEBUG("[InitInfo] fan out: " << init.m_fanout << ", height: " << init.m_height << ", leaves_sz: " << init.m_leaves_sz << ", inodes_sz: " << init.m_inodes_sz);
    for(int i = 0; i < m_rightmost_sz; i++){
        TOURNAMENT_TREE_DEBUG("rightmost[" << i << "]: root_sz: " << init.m_rightmost[i].m_root_sz << ", right_height: " << init.m_rightmost[i].m_right_height);
    }
#endif

    return init;
}

template<typename K, typename V, typename Comparator, int RASz>
TournamentTree<K, V, Comparator, RASz>::~TournamentTree(){
    free(m_inodes); m_inodes = nullptr;
    free(m_leaves); m_leaves = nullptr;
}

template<typename K, typename V, typename Comparator, int RASz>
uint64_t TournamentTree<K, V, Comparator, RASz>::fanout() const noexcept {
    // cast to uint64_t
    return m_fanout;
}

template<typename K, typename V, typename Comparator, int RASz>
uint64_t TournamentTree<K, V, Comparator, RASz>::capacity() const noexcept {
    return m_capacity;
}

template<typename K, typename V, typename Comparator, int RASz>
int TournamentTree<K, V, Comparator, RASz>::height() const noexcept {
    return m_height;
}

template<typename K, typename V, typename Comparator, int RASz>
uint64_t TournamentTree<K, V, Comparator, RASz>::get_subtree_sz(int fanout, int height) {
    // geometric series: (fanout -1) * Sum_k=0^{fanout -1}{ fanout ^ k }
    return ((1.0 - pow(fanout, height)) / (1.0 - static_cast<double>(fanout))) * (fanout -1);
}

template<typename K, typename V, typename Comparator, int RASz>
bool TournamentTree<K, V, Comparator, RASz>::done() const {
    return !(m_winner.m_active);
}

template<typename K, typename V, typename Comparator, int RASz>
std::pair<const K&, const V&> TournamentTree<K, V, Comparator, RASz>::top() const {
    if (!m_winner.m_active) throw std::logic_error("tournament tree exhausted");
    return std::pair<const K&, const V&>{ m_winner.m_key, m_leaves[m_winner.m_index].m_value };
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::set(uint64_t position, const K& key, const V& value){
    if(position >= capacity()) throw std::invalid_argument{"position out of bounds"};
    m_leaves[position].m_key = key;
    m_leaves[position].m_value = value;
    m_active.set((uint32_t) position);
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::unset(uint64_t position){
    if(position >= capacity()) throw std::invalid_argument{"position out of bounds"};
    m_active.unset((uint32_t) position);
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::rebuild() {
    m_winner = rebuild(0, 0, height(), true);
}

template<typename K, typename V, typename Comparator, int RASz>
auto TournamentTree<K, V, Comparator, RASz>::rebuild(uint64_t base_inodes, uint64_t base_leaves, int height, bool rightmost) -> Node {
    uint64_t node_sz = rightmost ? m_rightmost[height].m_root_sz : (height == 0 ? fanout() : fanout() -1);
//    COUT_DEBUG("[h = " << height << "] subtree: " << m_inodes + base_inodes << " [offset: " << (base_inodes) << "], leaves_start_offset: " << base_leaves << ", rightmost: " << rightmost << ", node_sz: " << node_sz);

    Node winner;
    if(height == 0){ // leaves

        Leaf* __restrict leaves = m_leaves;
        winner.m_key = leaves[base_leaves].m_key;
        winner.m_index = base_leaves;
        winner.m_active = m_active[base_leaves];

        // find the minimum among the leaves
        for(uint64_t i = 1; i < node_sz; i++){
            if(!m_active[base_leaves + i]) continue; // ignore inactive entries

            if(!winner.m_active || m_comparator(leaves[base_leaves +i].m_key, winner.m_key) ){
                winner.m_key = leaves[base_leaves +i].m_key;
                winner.m_index = base_leaves + i;
                winner.m_active = true;
            }
        }

    } else { // intermediate nodes
        // the root node of this subtree
        Node* __restrict root = m_inodes + base_inodes;

        // fetch the first candidate
        winner = rebuild(base_inodes + node_sz, base_leaves, height -1, /* rightmost subtree ? */ false);

        // build the intermediate nodes
        const uint64_t full_subtree_sz = get_subtree_sz(fanout(), height -1);
        uint64_t offset_inodes = node_sz + full_subtree_sz;
        uint64_t offset_leaves = pow(fanout(), height);
        for(uint64_t i = 0; i < node_sz; i++){
            bool rightmost_subtree = rightmost && ((i+1) == node_sz);
            root[i] = rebuild(base_inodes + offset_inodes, base_leaves + offset_leaves, rightmost_subtree ? m_rightmost[height].m_right_height : (height -1), rightmost_subtree);

            // update the minimum
            if(root[i].m_active && (!winner.m_active || m_comparator(root[i].m_key, winner.m_key))){
                std::swap(winner, root[i]);
            }

            // next iteration
            offset_inodes += full_subtree_sz;
            offset_leaves += pow(fanout(), height);
        }
    }

    return winner;
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::pop_and_replace(const K& key){
    m_active.set(m_winner.m_index);
    m_winner.m_key = m_leaves[m_winner.m_index].m_key = key;

    // extract the next winner from the tournament tree
    sift();
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::pop_and_replace(const K& key, const V& value){
    m_active.set(m_winner.m_index);
    m_leaves[m_winner.m_index].m_key = key;
    m_leaves[m_winner.m_index].m_value = value;

    // extract the next winner from the tournament tree
    sift();
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::pop_and_unset(){
    m_active.unset(m_winner.m_index);

    // extract the next winner from the tournament tree
    sift();
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::sift() {
    m_winner = sift(m_winner.m_index, 0, 0, height(), true);
}

template<typename K, typename V, typename Comparator, int RASz>
auto TournamentTree<K, V, Comparator, RASz>::sift(uint32_t index_previous_winner, uint64_t base_inodes, uint64_t base_leaves, int height, bool rightmost) -> Node {
    const uint64_t node_sz = rightmost ? m_rightmost[height].m_root_sz : (height == 0 ? fanout() : fanout() -1);

    Node winner;
    if(height == 0){ // leaves
        Leaf* __restrict leaves = m_leaves;
        winner.m_key = leaves[base_leaves].m_key;
        winner.m_index = base_leaves;
        winner.m_active = m_active[base_leaves];

        // find the minimum among the leaves
        for(uint64_t i = 1; i < node_sz; i++){
            if(!m_active[base_leaves + i]) continue; // ignore inactive entries

            if(!winner.m_active || m_comparator(leaves[base_leaves +i].m_key, winner.m_key)){
                winner.m_key = leaves[base_leaves +i].m_key;
                winner.m_index = base_leaves + i;
                winner.m_active = true;
            }
        }

    } else { // intermediate nodes
        // extract the next winner from the previous winner's subtree
        const uint64_t subtree_num_leaves = pow(fanout(), height);
        const uint64_t subtree_num_inodes = get_subtree_sz(fanout(), height -1);
        const uint64_t subtree_id = (static_cast<int64_t>(index_previous_winner) - base_leaves) / subtree_num_leaves;
        bool rightmost_subtree = rightmost && (subtree_id == node_sz);
        winner = sift(
                index_previous_winner,
                base_inodes + node_sz + subtree_id * subtree_num_inodes,
                base_leaves + subtree_id * subtree_num_leaves,
                rightmost_subtree ? m_rightmost[height].m_right_height : (height -1),
                rightmost_subtree
        );

        // fetch the minimum among the keys in the current root
        Node* __restrict root = m_inodes + base_inodes;
        for(uint64_t i = 0; i < node_sz; i++){
            if(root[i].m_active && (!winner.m_active || m_comparator(root[i].m_key, winner.m_key))){
                std::swap(winner, root[i]);
            }
        }
    }

    return winner;
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::dump(std::ostream& out) const {
    std::cout << "[Tournament tree] block size: " << fanout() << ", height: " << height() <<
            ", capacity (number of entries): " << capacity() << ", exhausted: " << std::boolalpha << done() << "\n";
    std::cout << "Winner: " << node2str(m_winner) << std::endl;

    dump_subtree(out, &m_winner, 0, 0 , height(), true);
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::dump_subtree(std::ostream& out, const Node* parent, uint64_t offset_root, uint64_t offset_leaves, int height, bool rightmost) const {
    using namespace std;
    int depth = static_cast<int32_t>(m_height) - height;
    uint64_t node_sz = rightmost ? m_rightmost[height].m_root_sz : (height == 0 ? fanout() : fanout() -1);

    if(height == 0){
        for(uint64_t i = 0; i < node_sz; i++){
            dump_tabs(out, depth);
            out << "Leaf[" << offset_leaves + i << "]: " << leaf2str(offset_leaves +i) << "\n";
        }
    } else {
        const uint64_t subtree_num_leaves = pow(fanout(), height);
        const uint64_t subtree_num_inodes = get_subtree_sz(fanout(), height -1);

        // first, dump the subtree of the winner
        uint64_t subtree_id = (static_cast<int64_t>(parent->m_index) - offset_leaves) / subtree_num_leaves;
        bool rightmost_subtree = rightmost && (subtree_id == node_sz);
        dump_tabs(out, depth);
        out << "Winner(" << height << "): " << node2str(parent) << ", subtree id: " << subtree_id;
        if(rightmost_subtree) out << ", rightmost";
        out << "\n";
        dump_subtree(out, parent,
                offset_root + node_sz + subtree_id * subtree_num_inodes,
                offset_leaves + subtree_id * subtree_num_leaves,
                rightmost_subtree ? m_rightmost[height].m_right_height : (height -1),
                rightmost_subtree
        );

        for(uint64_t i = 0; i < node_sz; i++){
            // identify the subtree
            subtree_id = static_cast<int64_t>(m_inodes[offset_root + i].m_index - offset_leaves) / subtree_num_leaves;

            // is this child the rightmost subtree ?
            bool rightmost_subtree = rightmost && (subtree_id == node_sz);

            // dump the content of the child
            dump_tabs(out, depth);
            out << "Node(" << height << "): " << node2str(m_inodes + offset_root + i) << ", subtree id: " << subtree_id;
            if(rightmost_subtree) out << ", rightmost";
            out << "\n";

            // traverse the child
            dump_subtree(out,
                    /* parent */ m_inodes + offset_root + i,
                    /* base inodes */ offset_root + node_sz + subtree_id * subtree_num_inodes,
                    /* base leaves */ offset_leaves + subtree_id * subtree_num_leaves,
                    /* height */ rightmost_subtree ? m_rightmost[height].m_right_height : (height -1),
                    /* is rightmost ? */ rightmost_subtree
            );
        }
    }
}

template<typename K, typename V, typename Comparator, int RASz>
void TournamentTree<K, V, Comparator, RASz>::dump_tabs(std::ostream& out, size_t depth){
    using namespace std;

    auto flags = out.flags();
    out << std::setw(2*(depth +1)) << setfill(' ') << ' ';
    out.setf(flags);
}

template<typename K, typename V, typename Comparator, int RASz>
std::string TournamentTree<K, V, Comparator, RASz>::node2str(const Node* node) const {
   if(node == nullptr){
       return "{inode nullptr}";
   } else {
       return node2str(*node, true);
   }
}

template<typename K, typename V, typename Comparator, int RASz>
std::string TournamentTree<K, V, Comparator, RASz>::node2str(const Node& node, bool print_all) const{
    std::stringstream ss;
    if(print_all){
        ss << "index: " << node.m_index << ", key: " << node.m_key;
        if(!node.m_active) ss << ", unset";
    } else if(!node.m_active){
        ss << "unset";
    } else {
        ss << "index: " << node.m_index << ", key: " << node.m_key;
    }
    return ss.str();
}

template<typename K, typename V, typename Comparator, int RASz>
std::string TournamentTree<K, V, Comparator, RASz>::leaf2str(uint64_t offset) const {
    assert(offset < capacity());
    std::stringstream ss;
    if(m_active[offset]){
        ss << "key: " << m_leaves[offset].m_key << ", value: " << m_leaves[offset].m_value;
    } else {
        ss << "unset";
    }
    return ss.str();
}

} // namespace
