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

namespace teseo::internal {

#include <cinttypes>
#include <iostream>
#include <string>

class Storage {
public:

    /**
     * A single key in the StaticIndex consists of an edge, that is a pair <source, destination>
     */
    class Key {
        uint64_t m_source; // the source of the edge
        uint64_t m_destination; // the destination of the edge

    public:
        // Constructor
        Key(); // an invalid key, a pair <int_max, int_max>
        Key(uint64_t vertex_id); // vertex_id -> 0 represents the start of all items for the given vertex
        Key(uint64_t source, uint64_t destination); // the edge source -> destination

        uint64_t get_source() const;
        uint64_t get_destination() const;
        void set(uint64_t vertex_id);
        void set(uint64_t source, uint64_t destination);
        bool operator==(const Key& other);
        bool operator!=(const Key& other);
        bool operator<(const Key& other);
        bool operator<=(const Key& other);
        bool operator>(const Key& other);
        bool operator>=(const Key& other);
        static Key min();
        static Key max();
    };

    /**
     * A static index with a fixed number of indexed entries. To change the number of entries
     * the whole index needs to be rebuilt (#rebuild(N)) providing the new size of entries.
     *
     * The node size B is determined on initialisation. A node size B actually requires B -1 slots
     * in terms of space, so it is recommended to set B to a power of 2 + 1 (e.g. 65) to fully
     * exploit aligned accesses to the cache.
     *
     * Note: this code has been adapted from the pma_comp driver. Nevertheless, here, the index
     * is always embedded into a leaf of B+Tree. The space for the keys is always assumed to be
     * enough to hold whatever size of the index is requested and __ALWAYS__ starts at the end
     * of the class instance.
     */
    class StaticIndex {
        StaticIndex(const StaticIndex&) = delete;
        StaticIndex& operator=(const StaticIndex&) = delete;

        const uint16_t m_node_size; // number of keys per node
        int16_t m_height; // the height of this tree
        int32_t m_capacity; // the number of segments/keys in the tree (note, unrelated to the actual space allocated in advance in the B+ tree leaf)
        Key* const m_keys; // the container of the keys => always equal to this + sizeof(StaticIndex)
        Key m_key_minimum; // the minimum key stored in the tree

        /**
         * Keep track of the cardinality and the height of the rightmost subtrees
         */
        struct RightmostSubtreeInfo {
            uint16_t m_root_sz; // the number of elements in the root
            uint16_t m_right_height; // the height of the rightmost subtree
        };
        constexpr static uint64_t m_rightmost_sz = 8;
        RightmostSubtreeInfo m_rightmost[m_rightmost_sz];

    protected:
        // Retrieve the slot associated to the given segment
        Key* get_slot(uint64_t segment_id) const;

        // Dump the content of the given subtree
        void dump_subtree(std::ostream& out, Key* root, int height, bool rightmost, Key fence_min, Key fence_max, bool* integrity_check) const;
        static void dump_tabs(std::ostream& out, int depth);

    public:
        /**
         * Initialise the index with the given node size and capacity
         */
        StaticIndex(uint64_t node_size, uint64_t num_segments = 1);

        /**
         * Destructor
         */
        ~StaticIndex();

        /**
         * Rebuild the tree to contain `num_segments'
         */
        void rebuild(uint64_t num_segments);

        /**
         * Set the separator key associated to the given segment
         */
        void set_separator_key(uint64_t segment_id, Key key);

        /**
         * Get the separator key associated to the given segment.
         * Used only for the debugging purposes.
         */
        Key get_separator_key(uint64_t segment_id) const;

        /**
         * Return a segment_id that contains the given key. If there are no repetitions in the indexed data structure,
         * this will be the only candidate segment for the given key.
         */
        uint64_t find(Key key) const noexcept;

        /**
         * Return the first segment id that may contain the given key
         */
        uint64_t find_first(Key key) const noexcept;

        /**
         * Return the last segment id that may contain the given key
         */
        uint64_t find_last(Key key) const noexcept;

        /**
         * Retrieve the minimum stored in the tree
         */
        Key minimum() const noexcept;

        /**
         * Retrieve the height of the current static tree
         */
        int height() const noexcept;

        /**
         * Retrieve the block size of each node in the tree
         */
        int64_t node_size() const noexcept;
//
//        /**
//         * Retrieve the memory footprint of this index, in bytes
//         */
//        size_t memory_footprint() const;

        /**
         * Dump the fields of the index
         */
        void dump(std::ostream& out, bool* integrity_check = nullptr) const;
        void dump() const;
    };



};

std::ostream& operator<<(std::ostream& out, const Storage::Key& key);

} // namespace
