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

#include <bitset>
#include <cinttypes>

namespace teseo::internal::util {

/**
 * A bitset where the capacity can be set at running time and it doesn't feature the
 * hateful interface of std::vector<bool>
 */
class Bitset {
    constexpr static int group_sz = 64; // number of bits
    using stdbitset = std::bitset<group_sz>; // stdlib bitsets are multiple of 8 bytes, at least on my machine

    const uint32_t m_capacity; // total number of bits in the bitset
    uint32_t m_num_bits_set; // number of bits set to 1 in the bitset
    stdbitset* m_sets; // array of std::bitsets

    // Get the std::bitset for the given position
    stdbitset* get_set(uint32_t abs_position) const {
        return m_sets + (abs_position / group_sz);
    }

    // Get the relative position inside the std::bitset
    uint32_t get_pos(uint32_t abs_position) const {
        return abs_position % group_sz;
    }


public:
    /**
     * Create a new bitset with the given size
     */
    Bitset(uint32_t size) : m_capacity(size), m_num_bits_set(0), m_sets(nullptr){
        m_sets = new stdbitset[(m_capacity / group_sz) + (m_capacity % group_sz != 0)];
    }

    /**
     * Destructor
     */
    ~Bitset() {
        delete[] m_sets; m_sets = nullptr;
    }

    /**
     * Set the bit at the given position
     */
    void set(uint32_t position){
        stdbitset* set = get_set(position);
        uint32_t index = get_pos(position);

        if(! set->test(index)){
            m_num_bits_set++;
            set->set(index);
        }
    }

    /**
     * Unset the bit at the given position
     */
    void unset(uint32_t position){
        stdbitset* set = get_set(position);
        uint32_t index = get_pos(position);

        if(set->test(index)){
            m_num_bits_set--;
            set->reset(index);
        }
    }
    void reset(uint32_t position){ // alias
        unset(position);
    }

    /**
     * The number of bits in the set
     */
    uint32_t size() const { return m_capacity; }

    /**
     * Is there any bit set?
     */
    bool any() const { return m_num_bits_set > 0; }
    bool none() const { return !any(); }

    /**
     * Check whether the given bit is set
     */
    bool test(uint32_t position) const {
        stdbitset* set = get_set(position);
        uint32_t index = get_pos(position);
        return set->test(index);
    }
    bool operator[](uint32_t position) const {
        return test(position);
    }

    /**
     * Reset all bits in the set
     */
    void reset(){
        uint32_t num_sets = (m_capacity / group_sz) + (m_capacity % group_sz != 0);
        for(uint32_t i = 0; i < num_sets; i++){
            m_sets[i].reset();
        }
        m_num_bits_set = 0;
    }

    /**
     * Reset all bits in [start, start+length)
     */
    void reset(uint32_t start, uint32_t length){
        stdbitset* __restrict set_start = get_set(start);
        uint32_t pos_start = get_pos(start);
        stdbitset* __restrict set_end = get_set(start + length);
        uint32_t pos_end = get_pos(start + length);

        if(set_start == set_end){
            for(uint32_t i = pos_start; i < pos_end; i++){
                if(set_start->test(i)){
                    set_start->reset(i);
                    m_num_bits_set--;
                }
            }
        } else {
            // clear the bits in set start
            for(size_t i = pos_start; i < group_sz; i++){
                if(set_start->test(i)){
                    set_start->reset(i);
                    m_num_bits_set--;
                }
            }

            // clear the bits in all the sets between set_start and set_end
            stdbitset* __restrict set_it = set_start + 1;
            while(set_it != set_end){
                m_num_bits_set -= set_it->count();
                set_it->reset();
                set_it++;
            }

            // clear the bits in set end
            for(size_t i = 0; i < pos_end; i++){
                if(set_end->test(i)){
                    set_end->reset(i);
                    m_num_bits_set--;
                }
            }
        }
    }
};

} // namespace
