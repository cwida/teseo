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

#include "teseo/rebalance/rebalanced_leaf.hpp"

#include <cassert>
#include <iostream>
#include <limits>
#include <sstream>

#include "teseo/memstore/leaf.hpp"

using namespace std;

namespace teseo::rebalance {


/*****************************************************************************
 *                                                                           *
 *  RebalancedLeaf                                                           *
 *                                                                           *
 *****************************************************************************/

RebalancedLeaf::RebalancedLeaf(memstore::Leaf* leaf) noexcept : RebalancedLeaf(leaf, 0, leaf != nullptr ? leaf->num_segments() : 0){

}

RebalancedLeaf::RebalancedLeaf(memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end) noexcept :
        m_leaf(leaf), m_window_start(window_start), m_window_length(window_end - window_start), m_flags(0){
    assert(window_start <= (uint64_t) numeric_limits<uint16_t>::max() && "Overflow");
    assert((window_end - window_start) <= (uint64_t) numeric_limits<uint16_t>::max() && "Overflow");
    assert(window_start <= window_end && "Invalid interval");
}

memstore::Leaf* RebalancedLeaf::leaf() const noexcept {
    return m_leaf;
}

uint64_t RebalancedLeaf::window_start() const noexcept {
    return m_window_start;
}

uint64_t RebalancedLeaf::window_end() const noexcept {
    return window_start() + window_length();
}

uint64_t RebalancedLeaf::window_length() const noexcept {
    return m_window_length;
}

void RebalancedLeaf::set_flag(uint32_t flag, int value) noexcept {
    m_flags = (m_flags & ~flag) | (value << __builtin_ctz(flag));
}

int RebalancedLeaf::get_flag(uint32_t flag) const noexcept {
    return static_cast<int>((m_flags & flag) >> __builtin_ctz(flag));
}

void RebalancedLeaf::set_existent() noexcept {
    set_flag(FLAG_EXISTENT, 1);
}

void RebalancedLeaf::set_created() noexcept {
    set_flag(FLAG_CREATED, 1);
}

void RebalancedLeaf::set_removed() noexcept {
    set_flag(FLAG_REMOVED, 1);
}

bool RebalancedLeaf::is_existent() const noexcept {
    return get_flag(FLAG_EXISTENT) != 0;
}

bool RebalancedLeaf::is_created() const noexcept {
    return get_flag(FLAG_CREATED) != 0;
}

bool RebalancedLeaf::is_removed() const noexcept {
    return get_flag(FLAG_REMOVED) != 0;
}

std::string RebalancedLeaf::to_string() const {
    stringstream ss;
    if(leaf() == nullptr){
        ss << "<nullptr>";
    } else {
        ss << "leaf: " << leaf() << ", window: [" << window_start() << ", " << window_end() << ")";
        if(is_existent()){ ss << ", EXISTENT"; }
        if(is_created()){ ss << ", CREATED"; }
        if(is_removed()){ ss << ", MARKED FOR DELETION"; }
    }

    return ss.str();
}

void RebalancedLeaf::dump() const {
    cout << to_string() << endl;
}

std::ostream& operator<<(std::ostream& out, const RebalancedLeaf& leaf){
    out << leaf.to_string();
    return out;
}

} // namespace
