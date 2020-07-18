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
#include "teseo/rebalance/scoped_leaf_lock.hpp"

#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/segment.hpp"

using namespace std;

namespace teseo::rebalance {

ScopedLeafLock::ScopedLeafLock(memstore::Leaf* leaf) : m_leaf(leaf) {
    for(uint64_t i = 0, end = m_leaf->num_segments(); i < end; i++){
        memstore::Segment* segment = m_leaf->get_segment(i);
        segment->writer_init_xlock();
    }
}

ScopedLeafLock::ScopedLeafLock(ScopedLeafLock&& other) : m_leaf(other.m_leaf){ // needed for the <vector>
    other.m_leaf = nullptr;
}

ScopedLeafLock::~ScopedLeafLock()  {
    if(m_leaf != nullptr){
        for(uint64_t i = 0, end = m_leaf->num_segments(); i < end; i++){
            memstore::Segment* segment = m_leaf->get_segment(i);
            segment->writer_exit();
        }
    }
}

} // namespace
