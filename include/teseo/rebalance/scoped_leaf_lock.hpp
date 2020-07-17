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

// forward declarations
namespace teseo::memstore { class Leaf; }

namespace teseo::rebalance {

/**
 * This class acquires and releases all segment latches in a given leaf in the RAII pattern.
 *
 * When leaves are created by a rebalancer, during a split operation, it needs to be ensured that no
 * other reader or writer can access any of the segment being constructed and access the
 * rebalancer itself.
 */
class ScopedLeafLock {
    ScopedLeafLock(const ScopedLeafLock&) = delete;
    ScopedLeafLock& operator=(const ScopedLeafLock&) = delete;


    memstore::Leaf* m_leaf; // the leaf whose all segments need to be locked
public:
    /**
     * Acquire a writer latch in all segments for the given leaf
     */
    ScopedLeafLock(memstore::Leaf* leaf);

    /**
     * Move constructor
     */
    ScopedLeafLock(ScopedLeafLock&& other);

    /**
     * Release all latches in the leaf
     */
    ~ScopedLeafLock();
};

} // namespace
