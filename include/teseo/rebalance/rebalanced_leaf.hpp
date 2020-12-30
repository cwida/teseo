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

#include <cinttypes>
#include <ostream>
#include <string>

namespace teseo::memstore { class Leaf; } // forward declaration

namespace teseo::rebalance {

/**
 * The state of a single leaf rebalanced. Used internally by the SpreadOperator to keep track
 * of the locks used in a leaf
 */
class RebalancedLeaf {
    memstore::Leaf* const m_leaf; // pointer to the underlying leaf
    const uint16_t m_window_start; // the first segment rebalanced in the leaf
    const uint16_t m_window_length; // total number of segments rebalanced
    uint32_t m_flags = 0; // whether the leaf already exists, has been just created or it is due to deletion

    const uint32_t FLAG_EXISTENT = 0x1; // the leaf already exists
    const uint32_t FLAG_CREATED = 0x2; // this leaf has just been created by the spread operator
    const uint32_t FLAG_REMOVED = 0x4; // this leaf must be removed

    // Set the given flag
    void set_flag(uint32_t flag, int value) noexcept;

    // Get the value associated to the given flag
    int get_flag(uint32_t flag) const noexcept;

public:
    // Create the state for the given leaf
    RebalancedLeaf(memstore::Leaf* leaf) noexcept;

    // Create the state for the given leaf. Explicitly set the window being rebalanced.
    RebalancedLeaf(memstore::Leaf* leaf, uint64_t /* incl */ window_start, uint64_t /* excl */ window_end) noexcept;

    // Obtain a pointer to the leaf being rebalanced
    memstore::Leaf* leaf() const noexcept;

    // Get the start of the window being rebalanced (inclusive)
    uint64_t window_start() const noexcept;

    // Get the end of the window being rebalanced (exclusive)
    uint64_t window_end() const noexcept;

    // Get the number of the segments in the window being rebalanced
    uint64_t window_length() const noexcept;

    // Mark the window as already existing
    void set_existent() noexcept;

    // Mark the window as just been created by a spread operation
    void set_created() noexcept;

    // Mark the window for deletion
    void set_removed() noexcept;

    // Check whether this leaf already exists
    bool is_existent() const noexcept;

    // Check whether this leaf was marked as just been created
    bool is_created() const noexcept;

    // Check whether this leaf was marked for deletion
    bool is_removed() const noexcept;

    // Get a string representation of this instance, for debugging purposes
    std::string to_string() const;

    // Dump the content of this instance to stdout, for debugging purposes
    void dump() const;
};

// Dump the content of the state of the leaf to the given output stream
std::ostream& operator<<(std::ostream& out, const RebalancedLeaf& leaf);

} // namespace
