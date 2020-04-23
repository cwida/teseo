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

#include <chrono>
#include <cinttypes>
#include <limits>
#include <string>

namespace teseo::context {

/**
 * Configuration parameters that need to be set at compile time
 */
struct StaticConfiguration {
    /**
     * The height of the calibrator tree [0 => log2(num_segments) ]
     */
    constexpr static uint64_t crawler_calibrator_tree_height = 0;

    /**
     * The number of segments in each leaf of the memstore
     */
    constexpr static uint64_t memstore_num_segments_per_leaf = 512;

    /**
     * The size of each segment, as multiple of sizeof(uint64_t)
     */
    constexpr static uint64_t memstore_segment_size = 256;

    /**
     * What is the lifetime of a computed active transactions list kept locally in a thread context
     */
    constexpr static std::chrono::milliseconds tctimer_txnlist_lifetime { 60 }; // ms

    /**
     * The fill factor, in [0, 1], on when a memory pool can be reused by another thread
     */
    constexpr static double transaction_memory_pool_ffreuse = 0.25;

    /**
     * The capacity of each memory pool, in terms of number of transactions.
     */
    constexpr static uint32_t transaction_memory_pool_size = 1024;
    static_assert(transaction_memory_pool_size <= std::numeric_limits<uint16_t>::max(), "Free spots in the memory pool are stored as uint16_t");

    /**
     * The default size, in bytes, of an undo buffer created by a transaction
     */
    constexpr static uint32_t transaction_undo_buffer_size = 4096;

    /**
     * The size, in bytes, of the first undo stored inside a transaction. When a transaction
     * is created, we always embed a small undo buffer with it of the following size:
     */
    constexpr static uint32_t transaction_undo_embedded_size = 64;
};

} // namespace
