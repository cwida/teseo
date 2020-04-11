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
#include <condition_variable>
#include <thread>

#include "key.hpp"
#include "latch.hpp"
#include "util/circular_array.hpp"

namespace teseo::internal::memstore {

class SparseArray; // forward decl.

/**
 * A background service to asynchronously rebalance gates upon request of writers
 */
class AsyncRebalancerService {
    SparseArray* const m_sparse_array; // the sparse array where to operate
    SpinLock m_mutex; // synchronisation with the background thread
    std::condition_variable_any m_condvar; // synchronisation with the background thread
    internal::util::CircularArray<Key> m_requests; // we don't expect too many requests to justify an unordered map
    std::thread m_thread; // handle to the background thread
    const int64_t SINGLE_GATE_THRESHOLD; // threshold when to switch rebalancing a single gate or multiple gates

    // Event loop in the background thread
    void main_thread();

    // Rebalance the gate identified by the given key
    void handle_request(Key key);

public:
    // Create the service
    AsyncRebalancerService(SparseArray* sparse_array);

    // Destructor
    ~AsyncRebalancerService();

    // Start the service
    void start();

    // Stop the service
    void stop();

    // Request to asynchronously rebalance the gate identified by the given key
    void request(Key key);
};

} // namespace
