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

#include "teseo/context/static_configuration.hpp" // numa_num_nodes
#include "teseo/util/latch.hpp"

#include <ostream>
#include <string>

namespace teseo::gc {  class GarbageCollector; }

namespace teseo::aux {

class StaticView; // forward declaration

/**
 * Cache for the last created view. Used by the global_context
 */
class Cache {
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    constexpr static uint64_t NUM_NODES = context::StaticConfiguration::numa_num_nodes;
    static_assert(NUM_NODES >= 1, "We expect to have at least one memory node available");

    mutable util::Latch m_latch; // to provide thread-safety
    uint64_t m_transaction_id; // the read ID associated to the last created view
    aux::StaticView* m_views[NUM_NODES]; // the last created view
    gc::GarbageCollector* m_garbage_collector; // to remove the references to leaves

    // Remove the previously cached views
    void unset(); // the latch must be held by the invoker

public:
    // Init the cache
    Cache(gc::GarbageCollector* garbage_collector);

    // Destructor
    ~Cache();

    // Retrieve the cached views, if suitable for the given transaction id
    bool get(uint64_t transaction_id, uint64_t highest_txn_rw_id, aux::StaticView** output);

    // Update the last saved views
    void set(aux::StaticView** views, uint64_t transaction_id);

    // Retrieve a representation of this instance, for debugging purposes
    std::string to_string() const;

    // Dump the content of this instance to stdout, for debugging purposes
    void dump() const;
};

// Dump the content of this instance to the passed output stream, for debugging purposes
std::ostream& operator<<(std::ostream& out, const Cache& cache);

} // namespace
