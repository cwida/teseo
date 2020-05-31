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
#include <mutex>

#include "teseo/util/abtree.hpp"

namespace teseo::memstore{ class Key; } // forward declaration

namespace teseo::aux {

class CountingTree; // forward declaration
class ItemUndirected; // forward declaration
class PartialResult; // forward declaration

/**
 * Class to create the final degree vectors out of a collection of partial results.
 */
class Builder{
    Builder(const Builder&) = delete;
    Builder& operator=(const Builder&) = delete;

    uint64_t m_num_partial_results; // the total number of instances of PartialResults issued
    std::mutex m_mutex; // to guarantee thread-safety
    std::condition_variable m_condvar; // to signal the builder a new item is available
    teseo::util::ABTree</* id */ uint64_t, PartialResult*> m_queue; // the available partial results, as collected from the workers
    uint64_t m_num_collected_results; // total number of items fetched from the queue, so far

public:
    // Init the builder
    Builder();

    // Destructor
    ~Builder();

    // Create a new partial result to be computed
    PartialResult* issue(const memstore::Key& from, const memstore::Key& to);

    // Collect a partial result previously issued
    void collect(PartialResult* partial_result);

    // Fetch the next item from the queue. Return NULL if the queue has been exhausted.
    // Remember to explicitly deallocate the item retrieved once used.
    PartialResult* next();

    // Create the degree vector
    ItemUndirected* create_dv_undirected(uint64_t num_vertices);

    // Create a counting tree
    CountingTree* create_ct_undirected();
};

} // namespace
