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

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <limits>

#include "teseo/memstore/index_entry.hpp"

namespace teseo::gc { class GarbageCollector; }

namespace teseo::aux {

/**
 * Invocations to the API of AuxiliaryView never throw exceptions. Rather, if
 * a vertex does not exist, return the special value NOT_FOUND
 */
constexpr static uint64_t NOT_FOUND = std::numeric_limits<uint64_t>::max();

/**
 * A materialised view to quickly fetch the rank (the logical ID) of a vertex and the
 * total number of attached edges.
 */
class View {
    View(const View&) = delete;
    View& operator=(const View&) = delete;

    const bool m_is_static; // Is the subclass a static or a dynamic view?
    std::atomic<int> m_ref_count = 1; // number of references to the class

protected:
    // Destructor, it must be implicitly invoked by #decr_ref_count
    virtual ~View();

public:
    // Initialise the class
    View(bool is_static);

    // Retrieve the actual vertex ID associated to the logical ID
    // Return NOT_FOUND if the logical_id does not exist
    uint64_t vertex_id(uint64_t logical_id) const noexcept;

    // Retrieve the logical ID associated to the vertex ID
    // Return NOT_FOUND if vertex_id does not exist
    uint64_t logical_id(uint64_t vertex_id) const noexcept;

    // Retrieve the degree associated to the given vertex
    // Return NOT_FOUND if the vertex does not exist
    uint64_t degree(uint64_t id, bool is_logical) const noexcept;

    // Retrieve the total number of vertices in the view
    uint64_t num_vertices() const noexcept;

    // Manage the number of incoming pointers to the class
    void incr_ref_count() noexcept;
    void decr_ref_count() noexcept;
};

} // namespace

