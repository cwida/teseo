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
#include <cinttypes>

namespace teseo::aux {

/**
 * A snapshot that allows to quickly fetch the rank (the logical ID) of a vertex and the
 * total number of attached edges.
 */
class AuxiliarySnapshot {
    AuxiliarySnapshot(const AuxiliarySnapshot&) = delete;
    AuxiliarySnapshot& operator=(const AuxiliarySnapshot&) = delete;

    std::atomic<int> m_ref_count = 1; // number of references to the class

public:
    // Initialise the class
    AuxiliarySnapshot();

    // Destructor
    virtual ~AuxiliarySnapshot();

    // Retrieve the actual vertex ID associated to the logical ID
    virtual uint64_t vertex_id(uint64_t logical_id) const = 0;

    // Retrieve the logical ID associated to the vertex ID
    virtual uint64_t logical_id(uint64_t vertex_id) const = 0;

    // Retrieve the degree associated to the given vertex
    virtual uint64_t degree(uint64_t id, bool is_logical) const = 0;

    // Manage the number of incoming pointers to the class
    void incr_ref_count();
    void decr_ref_count();
};


} // namespace

