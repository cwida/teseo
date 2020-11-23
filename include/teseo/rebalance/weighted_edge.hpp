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

#include <cstdint>
#include <ostream>

// Forward declarations
namespace teseo::memstore {
class Version;
class Vertex;
} // namespace

namespace teseo::rebalance {

/**
 * The representation of an edge in a Rebalancer scratchpad
 */
class WeightedEdge {
public:
    uint64_t m_destination;
    double m_weight;

    // Get a string representation of this edge, for debugging purposes
    std::string to_string(const memstore::Vertex* source, const memstore::Version* version) const;
};

} // namespace
