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

namespace teseo::aux {

/**
 * The value stored in the degree array for undirected graphs
 */
class ItemUndirected{
public:
    uint64_t m_vertex_id; // thea actual vertex id it refers
    uint64_t m_degree; // its associated degree, that is the number of edges attached

    // Initialise an empty item
    ItemUndirected();

    // Initialise the item with the given pair <vertex_id, degree>
    ItemUndirected(uint64_t vertex_id, uint64_t degree = 0);

    // Get a string representation of the item, for debugging purposes
    std::string to_string() const;
};


// Print to stdout the item, for debugging purposes
std::ostream& operator<<(std::ostream& out, const ItemUndirected& item);

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
ItemUndirected::ItemUndirected() : m_vertex_id(0), m_degree(0) { }

inline
ItemUndirected::ItemUndirected(uint64_t vertex_id, uint64_t degree) : m_vertex_id(vertex_id), m_degree(degree) { }

} // namespace
