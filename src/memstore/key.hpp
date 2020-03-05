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

namespace teseo::internal::memstore {

/**
 * A single separator key in the SparseArray consists of an edge, that is a pair <source, destination>
 */
class Key {
    uint64_t m_source; // the source of the edge
    uint64_t m_destination; // the destination of the edge

public:
    // Constructor
    Key(); // an invalid key, a pair <int_max, int_max>
    Key(uint64_t vertex_id); // vertex_id -> 0 represents the start of all items for the given vertex
    Key(uint64_t source, uint64_t destination); // the edge source -> destination

    uint64_t get_source() const;
    uint64_t get_destination() const;
    void set(uint64_t vertex_id);
    void set(uint64_t source, uint64_t destination);
    bool operator==(const Key& other) const;
    bool operator!=(const Key& other) const;
    bool operator<(const Key& other) const;
    bool operator<=(const Key& other) const;
    bool operator>(const Key& other) const;
    bool operator>=(const Key& other) const;
    static Key min();
    static Key max();
};


// The minimum and maximum key in the universe
extern Key KEY_MIN;
extern Key KEY_MAX;

std::ostream& operator<<(std::ostream& out, const Key& key);

}

