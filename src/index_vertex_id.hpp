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

namespace teseo::implementation {

class IndexVertexID {

    // A generic node in the
    class Node {

    };


public:
    void insert(uint64_t vertex_id, int64_t count, void* value);

    void update_key(uint64_t vertex_id_old, uint64_t vertex_id_new, int64_t count_diff);

    void update_count(uint64_t vertex_id, int64_t count_diff);

    bool remove(uint64_t vertex_id);

    void* get_value_by_logical_id(uint64_t logical_id) const;

    void* get_value_by_real_id(uint64_t vertex_id) const;

    /**
     * Get the total count stored in the tree
     */
    uint64_t get_total_count() const;

    /**
     * Dump the whole content of the tree to the stdout, for debugging purposes
     * This method is not thread-safe.
     */
    void dump() const;
};

} // namespace

