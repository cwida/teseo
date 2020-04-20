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

#include "key.hpp"

namespace teseo::memstore {


/**
 * The collection of exceptions that can be thrown during an update in the memstore
 */
struct Error {
    Key m_key; // pair <source, vertex>

    enum Type {
        VertexLocked, // -> Transaction conflict
        VertexAlreadyExists,
        VertexDoesNotExist,
        VertexPhantomWrite,
        EdgeLocked, // -> Transaction conflict
        EdgeAlreadyExists,
        EdgeDoesNotExist,

    };

    Type m_type;

    Error(Key key, Type type) : m_key(key), m_type(type) {}
};


/**
 * Internal exception thrown by SparseFile#insert_edge() when it's not sure whether the source vertex exists
 * in the sparse array. The caller should verify the existence of the vertex and then invoke the method
 * again #insert_edge() disabling the flag for this check.
 */
struct NotSureIfItHasSourceVertex{};

}
