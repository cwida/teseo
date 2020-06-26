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

#include "teseo/util/interface.hpp"

#include "teseo/memstore/error.hpp"
#include "teseo/memstore/key.hpp"
#include "teseo/util/error.hpp"

using namespace std;

namespace teseo::util {

void handle_error(const memstore::Error& error){
    using namespace memstore;

    const uint64_t source = error.m_key.source() -1; // external vertex 0 -> internal vertex 1
    const uint64_t destination = error.m_key.destination() -1; // external vertex 0 -> internal vertex 1

    switch(error.m_type){
    case Error::VertexLocked:
        RAISE(TransactionConflict, "Conflict detected, the vertex " << source << " is currently locked by another transaction. "
                "Restart this transaction to alter this object");
    case Error::VertexAlreadyExists:
        VERTEX_ERROR(source, "The vertex " << source << " already exists");
    case Error::VertexDoesNotExist:
        VERTEX_ERROR(source, "The vertex " << source << " does not exist");
    case Error::VertexPhantomWrite:
        RAISE(TransactionConflict, "Conflict detected, phantom write detected for the vertex " << source);
    case Error::VertexInvalidLogicalID:
        VERTEX_ERROR(source, "Invalid logical vertex identifier: " << source);
    case Error::EdgeLocked:
        RAISE(TransactionConflict, "Conflict detected, the edge " << source << " -> " << destination << " is currently locked "
                "by another transaction. Restart this transaction to alter this object");
    case Error::EdgeAlreadyExists:
        EDGE_ERROR(source, destination, "The edge " << source << " -> " << destination << " already exists");
    case Error::EdgeDoesNotExist:
        EDGE_ERROR(source, destination, "The edge " << source << " -> " << destination << " does not exist");
    case Error::EdgeSelf:
        EDGE_ERROR(source, destination, "Edges having the same source and destination are not supported: " << source << " -> " << destination);
    default:
        RAISE(InternalError, "Error type not registered");
    }
}

} // namespace


