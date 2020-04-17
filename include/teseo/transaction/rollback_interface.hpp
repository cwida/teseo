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

#include <string>

namespace teseo::transaction {

class Undo; // forward declaration

/**
 * Interface invoked by a transaction on a data structure to roll back a change stored in the undo
 * buffer.
 */
class RollbackInterface {
public:
    // Destructor placeholder
    virtual ~RollbackInterface();

    // Rollback a previously performed object
    // @param object the opaque item stored in the undo object, to reconstruct the change to revert
    // @param next the next item in the undo chain list, if anyone
    virtual void do_rollback(void* object, Undo* next) = 0;

    // Retrieve a string representation of the undo payload, for debugging purposes
    virtual std::string str_undo_payload(const void* object) const;
};

}
