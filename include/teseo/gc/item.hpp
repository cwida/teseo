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
#include <string>

namespace teseo::gc {

/**
 * A single entry in the garbage collector
 */
class Item {
    uint64_t m_timestamp; // the timestamp when this object has been added to the garbage collector
    void* m_pointer; // object to be deleted
    void (*m_deleter)(void*); // the function that can remove the pointer

public:
    // Create a dummy entry
    Item();

    // Create a new entry
    Item(void* pointer, void (*deleter)(void*));

    // Process this entry, that is invoke the deleter on the pointer
    void process();

    // Process this entry only iff its timestamp is less than the given epoch
    bool process_if(uint64_t epoch);

    // Get the pointer to deallocate, only used for debugging & testing purposes
    void* pointer() const;

    // Retrieve a string representation of the item, for debugging purposes
    std::string to_string() const;
};

// Dump the content of the item to the stdout, for debugging purposes
std::ostream& operator<<(std::ostream& out, const Item& item);

} // namespace
