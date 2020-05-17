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

#include "teseo/util/latch.hpp"

#include <ostream>
#include <string>

namespace teseo::aux {

class AuxiliaryView; // forward declaration

/**
 * Cache for the last created view. Used by the global_context
 */
class Cache {
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    mutable util::Latch m_latch; // to provide thread-safety
    uint64_t m_transaction_id; // the read ID associated to the last created view
    aux::AuxiliaryView* m_view; // the last created view

public:
    // Init the cache
    Cache();

    // Destructor
    ~Cache();

    // Retrieve the cached view, if suitable for the given transaction id
    AuxiliaryView* get(uint64_t transaction_id, uint64_t highest_txn_rw_id);

    // Update the last saved view
    void set(aux::AuxiliaryView* view, uint64_t transaction_id);

    // Retrieve a representation of this instance, for debugging purposes
    std::string to_string() const;

    // Dump the content of this instance to stdout, for debugging purposes
    void dump() const;
};

// Dump the content of this instance to the passed output stream, for debugging purposes
std::ostream& operator<<(std::ostream& out, const Cache& cache);

} // namespace
