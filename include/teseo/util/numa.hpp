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

namespace teseo::util {

/**
 * Methods to mess up with the NUMA nodes in the system
 */
struct NUMA {

// Validate that the settings determined at compiled time for the StaticConfiguration
// are still valid. In case of failure, the whole program needs to be reconfigured with
// autoconf and recompiled.
static void check_numa_support();

// Allocate a chunk of memory on the first node, i.e. node 0.
// If numa support is not available, fall back to standard malloc
static void* malloc(uint64_t size);

// Allocate a chunk of memory on the given node
// If numa support is not available, it will raise an exception InternalError
static void* malloc(uint64_t size, int node);

// Free the memory previously allocated with #numa_malloc
static void free(void* pointer);

// Copy the amount node previously allocated with #numa_malloc to the specific node
// If numa support is disabled, it will raise an exception InternalError
static void* copy(void* pointer, int node);

};

}
