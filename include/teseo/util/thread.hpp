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
#include <string>

namespace teseo::util {

/**
 * Utility methods to operate on threads
 */
struct Thread {

/**
 * Get the Linux thread ID, that is the identifier shown by the debugger
 */
static int64_t get_thread_id();

/**
 * Get the CPU where the current thread is executing
 */
static int get_cpu_id();

/**
 * Get the NUMA node for the current thread
 */
static int get_numa_id();

/**
 * Get the process ID associated to this process
 */
static int64_t get_process_id();

/**
 * Set the name of the current thread. The given name will appear in the debugger thread list.
 */
static void set_name(const std::string& name);

/**
 * Get the name of the current thread
 */
static std::string get_name();

};

} // namespace
