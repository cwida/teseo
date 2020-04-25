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

#include <mutex>
#include <string>

#include "teseo/util/thread.hpp"

namespace teseo::util {

/**
 * Get a string representation of the class & method where the macro is invoked
 */
#define DEBUG_WHOAMI ::teseo::util::debug_function_name(__PRETTY_FUNCTION__)

/**
 * Print to stdout the given message, but only iff the macro DEBUG is defined
 */
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<std::mutex> lock(::teseo::util::g_debugging_mutex); std::cout << "[" << DEBUG_WHOAMI << "] [" << ::teseo::util::Thread::get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/**
 * Mutex, to avoid concurrently clogging the stdout
 */
extern std::mutex g_debugging_mutex;

/**
 * Internal function to retrieve the class & function name of the given invocation
 */
std::string debug_function_name(const char* pretty_name);
}


