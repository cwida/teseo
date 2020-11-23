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

// Internal handling
//#define MAYBE_BREAK_INTO_DEBUGGER_ENABLED

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

/**
 * Parts of the code programmatically cause to jump into the debugger when some conditions are hit, e.g. an
 * unexpected exception is thrown.
 * To enable these jumps both the macro MAYBE_BREAK_INTO_DEBUGGER_ENABLED must be statically enabled and the
 * global variable maybe_break_into_debugger_enabled must be set to true at runtime, using:
 *      context::global_context()->set_break_into_debugger(true);
 */
#if defined(MAYBE_BREAK_INTO_DEBUGGER_ENABLED)
extern bool maybe_break_into_debugger_enabled;
#define MAYBE_BREAK_INTO_DEBUGGER if(::teseo::util::maybe_break_into_debugger_enabled){ ::teseo::util::break_into_debugger(); }
#else
#define MAYBE_BREAK_INTO_DEBUGGER
#endif

/**
 * Synchronously raise a SIGTRAP and jump the execution to the debugger
 */
void break_into_debugger();

}


