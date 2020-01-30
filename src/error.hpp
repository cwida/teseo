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

#include "teseo.hpp"

#include <mutex>
#include <sstream>

namespace teseo::internal {

extern std::stringstream exception_stream; // helper to create the exception message
extern std::mutex exception_mutex; // mutual exclusion to create the exception message


/**
 * It prepares the arguments `file', `line', `function', and `what' to be passed to an exception ctor
 * @param msg the message stream to concatenate
 */
#define RAISE_EXCEPTION_CREATE_ARGUMENTS(msg) const char* file = __FILE__; int line = __LINE__; const char* function = __FUNCTION__; \
        std::unique_lock<std::mutex> lock(::teseo::internal::exception_mutex); \
        auto& stream = ::teseo::internal::exception_stream; \
        stream.str(""); stream.clear(); \
        stream << msg; \
        std::string what = stream.str(); \
        stream.str(""); stream.clear(); /* reset once again */

/**
 * Raises an exception with the given message
 * @param exception the exception to throw
 * @param msg: an implicit ostream, with arguments concatenated with the symbol <<
 */

#define RAISE_EXCEPTION(exc, msg) { RAISE_EXCEPTION_CREATE_ARGUMENTS(msg); throw exc( #exc, what, file, line, function); }
#define RAISE(exc, msg) RAISE_EXCEPTION(exc, msg)


/**
 * These exception classes are so similar, so define a general macro to create the exception
 */
#define DEFINE_EXCEPTION1( exceptionName, exceptionSubClass ) class exceptionName: public exceptionSubClass { \
        public: exceptionName(const std::string& exceptionClass, const std::string& message, const std::string& file, \
                int line, const std::string& function) : \
                    exceptionSubClass(exceptionClass, message, file, line, function) { } \
} /* End of DEFINE_EXCEPTION1 */
#define DEFINE_EXCEPTION0( exceptionName ) DEFINE_EXCEPTION1( exceptionName, teseo::Exception )

/**
 * The exception type to throw when using the macro `ERROR'
 */
#define CURRENT_ERROR_TYPE ::teseo::Exception

/**
 * Helper for the macro `ERROR' to fully expand the definition CURRENT_ERROR_TYPE before
 * invoking RAISE_EXCEPTION
 */
#define _RAISE_EXCEPTION(exc, msg) RAISE_EXCEPTION(exc, msg)

/**
 * Convenience macro, it raises an exception of type `Error'.
 */
#define ERROR(message) _RAISE_EXCEPTION(CURRENT_ERROR_TYPE, message)


/**
 * Any internal logical error
 */
DEFINE_EXCEPTION0(InternalError);

}
