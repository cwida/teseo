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


#include <stdexcept>
#include <string>

namespace teseo {

/*****************************************************************************
 *                                                                           *
 *   Exceptions & errors                                                     *
 *                                                                           *
 *****************************************************************************/

/**
 * All exceptions thrown by Teseo are instances of the class or one of the subclasses of Error.
 */
class Exception : public std::runtime_error {
    const std::string m_class; // The actual class of this exception
    const char* m_file; // The file path where the exception has been thrown
    int m_line; // The line in the file that thrown the exception
    const char* m_function; // The funciton where the exception originated

public:
    Exception(const std::string& exc_class, const std::string& message, const char* file, int line, const char* function);

    int line() const;

    const char* file() const;

    const char* function() const;

    const char* exception_class() const;
};

/**
 * Print to the output stream a description of the given error
 */
std::ostream& operator<<(const Exception& error, std::ostream& out);

} // namespace
