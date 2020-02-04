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
    const std::string m_file; // The file path where the exception has been thrown
    int m_line; // The line in the file that thrown the exception
    const std::string m_function; // The funciton where the exception originated

public:
    Exception(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);

    int line() const;

    const std::string& file() const;

    const std::string& function() const;

    const std::string& exception_class() const;
};

/**
 * A logical error, due to the incorrect usage of the API or an incosistent state of the transaction
 */
class LogicalError : public Exception {
public:
    LogicalError(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);
};


/**
 * Print to the output stream a description of the given error
 */
std::ostream& operator<<(const Exception& error, std::ostream& out);

} // namespace
