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

#include "error.hpp"

using namespace std;

namespace teseo {

Exception::Exception(const std::string& exc_class, const std::string& message, const char* file, int line, const char* function)
    : runtime_error(message), m_class(exc_class), m_file(file), m_line(line), m_function(function){ }

const char* Exception::file() const{ return m_file; }
int Exception::line() const{ return m_line; }
const char* Exception::function() const{ return m_function; }
const char* Exception::exception_class() const { return m_class.c_str(); }

std::ostream& operator<<(std::ostream& out, Exception& e){
    out << "[" << e.exception_class() << ": " << e.what() << " - Raised at: `" << e.file() << "', "
            "line: " << e.line() << ", function: `" << e.function() << "']";
    return out;
}

} // namespace

namespace teseo::internal {

// globals
stringstream exception_stream;
mutex exception_mutex;

}
