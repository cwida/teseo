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

#include "teseo/util/error.hpp"

using namespace std;

namespace teseo {

Exception::Exception(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function)
    : runtime_error(message), m_class(exc_class), m_file(file), m_line(line), m_function(function){ }

const std::string& Exception::file() const{ return m_file; }
int Exception::line() const{ return m_line; }
const std::string& Exception::function() const{ return m_function; }
const std::string& Exception::exception_class() const { return m_class; }

std::ostream& operator<<(std::ostream& out, Exception& e){
    out << "[" << e.exception_class() << ": " << e.what() << " - Raised at: `" << e.file() << "', "
            "line: " << e.line() << ", function: `" << e.function() << "']";
    return out;
}

LogicalError::LogicalError(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function) :
    Exception(exc_class, message, file, line, function){ }

TransactionConflict::TransactionConflict(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function) :
    Exception(exc_class, message, file, line, function){ }

VertexError::VertexError(uint64_t vertex, const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function) :
        LogicalError(exc_class, message, file, line, function), m_vertex(vertex) { }

uint64_t VertexError::vertex() const noexcept { return m_vertex; }

EdgeError::EdgeError(uint64_t source, uint64_t destination, const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function):
    LogicalError(exc_class, message, file, line, function), m_source(source), m_destination(destination) { }

uint64_t EdgeError::source() const noexcept { return m_source; }

uint64_t EdgeError::destination() const noexcept { return m_destination; }

} // namespace

namespace teseo::util {

// globals
stringstream g_exception_stream;
mutex g_exception_mutex;

}
