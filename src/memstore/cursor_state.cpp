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

#include "teseo/memstore/cursor_state.hpp"

#include <cassert>
#include <sstream>

using namespace std;

namespace teseo::memstore {

CursorState::CursorState(const Context& context) : m_context(context), m_key(KEY_MIN), m_pos_vertex(0), m_pos_edge(0), m_pos_backptr(0){

}

CursorState::~CursorState(){
    close();
}

void CursorState::invalidate() noexcept {
    m_key = KEY_MIN;
}

void CursorState::close() noexcept {
    if(is_valid()){
        m_context.reader_exit();
        invalidate();
    }
}

void CursorState::save(const Context& context, Key key, uint64_t pos_vertex, uint64_t pos_edge, uint64_t pos_backptr) noexcept {
    assert(!is_valid() && "There are still open latches around...");

    m_context = context;
    m_key = key;
    m_pos_vertex = pos_vertex;
    m_pos_edge = pos_edge;
    m_pos_backptr = pos_backptr;
}

string CursorState::to_string() const {
    stringstream ss;
    ss << "context: " << m_context;
    if(!is_valid()){
        ss << ", closed";
    } else {
        ss << ", open, key: " << m_key;
        ss << ", pos_vertex: " << m_pos_vertex;
        ss << ", pos_edge: " << m_pos_edge;
        ss << ", pos_backptr: " << m_pos_backptr;
    }
    return ss.str();
}

void CursorState::dump() const {
    cout << "[CursorState] " << to_string() << "\n";
}

ostream& operator<<(ostream& out, const CursorState& bookmark) {
    out << bookmark.to_string();
    return out;
}

} // namespace
