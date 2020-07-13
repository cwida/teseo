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

#include "teseo/memstore/key.hpp"
#include "teseo/memstore/segment.hpp"

using namespace std;

namespace teseo::memstore {

CursorState::CursorState() : m_key(KEY_MIN) {

}

CursorState::~CursorState(){
    close();
}

void CursorState::invalidate() noexcept {
    m_key = KEY_MIN;
}

void CursorState::close() noexcept {
    if(is_valid()){
        m_position.segment()->reader_exit();
        invalidate();
    }
}

string CursorState::to_string() const {
    stringstream ss;
    ss << "cursor";
    if(!is_valid()){
        ss << ", closed";
    } else {
        ss << ", open, key: " << m_key << ", position: " << m_position;

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
