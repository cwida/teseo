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

#include "teseo/memstore/latch_state.hpp"

#include <sstream>

#include "teseo/memstore/segment.hpp"

using namespace std;

namespace teseo::memstore {

LatchState::LatchState(uint64_t latch) :
        m_xlock ( (latch & Segment::MASK_XLOCK) != 0 ),
        m_writer ( (latch & Segment::MASK_WRITER) != 0 ),
        m_rebalancer ( (latch & Segment::MASK_REBALANCER) != 0 ),
        m_wait ( (latch & Segment::MASK_WAIT) != 0 ),
        m_readers( (latch & Segment::MASK_READERS) >> 48),
        m_version ( latch & Segment::MASK_VERSION ){ }

string LatchState::to_string() const {
    stringstream ss;
    if(m_xlock){ ss << "xlock, "; }
    if(m_writer){ ss << "writer, "; }
    if(m_rebalancer){ ss << "rebalancer, "; }
    if(m_wait){ ss << "wait, "; }
    if(m_readers != 0){ ss << "readers(" << m_readers << "), "; }
    ss << "version: " << m_version;
    return ss.str();
}

ostream& operator<<(ostream& out, const LatchState& ls){
    out << ls.to_string();
    return out;
}

} // namespace
