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

#include <cinttypes>
#include <limits>
#include <ostream>

namespace teseo::memstore {

class Update;

/**
 * A single separator key in the SparseArray consists of an edge, that is a pair <source, destination>
 */
class Key {
    uint64_t m_source; // the source of the edge
    uint64_t m_destination; // the destination of the edge

public:
    // Constructor
    constexpr Key(uint64_t vertex_id) : m_source(vertex_id), m_destination(0) { }
    constexpr Key(uint64_t source, uint64_t destination) : m_source(source), m_destination(destination) { }

    uint64_t source() const;
    uint64_t destination() const;
    void set(uint64_t vertex_id);
    void set(uint64_t source, uint64_t destination);
    bool operator==(const Key& other) const;
    bool operator!=(const Key& other) const;
    bool operator<(const Key& other) const;
    bool operator<=(const Key& other) const;
    bool operator>(const Key& other) const;
    bool operator>=(const Key& other) const;
    constexpr static Key min();
    constexpr static Key max();
};

static_assert(sizeof(Key) == sizeof(uint64_t) * 2, "It should only occupy the pair <source, destination>");

/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/
inline
constexpr Key Key::min(){ return Key(std::numeric_limits< decltype(Key{0}.source()) >::min(), std::numeric_limits< decltype(Key{0}.destination()) >::min()); }
inline
constexpr Key Key::max(){ return Key(std::numeric_limits< decltype(Key{0}.source()) >::max(), std::numeric_limits< decltype(Key{0}.destination()) >::max()); }

// The minimum and maximum key in the universe
constexpr Key KEY_MIN = Key::min();
constexpr Key KEY_MAX = Key::max();

inline uint64_t Key::source() const { return m_source; }
inline uint64_t Key::destination() const { return m_destination; }
inline void Key::set(uint64_t vertex_id) { m_source = vertex_id; m_destination = 0; }
inline void Key::set(uint64_t source, uint64_t destination) { m_source = source; m_destination = destination; }
inline bool Key::operator==(const Key& other) const { return source() == other.source() && destination() == other.destination(); }
inline bool Key::operator!=(const Key& other) const { return !(*this == other); }
inline bool Key::operator<(const Key& other) const { return (source() < other.source()) || (source() == other.source() && destination() < other.destination()); }
inline bool Key::operator<=(const Key& other) const { return (source() < other.source()) || (source() == other.source() && destination() <= other.destination()); }
inline bool Key::operator>(const Key& other) const { return !(*this <= other); }
inline bool Key::operator>=(const Key& other) const { return !(*this < other); }

inline std::ostream& operator<<(std::ostream& out, const Key& key){ out << key.source() << " -> " << key.destination(); return out; }


} // namespace
