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


#include "key.hpp"

#include <limits>

using namespace std;

namespace teseo::internal::memstore {

Key::Key() : Key(numeric_limits<uint64_t>::max(), numeric_limits<uint64_t>::max()) { }
Key::Key(uint64_t vertex_id) : Key(vertex_id, 0) { }
Key::Key(uint64_t src, uint64_t dst) : m_source(src), m_destination(dst) { }
uint64_t Key::get_source() const { return m_source; }
uint64_t Key::get_destination() const { return m_destination; }
void Key::set(uint64_t vertex_id) { m_source = vertex_id; m_destination = 0; }
void Key::set(uint64_t source, uint64_t destination) { m_source = source; m_destination = destination; }
bool Key::operator==(const Key& other) const { return get_source() == other.get_source() && get_destination() == other.get_destination(); }
bool Key::operator!=(const Key& other) const { return !(*this == other); }
bool Key::operator<(const Key& other) const { return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() < other.get_destination()); }
bool Key::operator<=(const Key& other) const { return (get_source() < other.get_source()) || (get_source() == other.get_source() && get_destination() <= other.get_destination()); }
bool Key::operator>(const Key& other) const { return !(*this <= other); }
bool Key::operator>=(const Key& other) const { return !(*this < other); }
Key Key::min(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::min(), numeric_limits< decltype(Key{}.get_destination()) >::min()); }
Key Key::max(){ return Key(numeric_limits< decltype(Key{}.get_source()) >::max(), numeric_limits< decltype(Key{}.get_destination()) >::max()); }
std::ostream& operator<<(std::ostream& out, const Key& key){
    out << key.get_source() << " -> " << key.get_destination();
    return out;
}

} // namespace
