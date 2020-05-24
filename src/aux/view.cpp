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

#include "teseo/aux/view.hpp"

#include "teseo/aux/static_view.hpp"
#include "teseo/util/error.hpp"
#include "teseo/util/numa.hpp"

namespace teseo::aux {

View::View(bool is_static) : m_is_static(is_static) {

}

View::~View(){

}

uint64_t View::vertex_id(uint64_t logical_id) const noexcept {
    if(m_is_static){
        return reinterpret_cast<const StaticView*>(this)->vertex_id(logical_id);
    } else {
        // not implemented yet
        return 0;
    }
}

uint64_t View::logical_id(uint64_t vertex_id) const noexcept {
    if(m_is_static){
        return reinterpret_cast<const StaticView*>(this)->logical_id(vertex_id);
    } else {
        // not implemented yet
        return 0;
    }
}

uint64_t View::degree(uint64_t id, bool is_logical) const noexcept {
    if(m_is_static){
        return reinterpret_cast<const StaticView*>(this)->degree(id, is_logical);
    } else {
        // not implemented yet
        return 0;
    }
}

uint64_t View::num_vertices() const noexcept {
    if(m_is_static){
        return reinterpret_cast<const StaticView*>(this)->num_vertices();
    } else {
        // not implemented yet
        return 0;
    }
}

void View::incr_ref_count() noexcept {
    m_ref_count++;
}

void View::decr_ref_count() noexcept {
    if(--m_ref_count == 0){
        this->~View();
        util::NUMA::free(this);
    }
}

} // namespace
