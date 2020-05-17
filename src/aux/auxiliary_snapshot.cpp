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

#include "teseo/aux/auxiliary_snapshot.hpp"

using namespace std;

namespace teseo::aux {

AuxiliarySnapshot::AuxiliarySnapshot(){

}

AuxiliarySnapshot::~AuxiliarySnapshot(){

}

void AuxiliarySnapshot::incr_ref_count() noexcept {
    m_ref_count++;
}

void AuxiliarySnapshot::decr_ref_count() noexcept {
    if(--m_ref_count == 0){
        delete this;
    }
}

} // namespace


