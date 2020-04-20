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

#include "teseo/util/chrono.hpp"

#include <ctime>

using namespace std;
using namespace std::chrono;

namespace teseo::util {

string to_string(const time_point<system_clock>& tp){
    auto secs = duration_cast<seconds>(tp.time_since_epoch()).count();
    auto str = string( ctime(&secs) );
    return str.substr(0, str.size() -1); // remove the trailing \n
}

} // namespace
