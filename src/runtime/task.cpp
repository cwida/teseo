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

#include "teseo/runtime/task.hpp"

#include <sstream>

#include "teseo/third-party/magic_enum.hpp"

using namespace std;

namespace teseo::runtime {

string Task::to_string() const {
    stringstream ss;
    ss << "Task: " << magic_enum::enum_name(type()) << ", payload: " << payload() << "";
    return ss.str();
}

ostream& operator<<(ostream& out, const Task& task){
    out << task.to_string();;
    return out;
}

} // namespace


