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

#include "teseo/gc/item.hpp"

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>

#include "teseo/util/assembly.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::gc {


Item::Item() : m_timestamp(0), m_pointer(nullptr), m_deleter(free){

}


Item::Item(void* pointer, void (*deleter)(void*)) : m_timestamp(util::rdtscp()), m_pointer(pointer), m_deleter(deleter) {
    assert(deleter != nullptr);
}

void* Item::pointer() const {
    return m_pointer;
}

void Item::process(){
    COUT_DEBUG("item: " << *this);
    m_deleter(m_pointer);
    m_pointer = nullptr;
}

bool Item::process_if(uint64_t epoch){
    if(m_timestamp < epoch){
        process();
        return true;
    } else {
        return false;
    }
}

string Item::to_string() const {
    stringstream ss;
    ss << "timestamp: " << m_timestamp << ", pointer: " << m_pointer << ", deleter: " << m_deleter;
    return ss.str();
}

ostream& operator<<(ostream& out, const Item& item){
    out << item.to_string();
    return out;
}

} // namespace



