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
#include "teseo/aux/cb_serialise_build.hpp"

using namespace std;

namespace teseo::aux {

CbSerialiseBuild::CbSerialiseBuild() : m_done(false) { }

CbSerialiseBuild::~CbSerialiseBuild(){ }

void CbSerialiseBuild::done(){
    m_mutex.lock();
    m_done = true;
    m_mutex.unlock();
    m_condvar.notify_all();
}

void CbSerialiseBuild::wait(){
    unique_lock<mutex> xlock(m_mutex);
    m_condvar.wait(xlock, [this]{ return m_done == true; });
}

} // namespace
