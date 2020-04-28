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
#include "teseo/util/system.hpp"

#include <cerrno>
#include <cstring>
#include <unistd.h>

#include "teseo/util/error.hpp"

using namespace std;

namespace teseo::util {

string System::hostname(){
    constexpr int len = 512;
    char buffer[len];
    auto rc = gethostname(buffer, len);
    if(rc != 0){
        RAISE(InternalError, "[hostname] Cannot retrieve the hostname: " << strerror(errno)  << " (" << errno << ")");
    }
    string hostname{buffer};

    // Remove the suffix `.scilens.private' from the machines in the Scilens cluster
    const string scilens_suffix = ".scilens.private";
    if(hostname.length() >= scilens_suffix.length()){
        auto scilens_match = hostname.rfind(scilens_suffix);
        if(scilens_match != string::npos && scilens_match == hostname.length() - scilens_suffix.length()){
            hostname.replace(scilens_match, scilens_suffix.length(), nullptr, 0);
        }
    }

    return hostname;
}


} // namespace
