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

#include "teseo/util/thread.hpp"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <pthread.h>
#include <syscall.h>
#include <unistd.h>

#include "teseo/util/error.hpp"

using namespace std;

namespace teseo::util {

int64_t Thread::get_thread_id(){
    auto tid = (int64_t) syscall(SYS_gettid);
    assert(tid > 0);
    return tid;
}

int64_t Thread::get_process_id(){
    return getpid(); // unistd.h
}

string Thread::get_name(){
    pthread_t thread_id = pthread_self();
    constexpr size_t buffer_sz = 64;
    char buffer[buffer_sz];
    int rc = pthread_getname_np(thread_id, buffer, buffer_sz);
    if(rc != 0){
        RAISE_EXCEPTION(InternalError, "[Thread::get_name] error: " << strerror(errno) << " (" << errno << ")");
    }
    return string(buffer);
}

void Thread::set_name(const string& name){
    pthread_t thread_id = pthread_self();
    string truncated_name = name.substr(0, 15);
    int rc = pthread_setname_np(thread_id, truncated_name.c_str());
    if(rc != 0){
        RAISE_EXCEPTION(InternalError, "[Thread::set_name] error: " << strerror(errno) << " (" << errno << ")");
    }
}


} // namespace
