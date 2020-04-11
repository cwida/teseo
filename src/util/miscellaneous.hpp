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

#include <chrono>
#include <cinttypes>
#include <string>
#include <sys/time.h>
#include <vector>

struct event; // libevent forwar decl.
struct event_base; // libevent forward decl.

namespace teseo::internal::util {

/**
 * Compiler barrier
 */
inline void barrier(){
    __asm__ __volatile__("": : :"memory");
};

/**
 * Read the cpu timestamp counter
 */
inline uint64_t rdtscp(){
    uint64_t rax;
    asm volatile (
        "rdtscp ; shl $32, %%rdx; or %%rdx, %%rax; "
         : "=a" (rax)
         : /* no inputs */
         : "rcx", "rdx"
    );
    return rax;
}

/**
 * Get the Linux thread ID, that is the identifier shown by the debugger
 */
int64_t get_thread_id();

/**
 * Set the name of the current thread. The given name will appear in the debugger thread list.
 */
void set_thread_name(const std::string& name);

/**
 * Cast a chrono::duration to a timeval
 */
template<typename Duration>
struct timeval duration2timeval(Duration duration){
    uint64_t microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    struct timeval result;
    result.tv_sec = microseconds / (/* milli */ 1000 * /* micro */ 1000);
    result.tv_usec = microseconds % (/* milli */ 1000 * /* micro */ 1000);
    return result;
}

/**
 * Initialise the library libevent. If the library has been already initialised, this call is ignored.
 */
void libevent_init();

/**
 * Shutdown the library libevent. This should be invoked once for each call to libevent_init().
 */
void libevent_shutdown();

/**
 * Collect all events still present in the libevent's queue
 */
std::vector<struct event*> libevent_pending_events(struct event_base* queue);
}
