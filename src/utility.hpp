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

#include <cinttypes>
#include <string>

namespace teseo::internal {

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
 * Set the name of the current thread. The given name will appear in the debugger thread list.
 */
void set_thread_name(const std::string& name);



} // namespace
