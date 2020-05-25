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

namespace teseo::util {

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
 * Prefetch, fetch a block of memory into the local cache
 */
inline void prefetch(void* pointer){
    __builtin_prefetch(pointer, /* 0 = read only, 1 = read/write */ 0 /*, temporal locality, the default is 3 */);
}

} // namespace
