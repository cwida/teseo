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

#include <cassert>
#include <cinttypes>
#include <emmintrin.h>

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

/**
 * Emit the instruction PAUSE, a hint for the processor that it is performing a spin loop
 */
inline void pause(){
    _mm_pause();
}

/**
 * Atomically load the given 128-bits variable. This is a wrapper to the instruction cmpxchg16b w/ lock.
 */
inline unsigned __int128 atomic_load_16(unsigned __int128& variable){
    // The Intel Software Developer Manual demands that `the destination (memory) operand be 16-byte aligned.'
    assert(reinterpret_cast<uint64_t>(&variable) % 16 == 0 && "The given variable is not aligned to 16 bytes");

    uint64_t output_high {0}, output_low {0};
    asm volatile(
        "lock cmpxchg16b %0 ;"
        /* output */ : "+m" (variable), /* rdx */ "+d" (output_high), /* rax */ "+a" (output_low) // + = read/write, = write only
        /* input */  : /* rcx */ "c" (0), /* rbx */ "b" (0)
        /* clobber */: /* flags */ "cc"
    );
    return (static_cast<unsigned __int128>(output_high) << 64) | output_low;
}

/**
 * Atomically store the given value into the 128-bits variable. This is a wrapper to the instruction cmpxchg16b w/ lock.
 */
inline void atomic_store_16(unsigned __int128& variable, unsigned __int128 value){
    // The Intel Software Developer Manual demands that `the destination (memory) operand be 16-byte aligned.'
    assert(reinterpret_cast<uint64_t>(&variable) % 16 == 0 && "The destination variable is not aligned to 16 bytes");

    // Extract the 64 MSB and 64 LSB of the operands
    uint64_t input_high = static_cast<uint64_t>(value >> 64);
    uint64_t input_low = static_cast<uint64_t>(value);
    uint64_t output_high = static_cast<uint64_t>(variable >> 64);
    uint64_t output_low = static_cast<uint64_t>(variable);
    bool done = false;

    // This is a CAS instruction. The idea is that we keep testing & reloading the value of variable until we succeed.
    do { // cmpxchg16b: compare & exchange
        asm volatile(
           "lock cmpxchg16b %0 ;"
           /* output */ : "+m" (variable), "=@ccz" (done), /* rdx */ "+d" (output_high), /* rax */ "+a" (output_low) // + = read/write, = write only
           /* input */  : /* rcx */ "c" (input_high), /* rbx */ "b" (input_low)
           /* clobber */: /* flags */ "cc"
        );
    } while(!done);
}

} // namespace
