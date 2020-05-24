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
#include "teseo/util/numa.hpp"

#include <cerrno>
#include <cstring>
#include <mutex>
#if defined(HAVE_NUMA)
#include <numa.h>
#endif
#include <stdexcept>
#include <unistd.h>

#include "teseo/context/static_configuration.hpp"
#include "teseo/util/error.hpp"

using namespace std;

namespace teseo::util {

void NUMA::check_numa_support(){
    if(context::StaticConfiguration::numa_enabled){
#if defined(HAVE_NUMA)
        // first, check that numa_available() returns a value >= 0
        if(numa_available() < 0){
            RAISE(InternalError, "NUMA support is enabled, but the library libnuma returns " << numa_available() << " when invoking numa_available(). Expected a non negative value");
        }

        // second check, the nodes available must be a contiguous sequence in [0,... N). Arbitrary node IDs
        // and gaps are simply not supported by the current implementation.
        struct bitmask* bitmask = numa_get_mems_allowed();
        int num_nodes = 0;
        for(int i = 0, end = numa_num_possible_nodes(); i < end; i++){
            if(numa_bitmask_isbitset(bitmask, i)){
                if(i != num_nodes){
                    RAISE(InternalError, "NUMA support for arbitrary node IDs is not implemented. Node ID: " << i << ", expected: " << num_nodes);
                }

                num_nodes++;
            }
        }
        numa_free_nodemask(bitmask);

        // third, check that the number of nodes detected at runtime is still the same of those detected at compile time
        if(num_nodes != (int) context::StaticConfiguration::numa_num_nodes){
            RAISE(InternalError, "The number of NUMA nodes detected at runtime is " << num_nodes << ", while the program has been configured at compile time with " << context::StaticConfiguration::numa_num_nodes << " nodes. Reconfigure the program with autoconf and rebuild");
        }

#else
        RAISE(InternalError, "NUMA support is set in include/teseo/context/static_configuration.hpp but the macro HAVE_NUMA is not defined. Reconfigure the program in autoconf setting the option --enable-numa");
#endif
    } else { // NUMA support disabled
        if(context::StaticConfiguration::numa_num_nodes != 1){
            RAISE(InternalError, "The setting `numa_num_nodes' must be set to 1 when the NUMA support is disabled. Current value: " << context::StaticConfiguration::numa_num_nodes);
        }
    }
}

void* NUMA::malloc(uint64_t size){
#if defined(HAVE_NUMA)
    if(context::StaticConfiguration::numa_enabled){
        return malloc(size, 0);
    } else {
#else
    if(true){
#endif
        void* res = ::malloc(size);
        if(res == nullptr) throw std::bad_alloc{};
        return res;
    }
}

void* NUMA::malloc(uint64_t size, int node) {
#if defined(HAVE_NUMA)
    if(!context::StaticConfiguration::numa_enabled || numa_available() < 0) RAISE(InternalError, "NUMA support disabled");
    uint64_t* res = (uint64_t*) numa_alloc_onnode(size + /* header */ 1, node);
    if(res == nullptr) throw std::bad_alloc{};
    res[0] = size;
    return (void*) (res +1);
#else
    RAISE(InternalError, "NUMA support disabled");
#endif
}

void NUMA::free(void* pointer){
    if(pointer == nullptr) return;
#if defined(HAVE_NUMA)
    if(context::StaticConfiguration::numa_enabled){
        uint64_t* start = reinterpret_cast<uint64_t*>(pointer) -1;
        uint64_t size = start[0];
        ::numa_free((void*) start, size + /* header */ 1);
    } else {
        ::free(pointer);
    }
#else
    ::free(pointer);
#endif
}

void* NUMA::copy(void* pointer, int node){
    if(pointer == nullptr) throw std::invalid_argument{"null pointer"};
    uint64_t size = reinterpret_cast<uint64_t*>(pointer)[-1];
    void* copy = NUMA::malloc(size, node);
    memcpy(copy, pointer, size);
    return copy;
}

} // namespace

