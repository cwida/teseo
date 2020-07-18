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

#include <atomic>
#include <cinttypes>

#include "teseo/context/global_context.hpp"

#define HAVE_PROFILER_DIRECT_ACCESS

namespace teseo::profiler {

#if defined(HAVE_PROFILER_DIRECT_ACCESS)

class DirectAccessCounters {
    DirectAccessCounters(const DirectAccessCounters&) = delete;
    DirectAccessCounters& operator=(const DirectAccessCounters&) = delete;

public:
    std::atomic<uint64_t> m_memstore_invocations = 0; // total amount of invocations of the method Memstore#scan
    std::atomic<uint64_t> m_memstore_cs_present = 0; // total number of invocations to the method #can having a cursor state
    std::atomic<uint64_t> m_memstore_cs_key_match = 0; // cursor state match (expected key == requested key)
    std::atomic<uint64_t> m_memstore_cs_fkeys_match = 0; // cursor state partial match, same fence key
    std::atomic<uint64_t> m_memstore_cs_dptr_match = 0; // cursor state partial match, direct pointer match
    std::atomic<uint64_t> m_memstore_cs_no_filepos = 0; // cursor state partial match, no filepos available
    std::atomic<uint64_t> m_memstore_cs_no_match = 0; // cursor state present, but it doesn't match the key (release the latch)
    std::atomic<uint64_t> m_memstore_vt_lookups = 0; // total number of look ups to the vertex table, when not using the code path of the cursor state
    std::atomic<uint64_t> m_memstore_vt_fkeys_match = 0; // the fence keys match
    std::atomic<uint64_t> m_memstore_vt_invalid_filepos = 0; // the segment's version does not match
    std::atomic<uint64_t> m_context_invocations = 0; // total amount of calls to the method Context#reader_direct_access
    std::atomic<uint64_t> m_context_dptr_set = 0; // total number of invocations with a direct pointer set
    std::atomic<uint64_t> m_context_invalid_filepos = 0; // either the filepos or the segment ID do not match
    std::atomic<uint64_t> m_context_dptr_success = 0; // # successes with a direct pointer set
    std::atomic<uint64_t> m_context_dptr_failure = 0; // # failures (aborts) with a direct pointer set
    std::atomic<uint64_t> m_context_conventional = 0; // total number of accesses falling back to ART
    std::atomic<uint64_t> m_context_retry = 0; // number of retries when using a conventional access

public:
    // Constructor
    DirectAccessCounters();

    // Destructor
    ~DirectAccessCounters();

    // Reset all counters
    void reset();

    // Dump to stdout the counters
    void dump();
};

#define PROFILE_DIRECT_ACCESS(name) ::teseo::context::global_context()->profiler_direct_access()->m_##name ++

#else

class DirectAccessCounters {
public:
    void reset() { }
    void dump() { }
};
#define PROFILE_DIRECT_ACCESS(name)

#endif

} // namespace
