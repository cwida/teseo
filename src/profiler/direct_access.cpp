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

#include "teseo/profiler/direct_access.hpp"

#include <iostream>

using namespace std;

namespace teseo::profiler {

#if defined(HAVE_PROFILER_DIRECT_ACCESS)

DirectAccessCounters::DirectAccessCounters() {

}

DirectAccessCounters::~DirectAccessCounters() {

}

void DirectAccessCounters::reset(){
    m_memstore_invocations = 0;
    m_memstore_cs_present = 0;
    m_memstore_cs_key_match = 0;
    m_memstore_cs_fkeys_match = 0;
    m_memstore_cs_dptr_match = 0;
    m_memstore_cs_no_filepos = 0;
    m_memstore_cs_no_match = 0;
    m_memstore_vt_lookups = 0;
    m_memstore_vt_fkeys_match = 0;
    m_memstore_vt_invalid_filepos = 0;
    m_context_invocations = 0;
    m_context_dptr_set = 0;
    m_context_invalid_filepos = 0;
    m_context_dptr_success = 0;
    m_context_dptr_failure = 0;
    m_context_conventional = 0;
    m_context_retry = 0;
}

void DirectAccessCounters::dump(){
    cout << "[Direct Access Profiler]" << endl;
    cout << "Invocations to Memstore::scan: " << m_memstore_invocations << "\n";

    { // Cursor state
        double perc_cs_present = static_cast<double>(m_memstore_cs_present) / m_memstore_invocations * 100.0;
        cout << "  Invocations with a cursor state: " << m_memstore_cs_present << " (" << perc_cs_present << " %)\n";

        double perc_cs_match = static_cast<double>(m_memstore_cs_key_match) / m_memstore_cs_present * 100.0;
        cout << "    Cursor state match (expected key == requested key): " << m_memstore_cs_key_match << " (" << perc_cs_match << " %)\n";

        double perc_fkeys_match = static_cast<double>(m_memstore_cs_fkeys_match) / m_memstore_cs_present * 100.0;
        cout << "    Cursor state partial match (fence keys only): " << m_memstore_cs_fkeys_match << " (" << perc_fkeys_match << " %)\n";

        double perc_dptr_match = static_cast<double>(m_memstore_cs_dptr_match) / m_memstore_cs_fkeys_match * 100;
        cout << "      Filepos recovered from the vertex table: " << m_memstore_cs_dptr_match << " (" << perc_dptr_match << " %)\n";

        double perc_fkeys_no_filepos = static_cast<double>(m_memstore_cs_no_filepos) / m_memstore_cs_fkeys_match * 100;
        cout << "      Without filepos: " << m_memstore_cs_no_filepos << " (" << perc_fkeys_no_filepos << " %)\n";

        double perc_invalid_cs = static_cast<double>(m_memstore_cs_no_match) / m_memstore_cs_present * 100.0;
        cout << "    Invalid cursor state: " << m_memstore_cs_no_match << " (" << perc_invalid_cs << " %)\n";
    }

    { // Vertex table
        double perc_vt_lookups = static_cast<double>(m_memstore_vt_lookups) / m_memstore_invocations * 100.0;
        cout << "  Vertex table lookups: " << m_memstore_vt_lookups << " (" << perc_vt_lookups << " %)\n";

        double perc_vt_fkeys_match = static_cast<double>(m_memstore_vt_fkeys_match) / m_memstore_vt_lookups * 100.0;
        cout << "    Fence keys match: " << m_memstore_vt_fkeys_match << " (" << perc_vt_fkeys_match << " %)\n";

        uint64_t num_version_matches = m_memstore_vt_fkeys_match - m_memstore_vt_invalid_filepos;
        double perc_vt_version_match = static_cast<double>(num_version_matches) / m_memstore_vt_lookups * 100.0;
        cout << "    Version match: " << num_version_matches << " (" << perc_vt_version_match << " %)\n";
    }

    cout << "Invocations to Context::reader_direct_access: " << m_context_invocations << "\n";

    { // Direct pointer set
        double perc_dptr_set = static_cast<double>(m_context_dptr_set) / m_context_invocations * 100.0;
        cout << "  Direct pointer set: " << m_context_dptr_set << " (" << perc_dptr_set << " %)\n";

        double perc_dptr_success = static_cast<double>(m_context_dptr_success) / m_context_dptr_set * 100.0;
        cout << "    Segment found: " << m_context_dptr_success << " (" << perc_dptr_success << " %)\n";

        double perc_dptr_invalid_filepos = static_cast<double>(m_context_invalid_filepos) / m_context_dptr_set * 100.0;
        cout << "    Invalid filepos: " << m_context_invalid_filepos << " (" << perc_dptr_invalid_filepos << " %)\n";

        double perc_dptr_aborts = static_cast<double>(m_context_dptr_failure) / m_context_dptr_set * 100.0;
        cout << "    Aborts: " << m_context_dptr_failure << " (" << perc_dptr_aborts << " %)\n";
    }

    { // Convential access (through the trie)
        double perc_conventional_access = static_cast<double>(m_context_conventional) / m_context_invocations * 100.0;
        cout << "  Conventional accesses: " << m_context_conventional << " (" << perc_conventional_access << " %)\n";

        double perc_retries = static_cast<double>(m_context_retry) / m_context_conventional * 100.0;
        cout << "    Aborts: " <<  m_context_retry << " (" << perc_retries << " %)\n";
    }

    cout << endl;
}


#endif

} // namepsace


