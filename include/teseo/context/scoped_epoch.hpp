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

#include "thread_context.hpp"

namespace teseo::context {

/**
 * Automatically enter & exit from an epoch in the current thread context
 */
class ScopedEpoch {
    const bool m_active; // do not overwrite an epoch already set

public:
    // set the current epoch
    ScopedEpoch();

    // exit from the acquired epoch
    ~ScopedEpoch();

    // update the current epoch
    void bump();
};


/*****************************************************************************
 *                                                                           *
 *   Implementation details                                                  *
 *                                                                           *
 *****************************************************************************/

inline
ScopedEpoch::ScopedEpoch () : m_active(thread_context()->epoch() == std::numeric_limits<uint64_t>::max()) {
    bump();
}

inline
ScopedEpoch::~ScopedEpoch() {
    if(m_active){ thread_context()->epoch_exit(); }
}

inline
void ScopedEpoch::bump() {
    if(m_active){ thread_context()->epoch_enter(); };
}

} // namespace
