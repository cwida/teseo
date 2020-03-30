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

#include "latch.hpp"

namespace teseo::internal::memstore {

/**
 * A scoped lock, to acquire & release an optimistic latch in `phantom mode'. Phantom mode
 * implies that the version of the latch is not altered, and therefore optimistic readers
 * can still proceed.
 */
class ScopedPhantomLock {
    OptimisticLatch<0>& m_latch; // underlying latch
    bool m_is_released = false; // whether the latch has already been released
public:

    // Acquire the optimistic latch in t-mode
    ScopedPhantomLock(OptimisticLatch<0>& latch) : m_latch(latch) {
        m_latch.phantom_lock();
    };

    // Destructor
    ~ScopedPhantomLock(){
        unlock();
    }

    // Release the optimistic latch in t-mode
    // @return the version associated to the latch
    uint64_t unlock(){
        if(m_is_released){ return 0; }
        uint64_t version = m_latch.phantom_unlock();
        m_is_released = true;
        return version;
    }
};

} // namespace
