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

#include <condition_variable>
#include <mutex>

namespace teseo::aux {

/**
 * The auxiliary snapshot is lazily built when one of the threads in the
 * transaction requests it. We want to avoid that multiple threads, belonging
 * to the same transaction, do request the creation of the auxiliary snapshot
 * concurrently. This class ensures that only one thread can create the snapshot
 * while all the others wait for the operation to complete.
 */
class CbSerialiseBuild {
    CbSerialiseBuild(const CbSerialiseBuild&) = delete;
    CbSerialiseBuild& operator=(const CbSerialiseBuild&) = delete;

    volatile bool m_done; // check
    std::mutex m_mutex; // thread safety
    std::condition_variable m_condvar;

public:
    // Init the class
    CbSerialiseBuild();

    // Destructor
    ~CbSerialiseBuild();

    // Signal that the build has been completed. Invoked the by thread that performed the build.
    void done();

    // Wait for the auxiliary snapshot to be created. Invoked by all the other threads.
    void wait();
};

} // namespace

