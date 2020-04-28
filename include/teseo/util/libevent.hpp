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

#include <vector>

struct event; // libevent forward decl.
struct event_base; // libevent forward decl.

namespace teseo::util {

/**
 * Collection of utility functions to handle and operate on libevent
 */
struct LibEvent {

    /**
     * Initialise the library libevent. If the library has been already initialised, this call is ignored.
     */
    static void init();

    /**
     * Shutdown the library libevent. This should be invoked once for each call to libevent_init().
     */
    static void shutdown();

    /**
     * Collect all events still present in the libevent's queue
     */
    static std::vector<struct event*> get_pending_events(struct event_base* queue);

};


} // namespace
