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

#include "scoped_epoch.hpp"

#include "thread_context.hpp"

namespace teseo::internal::context {

ScopedEpoch::ScopedEpoch () {
    bump();
}

ScopedEpoch::~ScopedEpoch() {
    thread_context()->epoch_exit();
}

void ScopedEpoch::bump() {
    thread_context()->epoch_enter();
}

} // namespace
