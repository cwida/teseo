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

namespace teseo::profiler {

/**
 * List of events recorded by the internal profiler
 */
enum EventName {
    /* top level / external methods */
    TESEO_START_TRANSACTION,
    TESEO_INSERT_VERTEX,
    TESEO_REMOVE_VERTEX,
    TESEO_HAS_VERTEX,
    TESEO_INSERT_EDGE,
    TESEO_REMOVE_EDGE,
    TESEO_HAS_EDGE,
    TESEO_GET_WEIGHT,
    TESEO_NUM_VERTICES,
    TESEO_NUM_EDGES,
    /* transactions */
    TXN_ADD_UNDO,
    TXN_COMMIT,
    TXN_ROLLBACK,
    /* property snapshots */
    PROPSNAP_INSERT,
    PROPSNAP_PRUNE,
    /* undo */
    UNDO_PRUNE_AT,
    UNDO_PRUNE_HWM,
    /* sparse array */
    SA_INSERT_VERTEX,
    SA_HAS_VERTEX,
    SA_HAS_VERTEX_UNLOCKED,
    SA_REMOVE_VERTEX,
    SA_INSERT_EDGE,
    SA_HAS_EDGE,
    SA_GET_WEIGHT,
    SA_REMOVE_EDGE,
    SF_UPDATE_VERTEX,
    SF_UPDATE_EDGE,
    SF_IS_SOURCE_VISIBLE,
    SA_REBALANCE_GATE,
    SA_REBALANCE_GATE_FIND_WINDOW,
    SA_REBALANCE_CHUNK,
    SA_REBALANCE_CHUNK_FIND_WINDOW,
    SA_REBALANCE_RECOMPUTE_USED_SPACE,
    SA_UPDATE_FENCE_KEYS,
    SA_CHECK_FENCE_KEYS,
    SA_UPDATE_SEPARATOR_KEYS,
    SA_INDEX_INSERT,
    SA_INDEX_REMOVE,
    SA_WRITER_ON_ENTRY,
    SA_WRITER_ON_ENTRY_INDEX_FIND,
    SA_WRITER_ON_ENTRY_GET_GATE,
    SA_WRITER_ON_EXIT,
    SA_ROLLBACK,
    SA_ROLLBACK_SEGMENT,
    SA_ALLOCATE_CHUNK,
    /* gate */
    GATE_FIND,
    /* asynchronous rebalancer */
    ARS_HANDLE_REQUEST,
    ARS_REBALANCE_GATE,
    ARS_REBALANCE_CHUNK,
    /* merger */
    MERGER_EXECUTE,
    MERGER_VISIT_AND_PRUNE,
    MERGER_MERGE,
};

} // namespace
