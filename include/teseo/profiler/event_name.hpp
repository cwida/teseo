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
    MEMSTORE_INSERT_VERTEX,
    MEMSTORE_HAS_VERTEX,
    MEMSTORE_REMOVE_VERTEX,
    MEMSTORE_INSERT_EDGE,
    MEMSTORE_HAS_EDGE,
    MEMSTORE_GET_WEIGHT,
    MEMSTORE_REMOVE_EDGE,
    MEMSTORE_ROLLBACK,
    SF_UPDATE_VERTEX,
    SF_UPDATE_EDGE,
    SF_IS_SOURCE_VISIBLE,
    SF_HAS_ITEM_OPTIMISTIC,
    SF_GET_WEIGHT_OPTIMISTIC,
    SF_ROLLBACK,
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
    CONTEXT_WRITER_ENTER,
    CONTEXT_WRITER_ENTER_INDEX,
    CONTEXT_WRITER_ENTER_BROWSE,
    CONTEXT_WRITER_EXIT,
    CONTEXT_READER_ENTER,
    CONTEXT_READER_ENTER_INDEX,
    CONTEXT_READER_ENTER_BROWSE,
    CONTEXT_READER_EXIT,
    CONTEXT_OPTIMISTIC_ENTER,
    LEAF_CREATE,
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
