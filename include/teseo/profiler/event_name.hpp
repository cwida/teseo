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
    TESEO_DEGREE,
    TESEO_INSERT_EDGE,
    TESEO_REMOVE_EDGE,
    TESEO_HAS_EDGE,
    TESEO_GET_WEIGHT,
    TESEO_NUM_VERTICES,
    TESEO_NUM_EDGES,
    /* transactions */
    TXN_ADD_UNDO,
    TXN_COMMIT,
    TXN_COMMIT_CRITICAL_SECTION,
    TXN_COMMIT_UNREGISTER,
    TXN_ROLLBACK,
    /* property snapshots */
    PROPSNAP_INSERT,
    PROPSNAP_PRUNE,
    /* undo */
    UNDO_PRUNE_AT,
    UNDO_PRUNE_HWM,
    /* memstore */
    MEMSTORE_INSERT_VERTEX,
    MEMSTORE_HAS_VERTEX,
    MEMSTORE_REMOVE_VERTEX,
    MEMSTORE_INSERT_EDGE,
    MEMSTORE_WRITE_VERTEX,
    MEMSTORE_WRITE_EDGE,
    MEMSTORE_HAS_EDGE,
    MEMSTORE_GET_WEIGHT,
    MEMSTORE_GET_DEGREE,
    MEMSTORE_GET_DEGREE_OPTIMISTIC,
    MEMSTORE_REMOVE_EDGE,
    MEMSTORE_ROLLBACK,
    /* context */
    CONTEXT_WRITER_ENTER,
    CONTEXT_WRITER_ENTER_INDEX,
    CONTEXT_WRITER_ENTER_BROWSE,
    CONTEXT_WRITER_EXIT,
    CONTEXT_READER_ENTER,
    CONTEXT_READER_ENTER_INDEX,
    CONTEXT_READER_ENTER_BROWSE,
    CONTEXT_READER_NEXT,
    CONTEXT_READER_EXIT,
    CONTEXT_OPTIMISTIC_ENTER,
    CONTEXT_OPTIMISTIC_NEXT,
    /* leaf */
    LEAF_CREATE,
    /* segment */
    SEGMENT_UPDATE,
    SEGMENT_TO_DENSE,
    SEGMENT_TO_SPARSE,
    /* sparse file */
    SF_UPDATE_VERTEX,
    SF_UPDATE_EDGE,
    SF_IS_SOURCE_VISIBLE,
    SF_HAS_ITEM_OPTIMISTIC,
    SF_GET_WEIGHT_OPTIMISTIC,
    SF_GET_DEGREE,
    SF_GET_DEGREE_OPTIMISTIC,
    SF_ROLLBACK,
    SF_LOAD,
    SF_SAVE,
    SF_SAVE_FILL,
    SF_PRUNE,
    SF_PRUNE_VERSIONS,
    SF_PRUNE_ELEMENTS,
    /* dense file */
    DF_UPDATE_VERTEX,
    DF_UPDATE_EDGE,
    DF_IS_SOURCE_VISIBLE,
    DF_HAS_ITEM_OPTIMISTIC,
    DF_GET_WEIGHT_OPTIMISTIC,
    DF_ROLLBACK,
    DF_LOAD,
    DF_SORT_IN_PLACE,
    /* asynchronous rebalancer */
    ARS_HANDLE_REQUEST,
    /* crawler */
    CRAWLER_MAKE_PLAN,
    CRAWLER_LOCK2MERGE,
    /* merger */
    MERGER_EXECUTE,
    MERGER_VISIT_AND_PRUNE,
    MERGER_MERGE,
    /* garbage collector */
    GC_EXECUTE,
    GC_PERFORM_GC_PASS,
    GC_QUEUE_LOCAL,
    GC_QUEUE_SHARED,
    GC_QUEUE_PRIVATE,
    /* transaction memory pool */
    MEMPOOL_EXCHANGE,
    MEMPOOL_CLEANUP,
};

} // namespace
