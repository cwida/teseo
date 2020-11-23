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

#include "teseo/memstore/context.hpp"

namespace teseo::rebalance {

class ScratchPad; // forward decl.

/**
 * This class traverses the leaves in a sparse array, pruning obsolete records
 * and merging together consecutive leaves, where possible.
 */
class MergeOperator {
    memstore::Context m_context; // ptr to the path memstore -> leaf -> segment
    ScratchPad* m_scratchpad; // working area, used to merge leaves together

    /**
     * Visit the current leaf, prune all the old records and return and estimate of the
     * number of slots in use
     */
    uint64_t visit_and_prune();

    /**
     * The output of the
     */
    struct MergeOutput {
        bool m_invalidate_previous; // whether to invalidate the first leaf
        bool m_invalidate_current; // whether to invalidate the last leaf
        memstore::Leaf* m_leaf; // pointer to the last leaf of the merge
        uint64_t m_filled_space; // amount of space filled in the merged leaf
    };

    /**
     * Merge the content of the leaves `previous' and `current'
     */
    MergeOutput merge(memstore::Leaf* previous, memstore::Leaf* current, uint64_t cardinality, uint64_t used_space);

public:
    /**
     * Create a new instance of the operator
     */
    MergeOperator(const memstore::Context& context);

    /**
     * Destructor
     */
    ~MergeOperator();

    /**
     * Execute the operator!
     */
    void execute();
};

}
