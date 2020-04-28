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
     * Acquire a writer lock for the current segment
     */
    void xlock();

    /**
     * Release the writer lock for the current segment
     */
    void xunlock();

    /**
     * Merge the content of `current' into `previous' and return the amount of used space
     */
    uint64_t merge(memstore::Leaf* previous, memstore::Leaf* current, uint64_t cardinality);

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
