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

#include "teseo/context/property_snapshot.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

#include "teseo/context/global_context.hpp"
#include "teseo/context/thread_context.hpp"
#include "teseo/gc/garbage_collector.hpp"
#include "teseo/profiler/scoped_timer.hpp"
#include "teseo/transaction/transaction_iterator.hpp"
#include "teseo/transaction/transaction_sequence.hpp"

#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::context {

void gc_list_deleter(void* list){ delete[] reinterpret_cast<PropertySnapshot*>(list); }

PropertySnapshotList::PropertySnapshotList() : m_list(nullptr), m_capacity(MIN_CAPACITY), m_size(0), m_last_txseq(nullptr)  {
    m_list = new PropertySnapshot[m_capacity];
}

PropertySnapshotList::~PropertySnapshotList(){
    m_capacity = m_size = 0;

    // Normally, m_list should be released by the epoch GC.
    // Still, there are two cases where this cannot occur:
    // 1) if the property list belongs to a thread context, then it must have already lost its ownership
    //    when the thread context was unregistered from the global context. Indeed the global context
    //    must have invoked the method #merge and set m_list == nullptr. Therefore delete[] m_list => nop
    // 2) this is the property list of the global context. We're removing the last property in the database
    //    and there are no other thread contexts still alive. This is the case when m_list != nullptr, but
    //    delete[] m_list is still safe
    delete[] m_list; m_list = nullptr;
}

void PropertySnapshotList::resize(uint64_t new_capacity){
    assert(new_capacity >= m_size);

    std::unique_ptr<PropertySnapshot[]> ptr_new_list { new PropertySnapshot[new_capacity] };
    PropertySnapshot* new_list = ptr_new_list.get();
    memcpy(new_list, m_list, m_size * sizeof(PropertySnapshot));
    m_capacity = new_capacity;

    //global_context()->gc()->mark(m_list, list_deleter);
    thread_context()->gc_mark(m_list, gc_list_deleter);
    m_list = ptr_new_list.release();
}

void PropertySnapshotList::insert(const PropertySnapshot& property, const transaction::TransactionSequence* txseq){
    profiler::ScopedTimer profiler { profiler::PROPSNAP_INSERT };
#if defined(PROPERTY_SNAPSHOT_LIST_PROFILER_COUNTERS)
    m_profile_inserted_elements++;
#endif

    m_latch.lock();
    // ensure there is enough space in the array
    if(m_size >= m_capacity) resize(max(MIN_CAPACITY, m_capacity * 2));

    // keep the list sorted
    int64_t i = m_size;
    while(i > 0 && property.m_transaction_id < m_list[i -1].m_transaction_id){
        m_list[i] = m_list[i -1];
        i--;
    }
    m_list[i] = property;

    m_size++;

    // Try to prune the property list?
    prune0(txseq);

    m_latch.unlock();
}

void PropertySnapshotList::prune(const transaction::TransactionSequence* txseq){
    m_latch.lock();
    prune0(txseq);
    m_latch.unlock();
}

void PropertySnapshotList::prune0(const transaction::TransactionSequence* txseq){
    profiler::ScopedTimer profiler { profiler::PROPSNAP_PRUNE };
    if(txseq == nullptr || txseq == m_last_txseq || txseq->size() == 0 || m_size <= 1) return; // we can't prune with less than one element in the list

#if defined(PROPERTY_SNAPSHOT_LIST_PROFILER_COUNTERS)
    m_profile_prune_invocations++;
    uint64_t _size_old = m_size;
#endif

    transaction::TransactionSequenceBackwardsIterator A(txseq);

    // positions in the transaction list
    uint64_t cur = 0, next = 1;
    while(!A.done() && next < m_size){
        if(A.key() < m_list[cur].m_transaction_id){
            A.next();
        }

        // invariant: A.key() >= m_list[cur]
        else if(A.key() >= m_list[next].m_transaction_id){ // merge the items together
            do {
                m_list[cur].m_transaction_id = m_list[next].m_transaction_id;
                m_list[cur].m_property += m_list[next].m_property;
                next++;
            } while(next < m_size && A.key() >= m_list[next].m_transaction_id);

            if(next < m_size){
                cur++;
                m_list[cur] = m_list[next];
                next++;
            }

        } else { // lst[cur] <= A.key() < lst[next]
            cur++;
            if(cur != next) m_list[cur] = m_list[next];
            next++;
        }
    }

    // shift the remaining records backwards
    if((cur +1) != next){
        while(next < m_size){
            cur++;
            m_list[cur] = m_list[next];
            next++;
        }
        m_size = cur +1;
    }

#if defined(PROPERTY_SNAPSHOT_LIST_PROFILER_COUNTERS)
    uint64_t _size_new = m_size;
    m_profile_pruned_elements += (_size_old - _size_new);
#endif

    // resize the underlying array
    uint64_t new_capacity = std::max<uint64_t>(m_size * 2, MIN_CAPACITY);
    if(new_capacity < (m_capacity /2)){ resize(new_capacity); }

    m_last_txseq = txseq;
}


void PropertySnapshotList::prune(uint64_t high_water_mark){
    m_latch.lock();
    prune0(high_water_mark);
    m_latch.unlock();
}

void PropertySnapshotList::prune0(uint64_t high_water_mark){
    if(m_size <= 1){ return; } // we cannot prune this list

    uint64_t pstart = 0;
    uint64_t pend = 1;
    while(pend < m_size && m_list[pend].m_transaction_id <= high_water_mark){
        m_list[pstart].m_transaction_id = m_list[pend].m_transaction_id;
        m_list[pstart].m_property += m_list[pend].m_property;
        pend++;
    }

    // shift the remaining records backwards
    if(pend != 1){
        while(pend < m_size){
            pstart++;
            m_list[pstart] = m_list[pend];
            pend++;
        }
        m_size = pstart +1;
    }

    // resize the underlying array
    uint64_t new_capacity = std::max<uint64_t>(m_size * 2, MIN_CAPACITY);
    if(new_capacity < (m_capacity /2)){ resize(new_capacity); }
}

void PropertySnapshotList::acquire(GlobalContext* gcntxt, PropertySnapshotList& plist2){
    bool latch1_released = false;
    bool latch2_released = false;

    m_latch.lock();
    plist2.m_latch.lock();

    try {
        // Init
        PropertySnapshot* __restrict list1 = m_list;
        uint64_t size1 = m_size;
        PropertySnapshot* __restrict list2 = plist2.m_list;
        uint64_t size2 = plist2.m_size;
        uint64_t new_capacity = std::max(MIN_CAPACITY, size1 + size2);
        std::unique_ptr<PropertySnapshot[]> ptr_new_list { new PropertySnapshot[new_capacity] };
        PropertySnapshot* __restrict new_list = ptr_new_list.get();

        // Merge the two lists
        uint64_t i = 0, j = 0, k = 0;
        while(i < size1 && j < size2){
            if(list1[i].m_transaction_id  < list2[j].m_transaction_id){
                new_list[k] = list1[i];
                i++;
            } else {
                new_list[k] = list2[j];
                j++;
            }
            k++;
        }
        while(i < size1){ new_list[k++] = list1[i++]; }
        while(j < size2){ new_list[k++] = list2[j++]; }

        plist2.m_list = nullptr;
        plist2.m_size = 0;
        plist2.m_capacity = 0;
        plist2.dump_counters();
        plist2.m_latch.unlock(); latch2_released = true;

        gcntxt->gc()->mark(list1, gc_list_deleter);
        gcntxt->gc()->mark(list2, gc_list_deleter);

        m_list = list2 = nullptr;
        m_list = ptr_new_list.release();
        m_size = size1 + size2;
        m_capacity = new_capacity;

        m_latch.unlock(); latch1_released = true;

    } catch (...){
        if(!latch2_released) plist2.m_latch.unlock();
        if(!latch1_released) m_latch.unlock();
        throw;
    }
}

GraphProperty PropertySnapshotList::snapshot(uint64_t transaction_id) const {
    assert(thread_context()->epoch() != std::numeric_limits<uint64_t>::max() && "Must be inside an epoch");

    GraphProperty snapshot;
    bool done = false;
    do {
        try {
            uint64_t version = m_latch.read_version();
            PropertySnapshot* __restrict list = m_list;
            uint64_t size = m_size;
            m_latch.validate_version(version);

            uint64_t k = 0;
            while(k < size && list[k].m_transaction_id <= transaction_id){
                snapshot += list[k].m_property;
                k++;
            }

            m_latch.validate_version(version);

            done = true;
        } catch(Abort){
            snapshot = GraphProperty(); // reset the initial value of the snapshot
            // retry ...
        }
    } while (!done);

    return snapshot;
}

uint64_t PropertySnapshotList::version() const {
    return m_latch.read_version();
}

uint64_t PropertySnapshotList::size() const {
    return m_size;
}

void PropertySnapshotList::dump_counters() const {
#if defined(PROPERTY_SNAPSHOT_LIST_PROFILER_COUNTERS)
    COUT_DEBUG_FORCE("insertions: " << m_profile_inserted_elements << ", pruned invocations: " << m_profile_prune_invocations << ", pruned elements: " << m_profile_pruned_elements);
#endif
}


} // namespace

