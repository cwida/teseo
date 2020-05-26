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
#include "teseo/aux/cache.hpp"

#include <cassert>
#include <iostream>
#include <sstream>

#include "teseo/aux/static_view.hpp"
#include "teseo/util/latch.hpp"

using namespace std;

namespace teseo::aux {

Cache::Cache(gc::GarbageCollector* garbage_collector) : m_transaction_id(0), m_garbage_collector(garbage_collector){
    for(uint64_t i = 0; i < NUM_NODES; i++){
        m_views[i] = nullptr;
    }
}

Cache::~Cache() {
    unset();
}

bool Cache::get(uint64_t transaction_id, uint64_t highest_txn_rw_id, aux::StaticView** output){
    util::WriteLatch xlock(m_latch);

    if(m_views[0] == nullptr){ // the cache is empty
        return false;
    } else if(highest_txn_rw_id > m_transaction_id){ // the cache became invalid
        unset();
        return false;
    } else if(transaction_id >= m_transaction_id){
        for(uint64_t i = 0; i < NUM_NODES; i++){
            m_views[i]->incr_ref_count();
            output[i] = m_views[i];
        }

        return true;
    } else { // transaction_id < m_transaction_id
        return false;
    }
}

void Cache::set(aux::StaticView** views, uint64_t transaction_id){
    util::WriteLatch xlock(m_latch);

    if(transaction_id > m_transaction_id){
        unset();

        for(uint64_t i = 0; i < NUM_NODES; i++){
            views[i]->incr_ref_count();
            m_views[i] = views[i];
        }

        m_transaction_id = transaction_id;
    }
}

void Cache::unset(){
    if(m_views[0] != nullptr){
        for(uint64_t i = 0; i < NUM_NODES; i++){
            m_views[i]->decr_ref_count(m_garbage_collector);
            m_views[i] = nullptr;
        }
    }
}

string Cache::to_string() const {
    stringstream ss;
    util::WriteLatch xlock(m_latch);

    ss << "transaction_id: " << m_transaction_id << ", views: [";
    for(uint64_t i = 0; i < NUM_NODES; i++){
        if(i > 0) ss << ", ";
        ss << m_views[i];
    }
    ss << "]";


    return ss.str();
}

void Cache::dump() const {
    cout << "[Cache] " << to_string() << endl;
}

ostream& operator<<(ostream& out, const Cache& cache){
    out << cache.to_string();
    return out;
}

} // namespace
