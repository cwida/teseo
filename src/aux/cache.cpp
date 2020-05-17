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

#include "teseo/aux/auxiliary_view.hpp"
#include "teseo/util/latch.hpp"

using namespace std;

namespace teseo::aux {

Cache::Cache() : m_transaction_id(0), m_view(nullptr){

}

Cache::~Cache() {
    if(m_view != nullptr){
        m_view->decr_ref_count();
        m_view = nullptr;
    }
}

AuxiliaryView* Cache::get(uint64_t transaction_id, uint64_t highest_txn_rw_id){
    util::WriteLatch xlock(m_latch);

    if(m_view == nullptr){
        return nullptr;
    } else if(highest_txn_rw_id > m_transaction_id){
        m_view->decr_ref_count();
        m_view = nullptr;
        return nullptr;
    } else if(transaction_id >= m_transaction_id){
        m_view->incr_ref_count();
        return m_view;
    } else { // transaction_id < m_transaction_id
        return nullptr;
    }
}

void Cache::set(aux::AuxiliaryView* view, uint64_t transaction_id){
    util::WriteLatch xlock(m_latch);

    if(transaction_id > m_transaction_id){
        if(m_view != nullptr){
            m_view->decr_ref_count();
        }
        view->incr_ref_count();
        m_view = view;
        m_transaction_id = transaction_id;
    }
}

string Cache::to_string() const {
    stringstream ss;
    util::WriteLatch xlock(m_latch);

    ss << "transaction_id: " << m_transaction_id << ", view: " << m_view;
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
