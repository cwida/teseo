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

#include "teseo/rebalance/plan.hpp"

#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string>

#include "teseo/context/static_configuration.hpp"
#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/sparse_file.hpp"

//#define DEBUG
#include "teseo/util/debug.hpp"

using namespace std;

namespace teseo::rebalance {

/*****************************************************************************
 *                                                                           *
 *  Initialisation                                                           *
 *                                                                           *
 *****************************************************************************/

Plan::Plan() : m_leaf1(nullptr), m_leaf2(nullptr), m_window_start(0), m_window_end(0), m_num_output_segments(0), m_is_resize(false), m_cardinality(0) {

}

Plan Plan::create_split(uint64_t cardinality, uint64_t used_space, memstore::Leaf* leaf){

    return create_resize(cardinality, used_space, leaf, nullptr);
}

Plan Plan::create_spread(uint64_t cardinality, memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end){
    assert(window_start < window_end);

    Plan plan;
    plan.m_leaf1 = leaf;
    plan.m_window_start = window_start;
    plan.m_window_end = window_end;
    plan.m_num_output_segments = window_end - window_start;
    plan.m_is_resize = false;
    plan.m_cardinality = cardinality;

    COUT_DEBUG("plan: " << plan);
    return plan;
}

Plan Plan::create_merge(uint64_t cardinality, uint64_t used_space, memstore::Leaf* leaf1, memstore::Leaf* leaf2){
    return create_resize(cardinality,used_space, leaf1, leaf2);
}

Plan Plan::create_resize(uint64_t cardinality, uint64_t used_space, memstore::Leaf* leaf1, memstore::Leaf* leaf2) {
    COUT_DEBUG("cardinality: " << cardinality << ", used_space: " << used_space << " qwords" << ", leaves: [" << leaf1 << ", " << leaf2 << "]");

    Plan plan;
    plan.m_leaf1 = leaf1;
    plan.m_leaf2 = leaf2;
    plan.m_window_start = 0;
    plan.m_window_end = leaf1->num_segments();

    plan.m_num_output_segments = max<uint64_t>(
            // round up always for safety reasons
            ceil( static_cast<double>(used_space) / (0.75 * memstore::SparseFile::max_num_qwords()) ),
            context::StaticConfiguration::memstore_max_num_segments_per_leaf/2
    );
    plan.m_is_resize = (static_cast<uint64_t>(plan.m_num_output_segments) != leaf1->num_segments());
    plan.m_cardinality = cardinality;

    COUT_DEBUG("plan: " << plan);
    return plan;
}

/*****************************************************************************
 *                                                                           *
 *  Properties                                                               *
 *                                                                           *
 *****************************************************************************/

bool Plan::is_rebalance() const {
    return !m_is_resize;
}

bool Plan::is_resize() const {
    return m_is_resize;
}

bool Plan::is_merge() const {
    return m_leaf2 != nullptr;
}


uint64_t Plan::window_start() const {
    return m_window_start;
}

uint64_t Plan::window_length() const {
    return window_end() - window_start();
}

uint64_t Plan::window_end() const {
    return m_window_end;
}

uint64_t Plan::num_output_segments() const {
    return m_num_output_segments;
}

void Plan::set_num_output_segments(uint64_t value){
    m_num_output_segments = value;
}

void Plan::set_resize(bool value){
    m_is_resize = value;
}

uint64_t Plan::cardinality() const {
    return m_cardinality;
}

uint64_t Plan::cardinality_ub() const{
    return cardinality() +1;
}

memstore::Leaf* Plan::leaf() const {
    return first_leaf();
}

memstore::Leaf* Plan::first_leaf() const {
    return m_leaf1;
}

memstore::Leaf* Plan::last_leaf() const {
    return m_leaf2 == nullptr ? m_leaf1 : m_leaf2;
}

/*****************************************************************************
 *                                                                           *
 *  Dump                                                                     *
 *                                                                           *
 *****************************************************************************/

string Plan::to_string() const{
    stringstream ss;
    ss << "[RebalancePlan] ";
    if(is_merge()){
        ss << "merge from leaf " << first_leaf() << " up to leaf " << last_leaf();
        if(is_rebalance()){
            ss << " into the first leaf";
        } else {
            ss << " into a new leaf";
        }
        ss << " of " << num_output_segments() << " segments";
    } else {
        if(is_rebalance()){
            ss << "rebalance leaf: " << leaf() << ", segments: [" << window_start() << ", " << window_end() << ")";
        } else if (num_output_segments() <= teseo::context::StaticConfiguration::memstore_max_num_segments_per_leaf) {
            ss << "resize leaf: " << leaf() << " into a new leaf of " << num_output_segments() << " segments";
        } else {
            ss << "split leaf: " << leaf() << " into " <<
                    (uint64_t) ceil( static_cast<double>(num_output_segments()) / teseo::context::StaticConfiguration::memstore_max_num_segments_per_leaf ) << " new leaves";
        }
    }

    ss << ", cardinality: " << cardinality();

    return ss.str();
}

void Plan::dump() const {
    cout << to_string() << endl;
}

ostream& operator<<(ostream& out, const Plan& plan){
    out << plan.to_string();
    return out;
}

} // namespace


