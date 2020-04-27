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
#include <iostream>
#include <sstream>
#include <string>

#include "teseo/context/static_configuration.hpp"

using namespace std;

namespace teseo::rebalance {

/*****************************************************************************
 *                                                                           *
 *  Initialisation                                                           *
 *                                                                           *
 *****************************************************************************/

Plan::Plan() : m_leaf1(nullptr), m_leaf2(nullptr), m_window_start(0), m_window_end(0), m_num_output_segments(0), m_cardinality(0) {

}

Plan Plan::create_split(uint64_t cardinality, memstore::Leaf* leaf, uint64_t num_out_segments){
    assert(num_out_segments > context::StaticConfiguration::memstore_num_segments_per_leaf);

    Plan plan;
    plan.m_leaf1 = leaf;
    plan.m_window_start = 0;
    plan.m_window_end = context::StaticConfiguration::memstore_num_segments_per_leaf;
    plan.m_num_output_segments = num_out_segments;
    plan.m_cardinality = cardinality;
    return plan;
}

Plan Plan::create_spread(uint64_t cardinality, memstore::Leaf* leaf, uint64_t window_start, uint64_t window_end){
    assert(window_start < window_end);

    Plan plan;
    plan.m_leaf1 = leaf;
    plan.m_window_start = window_start;
    plan.m_window_end = window_end;
    plan.m_num_output_segments = window_end - window_start;
    plan.m_cardinality = cardinality;
    return plan;
}

Plan Plan::create_merge(uint64_t cardinality, memstore::Leaf* leaf1, memstore::Leaf* leaf2){

    Plan plan;
    plan.m_leaf1 = leaf1;
    plan.m_leaf2 = leaf2;
    plan.m_window_start = 0;
    // we actually don't know the number of segmnet in input
    plan.m_window_end = context::StaticConfiguration::memstore_num_segments_per_leaf * 2;
    plan.m_num_output_segments = context::StaticConfiguration::memstore_num_segments_per_leaf;
    plan.m_cardinality = cardinality;
    return plan;
}

/*****************************************************************************
 *                                                                           *
 *  Properties                                                               *
 *                                                                           *
 *****************************************************************************/

bool Plan::is_spread() const {
    return window_length() == num_output_segments();
}

bool Plan::is_merge() const {
    // also m_leaf2 != nullptr
    return window_length() > num_output_segments();
}

bool Plan::is_split() const {
    return window_length() < num_output_segments();
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
    return m_leaf2;
}

/*****************************************************************************
 *                                                                           *
 *  Dump                                                                     *
 *                                                                           *
 *****************************************************************************/

string Plan::to_string() const{
    stringstream ss;
    ss << "[RebalancePlan] ";
    if(is_spread()){
        ss << "spread leaf: " << leaf() << ", segments: [" << window_start() << ", " << window_end() << ")";
    } else if(is_merge()){
        ss << "merge from leaf " << first_leaf() << " up to leaf " << last_leaf();
    } else if(is_split()){
        ss << "split leaf: " << leaf() << " into " << num_output_segments();
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


