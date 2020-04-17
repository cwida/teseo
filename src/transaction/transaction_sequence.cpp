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

#include "teseo/transaction/transaction_sequence.hpp"

#include <cassert>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;

namespace teseo::transaction {

TransactionSequence::TransactionSequence(): m_num_transactions(0) { }

TransactionSequence::TransactionSequence(uint64_t num_transactions) : m_num_transactions(num_transactions){
    m_transaction_ids = new uint64_t[m_num_transactions]();
}

TransactionSequence::TransactionSequence(TransactionSequence&& move) :
        m_transaction_ids(move.m_transaction_ids), m_num_transactions(move.m_num_transactions){
    move.m_num_transactions = 0;
    move.m_transaction_ids = nullptr;
}

TransactionSequence& TransactionSequence::operator=(TransactionSequence&& move) {
    if(this != &move){
        delete[] m_transaction_ids;
        m_transaction_ids = move.m_transaction_ids;
        m_num_transactions = move.m_num_transactions;

        move.m_num_transactions = 0;
        move.m_transaction_ids = nullptr;
    }
    return *this;
}

TransactionSequence::~TransactionSequence() {
    delete[] m_transaction_ids; m_transaction_ids = nullptr;
}

uint64_t TransactionSequence::size() const {
    return m_num_transactions;
}

uint64_t TransactionSequence::operator[](uint64_t index) const {
    assert(index < size());
    return m_transaction_ids[index];
}

string TransactionSequence::to_string() const  {
    stringstream ss;
    if(size() == 0){
        ss << "empty";
    } else {
        ss << "[";
        for(uint64_t i = 0, sz = size(); i < sz; i++){
            if(i > 0) ss << ", ";
            ss << (*this)[i];
        }
        ss << "]";
    }
    return ss.str();
}

void TransactionSequence::dump() const {
    cout << to_string();
}

ostream& operator<<(std::ostream& out, const TransactionSequence& sequence){
    out << sequence.to_string();
    return out;
}


} // namespace
