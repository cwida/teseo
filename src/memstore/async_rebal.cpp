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

#include "async_rebal.hpp"

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

#include "context/global_context.hpp"
#include "context/scoped_epoch.hpp"
#include "context/thread_context.hpp"
#include "profiler/scoped_timer.hpp"
#include "util/miscellaneous.hpp"
#include "error.hpp"
#include "gate.hpp"
#include "rebalancer.hpp"
#include "sparse_array.hpp"

using namespace teseo::internal::util;
using namespace std;

namespace teseo::internal::memstore {

static int64_t compute_single_gate_threshold(uint64_t num_slots_per_segment, uint64_t num_segments_per_gate);

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(teseo::internal::context::g_debugging_mutex); std::cout << "[AsyncRebalancerService::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/

AsyncRebalancerService::AsyncRebalancerService(SparseArray* sparse_array) : m_sparse_array(sparse_array),
        SINGLE_GATE_THRESHOLD(compute_single_gate_threshold(sparse_array->get_num_qwords_per_segment(), sparse_array->get_num_segments_per_lock())) {

}

AsyncRebalancerService::~AsyncRebalancerService(){
    stop();
}

void AsyncRebalancerService::start(){
    COUT_DEBUG("Starting...");
    m_mutex.lock();
    if(m_thread.joinable()){
        m_mutex.unlock();
        ERROR("Invalid state. The background thread is already running");
    }

    m_requests.clear();
    m_thread = thread(&AsyncRebalancerService::main_thread, this);
    m_mutex.unlock();
}

void AsyncRebalancerService::stop(){
    m_mutex.lock();
    if(!m_thread.joinable()){ m_mutex.unlock(); return; }
    COUT_DEBUG("Stopping...");
    m_requests.prepend(KEY_MAX);
    m_condvar.notify_one();
    m_mutex.unlock();
    m_thread.join();
}

void AsyncRebalancerService::request(Key key){
    m_mutex.lock();
    uint64_t i = 0;
    uint64_t sz = m_requests.size();
    while(i < sz && m_requests[i] != key) { i++; }
    if(i < sz){ // this key is already present in the queue
        m_mutex.unlock();
        return;
    }

    m_requests.append(key);
    m_mutex.unlock();

    m_condvar.notify_one();
}

/*****************************************************************************
 *                                                                           *
 *   Background service                                                      *
 *                                                                           *
 *****************************************************************************/
void AsyncRebalancerService::main_thread(){
    COUT_DEBUG("Started");
    set_thread_name("Teseo.Async");
    m_sparse_array->global_context()->register_thread();

    bool first_request = true;

    while(true){
        // extract the next key to process
        m_mutex.lock();
        if(!first_request) m_requests.pop(); // remove the last processed key
        m_condvar.wait(m_mutex, [this]{ return !m_requests.empty(); });
        Key key = m_requests[0];
        m_mutex.unlock();
        if(key == KEY_MAX) break; // terminate the while loop

        handle_request(key);

        first_request = false; // process the next request
    }

    m_sparse_array->global_context()->unregister_thread();
    COUT_DEBUG("Stopped");
}

void AsyncRebalancerService::handle_request(Key key){
    profiler::ScopedTimer profiler { profiler::ARS_HANDLE_REQUEST };
    COUT_DEBUG("Key: " << key);
    context::ScopedEpoch epoch; // protect from the GC
    SparseArray::Chunk* chunk { nullptr };
    Gate* gate { nullptr };

    try {
        bool rebalance_chunk = false;
        std::tie(chunk, gate) = m_sparse_array->writer_on_entry(key);

        if(gate->m_fence_low_key == key){
            COUT_DEBUG("chunk: " << chunk << ", gate: " << gate->id());

            if(gate->m_used_space < SINGLE_GATE_THRESHOLD){
                profiler::ScopedTimer profiler { profiler::ARS_REBALANCE_GATE };
                int64_t window_start = gate->id() * m_sparse_array->get_num_segments_per_lock();
                int64_t window_length = m_sparse_array->get_num_segments_per_lock();

                // Load & restore all records
                RebalancerScratchPad scratchpad { window_length * m_sparse_array->get_num_qwords_per_segment() /2 };
                Rebalancer spad { m_sparse_array, window_length, window_length, scratchpad };
                spad.load(chunk, window_start, window_length);
                spad.save(chunk, window_start, window_length);
                spad.validate();

                // Update the separator keys inside the gate
                m_sparse_array->update_separator_keys(chunk, gate, 0, window_length *2);

                // As the delta records have been compacted, update the amount of used space in the gate
                m_sparse_array->rebalance_recompute_used_space(chunk, gate);

                // Update the time when this chunk was rebalanced for the last time
                gate->m_time_last_rebal = chrono::steady_clock::now();
            } else {
                profiler::ScopedTimer profiler { profiler::ARS_REBALANCE_CHUNK };

                rebalance_chunk = true;

                m_sparse_array->rebalance_chunk(chunk, gate);
            }

        }

        if(!rebalance_chunk) {
            m_sparse_array->writer_on_exit(chunk, gate);
        }

    } catch (Abort) {
        if(gate != nullptr){
            m_sparse_array->writer_on_exit(chunk, gate);
        }
    } catch (RebalancingAbort){
        /* nop */
    }
}

static int64_t compute_single_gate_threshold(uint64_t num_slots_per_segment, uint64_t num_segments_per_gate){
    uint64_t num_slots_per_gate = num_slots_per_segment * num_segments_per_gate;

    return std::min<int64_t>(
            /* either 90% of the gate */ 0.9 * num_slots_per_gate,
            /* or, especially for debug runs, at least 6 qwords per segment */ num_slots_per_gate - 6 * num_segments_per_gate
    );
}


} // namespace
