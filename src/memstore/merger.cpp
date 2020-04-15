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

#include "merger.hpp"

#include <cassert>
#include <condition_variable>
#include <event2/event.h>
#include <future>
#include <mutex>
#include <vector>

#include "profiler/scoped_timer.hpp"
#include "util/miscellaneous.hpp"
#include "context.hpp"
#include "error.hpp"
#include "gate.hpp"
#include "key.hpp"
#include "rebalancer.hpp"
#include "sparse_array.hpp"

using namespace std;
using namespace teseo::internal::context;
using namespace teseo::internal::util;

namespace teseo::internal::memstore {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_CLASS "?"
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[" << COUT_DEBUG_CLASS << "::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Merger                                                                  *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_CLASS
#define COUT_DEBUG_CLASS "Merger"

Merger::Merger(SparseArray* sparse_array) : m_sparse_array(sparse_array), m_scratchpad(nullptr){
    m_scratchpad = new RebalancerScratchPad(m_sparse_array->get_num_segments_per_chunk() * 2 * m_sparse_array->get_num_qwords_per_segment() /2);
}

Merger::~Merger(){
    delete m_scratchpad; m_scratchpad = nullptr;
}

void Merger::execute(){
    COUT_DEBUG("init");
    profiler::ScopedTimer profiler { profiler::MERGER_EXECUTE };

    Key key = KEY_MIN; // the min fence key for the current chunk
    SparseArray::Chunk* previous = nullptr; // the last visited chunk
    uint64_t prev_sz = 0; // the number of slots occupied in the previous chunk
    const uint64_t MERGE_THRESHOLD = 0.6 * m_sparse_array->get_num_qwords_per_segment() * m_sparse_array->get_num_segments_per_chunk();

    do {
        ScopedEpoch epoch; // protect from the GC, before using #index_find()
        SparseArray::Chunk* current = SparseArray::get_chunk( m_sparse_array->index_find(key) );
        uint64_t cur_sz = visit_and_prune(current);

        // check #1: before acquiring any lock, can we, say at least hope to merge the previous &
        // current chunks together?
        assert(previous != current && "As only this service can merge together chunks, we cannot jump again on the same chunk twice");
        if(
                previous != nullptr && /* do we have a previous chunk ? */
                prev_sz + cur_sz < MERGE_THRESHOLD /* the space used is less than 75% */
                && m_sparse_array->get_fence_hkey(previous) == key /* previous & current are still siblings, i.e. no leaf splits occurred in the meanwhile */
        ) {
            prev_sz = xlock(previous);
            cur_sz = xlock(current);

            // check #2: this time we should have a better estimate of the actual size of both previous & current
            if(prev_sz + cur_sz < MERGE_THRESHOLD && m_sparse_array->get_fence_hkey(previous) == key){
                prev_sz = merge(previous, current);
                key = m_sparse_array->get_fence_hkey(previous); // next iteration
                xunlock(current, /* invalidate = */ true);
                xunlock(previous, /* invalidate = */ false);

                auto sa = m_sparse_array;
                auto deleter = [sa](SparseArray::Chunk* chunk){ sa->free_chunk(chunk); };
                global_context()->gc()->mark(current, deleter);
            } else {
                key = m_sparse_array->get_fence_hkey(current); // next iteration
                xunlock(current);
                xunlock(previous);

                // next iteration
                previous = current;
                prev_sz = cur_sz;
            }
        } else {
            // next iteration
            key = m_sparse_array->get_fence_hkey(current);
            previous = current;
            prev_sz = cur_sz;
        }
    } while (key != KEY_MAX);

    COUT_DEBUG("done");
}

uint64_t Merger::visit_and_prune(SparseArray::Chunk* chunk){
    profiler::ScopedTimer profiler { profiler::MERGER_VISIT_AND_PRUNE };

    uint64_t cur_sz = 0; // number of slots in use in the chunk
    for(uint64_t gate_id = 0; gate_id < m_sparse_array->get_num_gates_per_chunk(); gate_id++){
        Gate* gate = m_sparse_array->get_gate(chunk, gate_id);
        if(m_sparse_array->is_gate_dirty(chunk, gate)){
            int64_t window_start = gate->id() * m_sparse_array->get_num_segments_per_lock();
            int64_t window_length = m_sparse_array->get_num_segments_per_lock();

            xlock(gate);

            // Load & restore all records
            Rebalancer spad { m_sparse_array, window_length, window_length, *m_scratchpad };
            spad.load(chunk, window_start, window_length);
            spad.save(chunk, window_start, window_length);
            spad.validate();

            // Update the separator keys inside the gate
            m_sparse_array->update_separator_keys(chunk, gate, 0, window_length *2);

            // As the delta records have been compacted, update the amount of used space in the gate
            m_sparse_array->rebalance_recompute_used_space(chunk, gate);
            cur_sz += gate->m_used_space;

            // Update the time when this chunk was rebalanced for the last time
            gate->m_time_last_rebal = chrono::steady_clock::now();

            xunlock(gate);

        } else {
            cur_sz += gate->m_used_space;
        }
    }

    return cur_sz;
}

void Merger::xlock(Gate* gate){
    bool done = false;

    do {
        unique_lock<Gate> lock(*gate);
        switch(gate->m_state){
        case Gate::State::FREE:
            assert(gate->m_num_active_threads == 0 && "Precondition not satisfied");
            gate->m_state = Gate::State::WRITE;
            gate->m_num_active_threads = 1;
#if !defined(NDEBUG) /* for debugging purposes only */
            gate->m_writer_id = get_thread_id();
#endif
            done = true;
            break;
        case Gate::State::READ:
        case Gate::State::WRITE:
        case Gate::State::REBAL:
            std::promise<void> producer;
            std::future<void> consumer = producer.get_future();
            gate->m_queue.append({ Gate::State::WRITE, &producer } );
            lock.unlock();
            consumer.wait();
        }
    } while(!done);
}

void Merger::xunlock(Gate* gate){
    m_sparse_array->writer_on_exit(/* chunk, it doesn't matter */ nullptr, gate);
}

uint64_t Merger::merge(SparseArray::Chunk* previous, SparseArray::Chunk* current){
    profiler::ScopedTimer profiler { profiler::MERGER_MERGE };
    COUT_DEBUG("chunk 1: " << previous << ", chunk 2: " << current);

    int64_t window_length = m_sparse_array->get_num_segments_per_chunk();

    // Spread the content from `previous' & `current' into `current'
    Rebalancer spad { m_sparse_array, /* input */ 2 * window_length, /* output */ window_length, *m_scratchpad };
    spad.load(previous);
    spad.load(current);
    spad.save(previous);
    spad.validate();

    // Fence keys
    m_sparse_array->index_remove(previous);
    m_sparse_array->index_remove(current);
    auto lfkey = m_sparse_array->get_fence_lkey(previous);
    auto hfkey = m_sparse_array->get_fence_hkey(current);
    m_sparse_array->update_fence_keys(previous, 0, m_sparse_array->get_num_gates_per_chunk(), hfkey);
    m_sparse_array->get_gate(previous, 0)->m_fence_low_key = lfkey; // do not alter the lower fence key of the interval, as it's linked to the previous leaf
    m_sparse_array->index_insert(previous);
    m_sparse_array->validate_index(previous);

    // Update the amount of used space inside the gates
    uint64_t num_filled_qwords = m_sparse_array->rebalance_recompute_used_space(previous);

    return num_filled_qwords;
}

uint64_t Merger::xlock(SparseArray::Chunk* chunk){
    // init the context
    RebalancingContext context;
    context.m_can_continue = true;
    context.m_can_be_stopped = false;
    context.m_gate_start = 0;
    context.m_gate_end = m_sparse_array->get_num_gates_per_chunk();
    context.m_space_filled = 0;

    // lock the gate
    m_sparse_array->rebalance_chunk_xlock(chunk, &context);
    for(int64_t gate_id = 0, end = m_sparse_array->get_num_gates_per_chunk(); gate_id < end; gate_id ++){
        m_sparse_array->rebalance_chunk_acquire_gate(chunk, &context, /* inout */ gate_id, /* right direction ? */ true);
    }
    m_sparse_array->rebalance_chunk_xunlock(chunk);

    // wait for the threads in the wait list to leave their gate
    for(auto ptr_consumer : context.m_threads2wait){
        auto f = ptr_consumer->get_future(); // allocate the future object
        f.get(); // wait to be released by the other reader/writer
        delete ptr_consumer;
    }

    return context.m_space_filled;
}

void Merger::xunlock(SparseArray::Chunk* chunk, bool invalidate){
    for(int64_t gate_id = 0, end = m_sparse_array->get_num_gates_per_chunk(); gate_id < end; gate_id ++){
        m_sparse_array->rebalance_chunk_release_gate(chunk, gate_id, invalidate);
    }

    // Invalidate the chunk's rebalancer latch
    if(invalidate){
        chunk->m_latch.lock_write();
        while(chunk->m_active == true){
            std::promise<void> producer;
            std::future<void> consumer = producer.get_future();
            chunk->m_queue.append(&producer);
            chunk->m_latch.unlock_write();
            consumer.wait();
            chunk->m_latch.lock_write();
        }
        assert(chunk->m_active == false && "Someone else is operating at the chunk level");

        while(!chunk->m_queue.empty()){
            chunk->m_queue[0]->set_value();
            chunk->m_queue.pop();
        }

        chunk->m_latch.invalidate();
    }
}

/*****************************************************************************
 *                                                                           *
 *   MergerService                                                           *
 *                                                                           *
 *****************************************************************************/
#undef COUT_DEBUG_CLASS
#define COUT_DEBUG_CLASS "MergerService"

// synchronisation to start/stop the background thread
static std::mutex g_mutex;
static std::condition_variable g_condvar;

struct MergerCallbackData {
    MergerService* m_instance; // pointer to the background service
    promise<void>* m_producer; // pointer to the producer/consumer queue, only used for synchronous invocations through #execute_now()
};

MergerService::MergerService(SparseArray* sparse_array, std::chrono::milliseconds interval) : m_queue(nullptr), m_eventloop_exec(false), m_sparse_array(sparse_array), m_time_interval(interval) {
    if(m_sparse_array == nullptr) { throw std::invalid_argument("the sparse array instance is a nullptr"); }
    if(m_time_interval == 0ms) { throw std::invalid_argument("time interval is zero"); }

    libevent_init();
    m_queue = event_base_new();
    if(m_queue == nullptr) ERROR("Cannot initialise the libevent queue");
}

MergerService::~MergerService() {
    stop();
    event_base_free(m_queue); m_queue = nullptr;
    libevent_shutdown();
}

void MergerService::start(){
    COUT_DEBUG("Starting...");
    unique_lock<mutex> lock(g_mutex);
    if(m_background_thread.joinable()) ERROR("Invalid state. The background thread is already running");
    m_eventloop_exec = false;

    auto timer = duration2timeval(0s); // fire the event immediately
    int rc = event_base_once(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, &MergerService::callback_start, /* argument */ this, &timer);
    if(rc != 0) ERROR("Cannot initialise the event loop");

    m_background_thread = thread(&MergerService::main_thread, this);

    // create the periodic event, to invoke the service every `m_time_interval' millisecs
    MergerCallbackData* event_payload = (MergerCallbackData*) malloc(sizeof(MergerCallbackData));
    if(event_payload == nullptr) throw std::bad_alloc{};
    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_execute, event_payload);
    if(event == nullptr) throw std::bad_alloc{};
    event_payload->m_instance = this;
    event_payload->m_producer = nullptr;
    timer = duration2timeval(m_time_interval);
    rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: MergerService::start, event_add failed");
        std::abort(); // not sure what we can do here
    }

    g_condvar.wait(lock, [this](){ return m_eventloop_exec; });
    COUT_DEBUG("Started");
}

void MergerService::stop(){
    scoped_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) return;
    COUT_DEBUG("Stopping...");

    int rc = event_base_loopbreak(m_queue);
    if(rc != 0) ERROR("event_base_loopbreak");
    m_background_thread.join();

    // remove all enqueued events still in the queue
    std::vector<struct event*> pending_events = libevent_pending_events(m_queue);
    COUT_DEBUG("Pending events to remove: " << pending_events.size());
    for(auto e : pending_events){
        free(event_get_callback_arg(e));
        event_free(e);
    }

    COUT_DEBUG("Stopped");
}

void MergerService::execute_now(){
    // This method is not completely thread safe, because other threads may still stop the service while
    // the invoker of this method is still waiting for the request to be accomplished. But, anyway, this
    // method is supposed to be used only for debugging purposes.
    unique_lock<mutex> lock(g_mutex);
    if(!m_background_thread.joinable()) { ERROR("The service is not running"); }
    lock.unlock();

    // init the producer/consumer queue
    promise<void> producer;
    auto consumer = producer.get_future();

    // create the event
    MergerCallbackData* event_payload = (MergerCallbackData*) malloc(sizeof(MergerCallbackData));
    if(event_payload == nullptr) throw std::bad_alloc{};
    struct event* event = event_new(m_queue, /* fd, ignored */ -1, EV_TIMEOUT, callback_execute, event_payload);
    if(event == nullptr) throw std::bad_alloc{};
    event_payload->m_instance = this;
    event_payload->m_producer = &producer;
    // int rc = event_add(event, nullptr); // nullptr -> execute immediately // it didn't work!
    auto timer = duration2timeval(0s);
    int rc = event_add(event, &timer);
    if(rc != 0) {
        COUT_DEBUG_FORCE("FATAL: MergerService::execute_now, event_add failed");
        std::abort(); // not sure what we can do here
    }

    // wait for the execution to complete
    consumer.get();
}

void MergerService::callback_start(int fd, short flags, void* event_argument){
    COUT_DEBUG("Event loop started");

    MergerService* instance = reinterpret_cast<MergerService*>(event_argument);
    {
        unique_lock<mutex> lock(g_mutex); // not really necessary, but it does silence tsan
        instance->m_eventloop_exec = true;
    }
    g_condvar.notify_all();
}

void MergerService::callback_execute(int fd, short flags, void* /* MergerCallbackData */ raw_callback_arg){
    MergerCallbackData* callback_arg = reinterpret_cast<MergerCallbackData*>(raw_callback_arg);

    MergerService* instance = callback_arg->m_instance;
    SparseArray* sa = instance->m_sparse_array;
    Merger merger (sa);
    merger.execute();

    if(callback_arg->m_producer != nullptr){ // this is not the persistent event
        callback_arg->m_producer->set_value();
        free(callback_arg);
        free(event_base_get_running_event(instance->m_queue));
    }
}

void MergerService::main_thread(){
    COUT_DEBUG("Service thread started");
    set_thread_name("Teseo.Merger");
    m_sparse_array->global_context()->register_thread();

    // delegate libevent to run the loop
    int rc = event_base_loop(m_queue, EVLOOP_NO_EXIT_ON_EMPTY);
    if(rc != 0){ COUT_DEBUG_FORCE("event_base_loop rc: " << rc); }

    m_sparse_array->global_context()->unregister_thread();
    COUT_DEBUG("Service thread stopped");
}


} // namespace
