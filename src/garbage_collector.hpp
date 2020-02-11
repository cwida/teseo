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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional> // std::invoke
#include <iostream>
#include <mutex>
#include <memory>
#include <thread>

#include "circular_array.hpp"
#include "utility.hpp"

namespace teseo::internal {

// Forward declarations
class GlobalContext;

class GarbageCollector {
private:
    std::thread m_background_thread;
    std::atomic<bool> m_thread_can_execute = false;
    bool m_thread_is_running = false;
    GlobalContext* m_global_context;
    mutable std::mutex m_mutex; // sync
    mutable std::condition_variable m_condvar; // only to start the instance
    const std::chrono::milliseconds m_timer_interval; // sleep duration

    struct DeleteInterface {
        virtual void free(void* ptr) = 0;
        virtual ~DeleteInterface();
        void operator()(void* ptr){ free(ptr); } // syntactic sugar
    };
    template<typename T, typename Callable>
    struct DeleteImplementation : public DeleteInterface {
        Callable m_callable;

        DeleteImplementation(Callable callable) : m_callable(callable){ }
        void free(void* ptr) override {
            std::invoke(m_callable, reinterpret_cast<T*>(ptr)); // C++17
        }
    };
    struct Item {
        uint64_t m_timestamp; // the timestamp when this object has been added to the garbage collector
        void* m_pointer; // object to be deleted
        std::unique_ptr<DeleteInterface> m_deleter;
    };
    CircularArray<Item*> m_items_to_delete;

protected:
    void run();

    /**
     * Start the background thread for the garbage collector
     */
    void start(); // invoked by the ctor

    /**
     * Stop the background thread for the garbage collector
     */
    void stop(); // invoked by the dtor

public:
    /**
     * Create a new instance of the Garbage Collector, activate once a second
     */
    GarbageCollector(GlobalContext* instance);

    /**
     * Create a new instance of the Garbage Collector with the given timer interval when the GC is performed
     */
    GarbageCollector(GlobalContext* instance, std::chrono::milliseconds timer_interval);

    /**
     * Destructor
     */
    ~GarbageCollector();

    // Run a single pass of the garbage collector
    void perform_gc_pass();

    /**
     * Mark the given object for deletion
     */
    template<typename T, typename Callable>
    void mark(T* ptr, Callable callable);

    /**
     * Mark the given object for deletion. Release the memory using `delete ptr';
     */
    template<typename T>
    void mark(T* ptr);

    /**
     * Dump the list of items waiting to be deallocated
     */
    void dump(std::ostream& out) const;
    void dump() const;
};

// Implementation detail
template<typename T, typename Callable>
void GarbageCollector::mark(T* ptr, Callable callable){
    using namespace std;
    auto ts = rdtscp(); // read timestamp counter (cpu clock)
    lock_guard<mutex> lock(m_mutex);
    m_items_to_delete.append(new Item{ts, ptr, unique_ptr<DeleteInterface>{ new DeleteImplementation<T, Callable>(callable) }});
}
template<typename T>
void GarbageCollector::mark(T* ptr){
    auto deleter = [](T* ptr){ delete ptr; };
    mark(ptr, deleter);
}

} // namespace
