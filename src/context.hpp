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

#include <cinttypes>
#include <memory>
#include <vector>

#include "latch.hpp"

namespace teseo::internal {

// forward declarations
class GarbageCollector;
class GlobalContext;
class ThreadContext;
class TransactionContext;

// sync messages to stdout, for debugging purposes only
extern std::mutex g_debugging_mutex;

class GlobalContext {
    GlobalContext(const GlobalContext&) = delete;
    GlobalContext& operator=(const GlobalContext& ) = delete;

    ThreadContext* m_tc_head {nullptr}; // linked list of the registered contexts
    mutable OptimisticLatch<0> m_tc_latch; // latch for the head of registered contexts
    std::atomic<uint64_t> m_txn_global_counter = 0; // global counter, where the startTime and commitTime for transactions are drawn
    GarbageCollector* m_garbage_collector { nullptr }; // centralised garbage collector

public:

    GlobalContext(); // ctor

    /**
     * Destructor
     */
    ~GlobalContext();

    void register_thread();

    void unregister_thread();

    uint64_t min_epoch() const;

    /**
     * Retrieve the current global context
     */
    static GlobalContext* context();

    /**
     * Instance to the garbage collector
     */
    GarbageCollector* gc() const noexcept;

    /**
     * Dump the content of the global context, for debugging purposes
     */
    void dump() const;

    /**
     * Generate a new transaction id from the global counter, to be used for the startTime & commitTime
     */
    uint64_t generate_transaction_id();
};

class ThreadContext {
    friend class GlobalContext;

    GlobalContext* m_global_context;
    uint64_t m_epoch;
    OptimisticLatch<0> m_latch;
    ThreadContext* m_next; // next thread context in the chain
    std::shared_ptr<TransactionContext> m_transaction; // transaction context
    TransactionContext *m_gc_tail, *m_gc_head; // transactions to remove

#if !defined(NDEBUG) // thread contexts are always associated to a single logical thread, keep thrack of its ID for debugging purposes
    const int64_t m_thread_id;
#endif

    // Append a transaction in the internal list of the distributed garbage collector
    void txn_mark_for_gc(TransactionContext* txn);

public:
    ThreadContext(GlobalContext* global_context);

    void epoch_enter();


    void epoch_exit();

    uint64_t epoch() const;

    /**
     * Retrieve the global context associated to the given local context
     */
    GlobalContext* global_context() const noexcept;

    /**
     * Retrieve the current local context
     */
    static ThreadContext* context();


    /**
     * Retrieve the current local transaction
     */
    static TransactionContext* transaction();


    /**
     *  Retrieve the transaction associated to the current thread
     */
    TransactionContext* txn() const;

    /**
     * Start a new transaction and associate to the given context
     */
    TransactionContext* txn_start();

    /**
     * Associate the given transaction to this thread context
     */
    void txn_join(TransactionContext* tx);

    /**
     * Remove the transaction from the thread context
     */
    void txn_leave();

    /**
     * Dump the content of this context to stdout, for debugging purposes
     */
    void dump() const;
};


/**
 * Automatically enter & exit from an epoch in the current thread context
 */
class ScopedEpoch {
public:
    ScopedEpoch(); // set the current epoch
    ~ScopedEpoch();

    void bump(); // update the current epoch
};


struct UndoTransactionBuffer {
    constexpr static uint64_t BUFFER_SZ = 264192; // 256kb
    char m_buffer[BUFFER_SZ];
    uint64_t m_space_left = BUFFER_SZ;
    UndoTransactionBuffer* m_next {nullptr}; // pointer to the next undo log in the chain
};


enum class TransactionState : uint8_t {
    PENDING,
    COMMITTED,
    ABORTED
};


class TransactionContext {
    UndoTransactionBuffer m_undo_buffer; // first undo log in the chain
    Latch m_latch;
    uint64_t m_transaction_id; // either the startTime or commitTime of the transaction, depending on m_state
    TransactionState m_state;
    UndoTransactionBuffer* m_undo_last; // last undo log in the chain

public:TransactionContext* m_next {nullptr}; // next transaction in the linked list of the garbage collector, maintained by the ThreadContext
private:

    // Claim space for an undo entry in the log buffer
    void* allocate_undo_entry(uint32_t length);

    void do_abort();

    // Attempt to remove the current txn from the thread context
    void try_release_context();

public:
    TransactionContext(uint64_t transaction_id);

    ~TransactionContext();

    // Create a new undo entry
    template<typename Undo, typename... Args>
    Undo* create_undo_entry(Args&&... args);

    // The temporary transaction ID used for writes
    uint64_t tx_write_id() const;

    // The actual transaction ID, either the startTime or the commitTime of the transaction
    uint64_t tx_read_id() const { return m_transaction_id; }

    // The state of the current transaction
    TransactionState state() const { return m_state; }

    void commit();

    void abort();

    // Dump the content of the transaction to stdout, for debugging purposes
    void dump() const;
};


enum class UndoType : uint32_t {
    VERTEX_ADD,
    VERTEX_REMOVE
};

class UndoEntry {
    friend class TransactionContext;

    TransactionContext* m_transaction { nullptr };
    UndoEntry* m_next; // linked list of undo entries
    const UndoType m_type; // the type of entry
    const uint32_t m_length; // the length of the payload, including the length of the class UndoEntry

protected:
    UndoEntry(UndoEntry* next, UndoType type, uint32_t length);

    // Dump the content of the record to stdout, for debugging purposes
    uint64_t dump(int num_blank_spaces = 2) const;

public:
    TransactionContext* transaction();
    uint64_t transaction_id();

    // Retrieve the next undo entry
    UndoEntry* next() const;

    // Retrieve the size (in bytes) of this entry in the undo buffer
    uint32_t length() const;

    // Retrieve the type of this entry
    UndoType type() const;

    // Check whether we can write the current version / undo entry
    static bool can_write(UndoEntry* version);

    bool is_locked_by_this_txn() const;
};

class UndoEntryVertex : public UndoEntry {
    const uint64_t m_vertex_id;
public:
    UndoEntryVertex(UndoEntry* next, UndoType type, uint64_t vertex_id);

    uint64_t vertex_id() const;
};

//class UndoEntryVertexLogicCount : public UndoEntry {
//    const uint64_t m_vertex_id;
//    int64_t m_count;
//
//public:
//    UndoEntryVertexLogicCount(UndoEntry* next, uint64_t vertex_id, int64_t count);
//    uint64_t get_vertex_id() const;
//    int64_t get_count() const;
//    void increment_count(int64_t count_diff);
//    void set_count(int64_t value);
//};


template<typename UndoEntrySubclass, typename... Args>
UndoEntrySubclass* TransactionContext::create_undo_entry(Args&&... args){
    constexpr uint32_t entry_size = sizeof(UndoEntrySubclass);
    void* memory = allocate_undo_entry(entry_size);
    UndoEntrySubclass* entry = new (memory) UndoEntrySubclass(std::forward<Args>(args)...);
    entry->m_transaction = this;
    return entry;
}


} // namespace
