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


enum class UndoType : uint16_t {
    VERTEX_ADD,
    VERTEX_REMOVE,
    EDGE_ADD,
    EDGE_REMOVE,
};

enum UndoFlag : uint16_t {
    HAS_BACKWARD_POINTER = 0x1,
    HAS_INCOMING_LINK_FROM_STORAGE = 0x2, //
};

class UndoEntry {
    friend class TransactionContext;

    TransactionContext* m_transaction { nullptr };
    UndoEntry* m_next; // linked list of undo entries
    const UndoType m_type; // the type of entry
    uint16_t m_flags;
    const uint32_t m_length; // the length of the payload, including the length of the class UndoEntry

protected:
    UndoEntry(UndoEntry* next, UndoType type, uint32_t length);

    // Dump the content of the record to stdout, for debugging purposes
    uint64_t dump(int num_blank_spaces = 2) const;

    // Set/unset the given flag
    void set_flag(uint16_t flag, bool value);

    // Mark the undo entry with a backward pointer
    void set_flag_backward_pointer(bool value = true);

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

    // Check whether this entry has a backward pointer
    bool has_backward_pointer() const;

    void set_orphan();

    bool is_orphan() const;
};

class UndoEntryVertex : public UndoEntry {
    uint64_t m_vertex_id;
public:
    UndoEntryVertex(UndoType type, UndoEntryVertex* next, , uint64_t vertex_id);

    uint64_t vertex_id() const;

    const UndoEntryVertex* backward_pointer() const;
    UndoEntryVertex* backward_pointer();

    void set_backward_pointer(UndoEntryVertex* parent);
};

class UndoEntryEdge : public UndoEntry {
    union{ uint64_t m_source; UndoEntryEdge* m_previous; };
    uint64_t m_destination;
    double m_weight;

public:
    UndoEntryEdge(UndoEntryEdge* next, UndoType type, uint64_t source, uint64_t destination, double weight);

    uint64_t source() const;
    uint64_t destination() const;
    double weight() const;

    const UndoEntryEdge* backward_pointer() const;
    UndoEntryEdge* backward_pointer();
    void set_backward_pointer(UndoEntryEdge* parent);
};


template<typename UndoEntrySubclass, typename... Args>
UndoEntrySubclass* TransactionContext::create_undo_entry(Args&&... args){
    constexpr uint32_t entry_size = sizeof(UndoEntrySubclass);
    void* memory = allocate_undo_entry(entry_size);
    UndoEntrySubclass* entry = new (memory) UndoEntrySubclass(std::forward<Args>(args)...);
    entry->m_transaction = this;
    return entry;
}


} // namespace
