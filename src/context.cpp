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

#include "context.hpp"

#include <bits/stdint-uintn.h>
#include <teseo.hpp>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <limits>
#include <mutex>

#include "error.hpp"
#include "garbage_collector.hpp"
#include "utility.hpp"

using namespace std;

namespace teseo::internal {

static thread_local ThreadContext* g_thread_context {nullptr};
std::mutex g_debugging_mutex; // to sync output messages to the stdout, for debugging purposes

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
#define DEBUG
#define COUT_CLASS_NAME "Unknown"
#define COUT_DEBUG_FORCE(msg) { std::scoped_lock<mutex> lock(g_debugging_mutex); std::cout << "[" << COUT_CLASS_NAME << "::" << __FUNCTION__ << "] [" << get_thread_id() << "] " << msg << std::endl; }
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *  GlobalContext                                                            *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME /* debug only */
#define COUT_CLASS_NAME "GlobalContext"

GlobalContext::GlobalContext() : m_garbage_collector( new GarbageCollector(this) ){
    register_thread();
}

GlobalContext::~GlobalContext(){
    unregister_thread(); // if still alive

    // stop the garbage collector
    delete m_garbage_collector; m_garbage_collector = nullptr;
}

void GlobalContext::register_thread(){
    // well, this should be a warning, if already registered
    if(g_thread_context != nullptr){ unregister_thread(); }
    g_thread_context = new ThreadContext(this);
    COUT_DEBUG("context: " << g_thread_context);

    // append the new context to the chain of existing contexts
    lock_guard<OptimisticLatch<0>> xlock(m_tc_latch);
    g_thread_context->m_next = m_tc_head;
    m_tc_head = g_thread_context;
}

void GlobalContext::unregister_thread(){
    if(g_thread_context == nullptr) return; // nop
    COUT_DEBUG("context: " << g_thread_context);

    // remove the current context in the chain of contexts
    bool done = false;
    ThreadContext* parent { nullptr };
    ThreadContext* current { nullptr };
    uint64_t version_parent {0}, version_current {0};
    do {

        try {
            parent = current = nullptr; // reinit
            g_thread_context->epoch_enter();

            assert(m_tc_head != nullptr && "At least the current thread_context must be in the linked list");
//            OptimisticLatch<0>* latch = &m_tc_latch;
            version_parent = m_tc_latch.read_version();
            current = m_tc_head;
            m_tc_latch.validate_version(version_parent); // current is still valid as a ptr
            version_current = current->m_latch.read_version();
            m_tc_latch.validate_version(version_parent); // current was still the first item in m_tc_head when the version was read

            // find m_thread_context in the list of contexts
            while(current != g_thread_context){
                parent = current;
                version_parent = version_current;

                current = current->m_next;
                assert(current != nullptr && "It cannot be a nullptr, because we haven't reached the current thread_context yet and it must be still present in the chain");

                parent->m_latch.validate_version(version_parent); // so far what we read from parent is good
                version_current = current->m_latch.read_version();
                parent->m_latch.validate_version(version_parent); // current is still the next node in the chain after parent
            }

            // acquire the xlock on the parent and the current node
            OptimisticLatch<0>& latch_parent = (parent == nullptr) ? m_tc_latch : parent->m_latch;
            latch_parent.update(version_parent);

            OptimisticLatch<0>& latch_current = current->m_latch;
            try {
                latch_current.update(version_current);
            } catch (Abort) {
                latch_parent.unlock();
                throw; // restart again
            }

            if(parent == nullptr){
                m_tc_head = g_thread_context->m_next;
            } else {
                parent->m_next = g_thread_context->m_next;
            }

            latch_parent.unlock();
            latch_current.invalidate(); // invalidate the current node

            done = true;
        } catch (Abort) { /* retry again */ }
    } while (!done);

    g_thread_context->epoch_exit();

    gc()->mark(g_thread_context);
    g_thread_context = nullptr;
}


uint64_t GlobalContext::min_epoch() const {
    uint64_t epoch = 0;
    bool done = false;

    do {
        try {
            epoch = numeric_limits<uint64_t>::max(); // reinit

            OptimisticLatch<0>* latch = &m_tc_latch;
            uint64_t version1 = latch->read_version();
            ThreadContext* child = m_tc_head;
            latch->validate_version(version1);
            if(child == nullptr) return epoch; // there are no registered contexts
            uint64_t version2 = child->m_latch.read_version();
            latch->validate_version(version1);
            version1 = version2;

            while(child != nullptr){
                ThreadContext* parent = child;
                epoch = std::min(epoch, parent->epoch());
                child = child->m_next;
                if(child != nullptr)
                    version2 = child->m_latch.read_version();
                parent->m_latch.validate_version(version1);

                version1 = version2;
            }

            done = true;

        } catch(Abort) { } /* retry */
    } while (!done);


    return epoch;
}

GlobalContext* GlobalContext::context(){
    return ThreadContext::context()->global_context();
}

GarbageCollector* GlobalContext::gc() const noexcept {
    return m_garbage_collector;
}

uint64_t GlobalContext::generate_transaction_id() {
    return m_txn_global_counter++;
}

void GlobalContext::dump() const {
    cout << "[Local contexts]\n";
    ThreadContext* local = m_tc_head;
    cout << "0. (head): " << local << " => "; local->dump();
    int i = 0;
    while(local->m_next != nullptr){
        local = local->m_next;
        cout << i << ". : " << local << "=> "; local->dump();

        i++;
    }
    cout << "\n";

    gc()->dump();
}

/*****************************************************************************
 *                                                                           *
 *  ThreadContext                                                            *
 *                                                                           *
 *****************************************************************************/
#undef COUT_CLASS_NAME /* debug only */
#define COUT_CLASS_NAME "ThreadContext"

ThreadContext::ThreadContext(GlobalContext* global_context) : m_global_context(global_context), m_next(nullptr), m_gc_tail(nullptr), m_gc_head(nullptr)
#if !defined(NDEBUG)
    , m_thread_id(get_thread_id())
#endif
{
    epoch_exit();
}

void ThreadContext::epoch_enter() {
    m_epoch = rdtscp();
}

void ThreadContext::epoch_exit() {
    m_epoch = numeric_limits<uint64_t>::max();
}

uint64_t ThreadContext::epoch() const {
    return m_epoch;
}

GlobalContext* ThreadContext::global_context() const noexcept {
    return m_global_context;
}

ThreadContext* ThreadContext::context(){
    if(g_thread_context == nullptr)
        RAISE(LogicalError, "No context for this thread. Use the function Database::register_thread() to associate the thread to a given Database");
    return g_thread_context;
}

TransactionContext* ThreadContext::transaction(){
    return context()->txn();
}

TransactionContext* ThreadContext::txn() const {
    TransactionContext* ptr = m_transaction.get();
    if(ptr == nullptr){ RAISE_EXCEPTION(LogicalError, "There is no active transaction in the current thread"); }
    return ptr;
}

TransactionContext* ThreadContext::txn_start(){
    if(m_transaction.get() != nullptr && m_transaction->state() == TransactionState::PENDING){
        RAISE_EXCEPTION(LogicalError, "There is already a pending transaction registered to the current thread");
    }

    auto deleter = [this](TransactionContext* txn) { txn_mark_for_gc(txn); };
    m_transaction.reset( new TransactionContext( global_context()->generate_transaction_id() ), deleter );
    return m_transaction.get();
}

void ThreadContext::txn_join(TransactionContext* txn){
    auto deleter = [this](TransactionContext* txn) { txn_mark_for_gc(txn); };
    m_transaction.reset(txn, deleter);
}

void ThreadContext::txn_leave() {
    m_transaction.reset();
}

void ThreadContext::txn_mark_for_gc(TransactionContext* txn){
    if(m_gc_head == nullptr){
        assert(m_gc_tail == nullptr);
        m_gc_head = m_gc_tail = txn;
    } else {
        m_gc_head->m_next = txn;
        m_gc_head = txn;
    }
}

void ThreadContext::dump() const {
#if !defined(NDEBUG)
    cout << "thread_id: " << m_thread_id << ", ";
#endif
    cout << "epoch: " << epoch();

    if(m_transaction.get() != nullptr){
        cout << ", transaction: " << m_transaction.get();
    }
    cout << "\n";

    if(m_transaction.get() != nullptr){
        cout << "  TXN: ";
        m_transaction->dump();
    }
}


/*****************************************************************************
 *                                                                           *
 *  ScopedEpoch                                                              *
 *                                                                           *
 *****************************************************************************/

ScopedEpoch::ScopedEpoch () { bump(); }
ScopedEpoch::~ScopedEpoch() { ThreadContext::context()->epoch_exit(); }
void ScopedEpoch::bump() { ThreadContext::context()->epoch_enter(); }

/*****************************************************************************
 *                                                                           *
 *  TransactionContext                                                       *
 *                                                                           *
 *****************************************************************************/

TransactionContext::TransactionContext(uint64_t transaction_id) : m_transaction_id(transaction_id), m_state(TransactionState::PENDING), m_undo_last(&m_undo_buffer) {
    // m_undo_last = &m_undo_buffer;
}

TransactionContext::~TransactionContext(){
    if(m_state == TransactionState::PENDING){ do_abort(); }
}

uint64_t TransactionContext::tx_write_id() const {
    if(state() == TransactionState::PENDING)
        return tx_read_id() + (numeric_limits<uint64_t>::max()>>1);
    else {
//        RAISE_EXCEPTION(LogicalError, "The transaction is closed, no write_id available");
        return tx_read_id();
    }
}

void* TransactionContext::allocate_undo_entry(uint32_t length){
    assert(length <= UndoTransactionBuffer::BUFFER_SZ && "This entry won't fit any undo buffer");
    UndoTransactionBuffer* undo_buffer = m_undo_last;
    if(undo_buffer->m_space_left < length){
        undo_buffer = new UndoTransactionBuffer();
        undo_buffer->m_next = m_undo_last;
        m_undo_last = undo_buffer;
    }

    void* ptr = undo_buffer->m_buffer + undo_buffer->m_space_left - length;
    COUT_DEBUG("ptr: " << ptr);
    undo_buffer->m_space_left -= length;
    return ptr;
}

void TransactionContext::try_release_context(){
    auto context = ThreadContext::context();
    try {
        if(context->txn() == this){
            context->txn_leave();
        }
    } catch( LogicalError& ){
        // ignore, the tx will be eventually recycled later
    }
}

void TransactionContext::commit(){
    COUT_DEBUG("tx " << tx_read_id());

    { // restrict the scope for the latch
        WriteLatch lock(m_latch);
        if(m_state != TransactionState::PENDING) { RAISE_EXCEPTION(LogicalError, "The transaction is already terminated"); }

        m_state = TransactionState::COMMITTED;
        m_transaction_id = ThreadContext::context()->global_context()->generate_transaction_id();
    }

    try_release_context();
}

void TransactionContext::abort(){
    COUT_DEBUG("tx " << tx_read_id());

    { // restrict the scope for the latch
        WriteLatch lock(m_latch);
        if(m_state != TransactionState::PENDING) { RAISE_EXCEPTION(LogicalError, "The transaction is already terminated"); }

        do_abort();
    }

    try_release_context();
}

void TransactionContext::do_abort(){

    // FIXME, go through the undo list and restore the changes
    // ...

    m_state = TransactionState::ABORTED;
}



void TransactionContext::dump() const {
    cout << "state: ";
    switch(m_state){
    case TransactionState::PENDING:
        cout << "pending, tx id read: " << tx_read_id() << ", write: " << tx_write_id();
        break;
    case TransactionState::COMMITTED:
        cout << "committed, tx id: " << tx_read_id();
        break;
    case TransactionState::ABORTED:
        cout << "aborted, tx id: " << tx_read_id();
        break;
    }

    UndoTransactionBuffer* undo_buffer = m_undo_last;
    while(undo_buffer != nullptr){
        uint64_t* buffer = reinterpret_cast<uint64_t*>(undo_buffer->m_buffer) + undo_buffer->m_space_left / 8;
        uint64_t buffer_sz = (UndoTransactionBuffer::BUFFER_SZ - undo_buffer->m_space_left) / 8;
        uint64_t i = 0;
        while(i < buffer_sz){
            cout << "\n";
            i += reinterpret_cast<UndoEntry*>(buffer + i)->dump();
        }

        undo_buffer = undo_buffer->m_next;
    }

    cout << "\n";
}



/*****************************************************************************
 *                                                                           *
 *  Undo entries                                                             *
 *                                                                           *
 *****************************************************************************/

UndoEntry::UndoEntry(UndoEntry* next, UndoType type, uint32_t length) : m_transaction(ThreadContext::context()->txn()), m_next(next), m_type(type), m_flags(0), m_length(length) {

}

TransactionContext* UndoEntry::transaction(){
    return m_transaction;
}

uint64_t UndoEntry::transaction_id() {
    return transaction()->tx_read_id();
}

uint64_t UndoEntry::dump(int num_blank_spaces) const {
    for(int i = 0; i < num_blank_spaces; i++) cout << " ";
    uint64_t entry_sz = 0;

    cout << "undo [tx " << m_transaction->tx_read_id();
    if(m_transaction->state() == TransactionState::PENDING){
        cout << ", pending id " << m_transaction->tx_write_id();
    }
    cout << "]: ";

    switch(m_type){
    case UndoType::VERTEX_ADD:
    case UndoType::VERTEX_REMOVE: {
        cout << (m_type == UndoType::VERTEX_ADD ? "VERTEX_ADD" : "VERTEX_REMOVE");
        cout << ", vertex_id: " << reinterpret_cast<const UndoEntryVertex*>(this)->vertex_id();
        entry_sz = sizeof(UndoEntryVertex) / 8;
        break;
    }
    }
    cout << ", next: " << m_next;

    if(m_next != nullptr){
        cout << "\n";
        m_next->dump(num_blank_spaces + 2);
    }

    return entry_sz;
}

UndoEntry* UndoEntry::next() const { return m_next; }
UndoType UndoEntry::type() const { return m_type; }
uint32_t UndoEntry::length() const { return m_length; }
bool UndoEntry::can_write(UndoEntry* entry) {
    if(entry == nullptr) return true; // base version, everyone can write it
    TransactionContext* myself = ThreadContext::transaction();

    return ( entry->m_transaction == myself ) /* either locked by myself */
            || ( entry->transaction()->state() != TransactionState::PENDING && entry->transaction()->tx_read_id() < myself->tx_read_id() ); /* or it belongs to a tx terminated before I started */
}

void UndoEntry::set_flag(uint16_t flag, bool value){
    if(value){
        m_flags |= flag;
    } else {
        m_flags &= ~flag;
    }
}

bool UndoEntry::has_backward_pointer() const {
    return m_flags & UndoFlag::HAS_BACKWARD_POINTER;
}

void UndoEntry::set_flag_backward_pointer(bool value){
    set_flag(UndoFlag::HAS_BACKWARD_POINTER, value);
}

bool UndoEntry::is_locked_by_this_txn() const {
    return m_transaction == ThreadContext::transaction();
}

UndoEntryVertex::UndoEntryVertex(UndoEntryVertex* next, UndoType type, uint64_t vertex_id): UndoEntry(next, type, sizeof(UndoEntryVertex)), m_vertex_id(vertex_id) {
    if(next != nullptr){ next->set_backward_pointer(this); }

}
uint64_t UndoEntryVertex::vertex_id() const { return has_backward_pointer() ? backward_pointer()->vertex_id() : m_vertex_id;}
const UndoEntryVertex* UndoEntryVertex::backward_pointer() const { return has_backward_pointer() ? reinterpret_cast<const UndoEntryVertex*>(m_vertex_id) : nullptr; }
UndoEntryVertex* UndoEntryVertex::backward_pointer() { return has_backward_pointer() ? reinterpret_cast<UndoEntryVertex*>(m_vertex_id) : nullptr; }
void UndoEntryVertex::set_backward_pointer(UndoEntryVertex* parent){
    m_vertex_id = reinterpret_cast<uint64_t>(parent);
    set_flag_backward_pointer(true);
}

UndoEntryEdge::UndoEntryEdge(UndoEntryEdge* next, UndoType type, uint64_t source, uint64_t destination, double weight) : UndoEntry(next, type, sizeof(UndoEntryEdge)){
    m_source = source;
    m_destination = destination;
    m_weight = weight;

    if(next != nullptr){ next->set_backward_pointer(this); }
}

uint64_t UndoEntryEdge::source() const { return has_backward_pointer() ? backward_pointer()->source() : m_source; }
uint64_t UndoEntryEdge::destination() const { return m_destination; }
double UndoEntryEdge::weight() const { return m_weight; }
const UndoEntryEdge* UndoEntryEdge::backward_pointer() const { return has_backward_pointer() ? m_previous : nullptr; }
UndoEntryEdge* UndoEntryEdge::backward_pointer() { return has_backward_pointer() ? m_previous : nullptr; }
void UndoEntryEdge::set_backward_pointer(UndoEntryEdge* parent){
    m_previous = parent;
    set_flag_backward_pointer(true);
}



} // namespace
