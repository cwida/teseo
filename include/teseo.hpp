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

#include <ostream>
#include <stdexcept>
#include <string>

namespace teseo {

class Iterator; // forward declaration
class Teseo; // forward declaration
class Transaction; // forward declaration

/*****************************************************************************
 *                                                                           *
 *   Exceptions & errors                                                     *
 *                                                                           *
 *****************************************************************************/

/**
 * All exceptions thrown by Teseo are instances of the class or one of the subclasses of Error.
 */
class Exception : public std::runtime_error {
    const std::string m_class; // The actual class of this exception
    const std::string m_file; // The file path where the exception has been thrown
    int m_line; // The line in the file that thrown the exception
    const std::string m_function; // The funciton where the exception originated

public:
    Exception(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);

    int line() const;

    const std::string& file() const;

    const std::string& function() const;

    const std::string& exception_class() const;
};

/**
 * A logical error, due to the incorrect usage of the API or an inconsistent state of the transaction
 */
class LogicalError : public Exception {
public:
    LogicalError(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);
};

/**
 * An exception raised when attempting to alter a record currently locked by another pending transaction, that is a conflict.
 */
class TransactionConflict : public Exception {
public:
    TransactionConflict(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);
};

/**
 * Print to the output stream a description of the given error
 */
std::ostream& operator<<(const Exception& error, std::ostream& out);

/*****************************************************************************
 *                                                                           *
 * Iterators                                                                 *
 *                                                                           *
 *****************************************************************************/
/**
 * An Iterator allows to discover and fetch the edges stored in the database:
 * - it must be created through a Transaction object, by the method #iterator();
 * - the same instance for an Iterator can be reused to fetch the edges of different vertices;
 * - an instance of an Iterator is not thread-safe and is not meant to be shared among threads. However, multiple
 *   Iterator instances can be created from the same Transaction, also in different threads, and can operate safely
 *   concurrently;
 * - while an Iterator is "open", the related Transaction cannot be terminated, either by commit or by roll-back. First,
 *   all iterators must be either explicitly closed by the method #close or should go out of scope, where they will be
 *   implicitly closed by their destructor.
 */
class Iterator {
    void* m_pImpl; // opaque pointer to the implementation
    bool m_is_closed; // keep track whether this iterator is still active
    mutable int m_num_alive; // check whether the iterator can be closed

    // Iterator instances must be explicitly created by a transaction
    friend class Transaction;
    Iterator(void* pImpl);

public:
    /**
     * Copy constructor
     */
    Iterator(const Iterator&);

    /**
     * Assignment operator
     */
    Iterator& operator=(const Iterator&);

    /**
     * Destructor. It implicitly closes the iterator.
     */
    ~Iterator();

    /**
     * Fetch all outgoing edges attached to the given vertex. The edges are passed one by one,
     * in sorted order to the callback function cb. The callback should return `true' if it
     * requires to fetch the next edge in the list, or false to terminate the scan.
     * @param vertex the vertex ID we are interested to fetch all edges
     * @param logical whether the vertices rank, in [0, num_vertices) rather than their actual identifiers.
     * @param cb a function with a signature bool fn(uint64_t destination, double weight);
     */
    template<typename Callback>
    void edges(uint64_t vertex, bool logical, Callback&& cb) const;

    /**
     * Check whether this iterator is still active
     */
    bool is_closed() const noexcept;

    /**
     * Explicitly close this instance.
     * If the iterator was already previously closed, this operation becomes a nop.
     */
    void close();
};

/*****************************************************************************
 *                                                                           *
 * A transaction to operate over the database                                *
 *                                                                           *
 *****************************************************************************/
class Transaction {
    friend class Teseo;
    void* m_pImpl; // opaque pointer to the implementation

    // Actual ctor. Use Teseo#start_transaction() to create a new transaction
    Transaction(void* opaque_handle);

public:

    /**
     * Copy constructor
     */
    Transaction(const Transaction& copy);

    /**
     * Copy assignment
     */
    Transaction& operator=(const Transaction& copy);

    /**
     * Move constructor
     */
    Transaction(Transaction&& copy);

    /**
     * Move assignment operator
     */
    Transaction& operator=(Transaction&& copy);

    /**
     * Destructor
     */
    ~Transaction();

    /**
     * Insert a vertex in the graph
     * @param vertex_id the identifier of the vertex to insert
     * @throws TransactionConflict if the record to alter in the storage is
     *   currently locked by another pending transaction
     * @throws LogicalError if any of the following conditions occur:
     *   - if the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - if the vertex being inserted already exists
     */
    void insert_vertex(uint64_t vertex_id);

    /**
     * Check whether the given vertex is already present in the graph
     */
    bool has_vertex(uint64_t vertex_id) const;

    /**
     * Retrieve the number of edges attached to the given vertex
     * @param logical whether vertex_id refers to an actual vertex ID (false) or
     *        to its rank [0, num_vertices) in the transaction
     */
    uint64_t degree(uint64_t vertex_id, bool logical = false) const;

    /**
     * Remove the given vertex and all its attached edges
     * @param vertex_id the identifier of the vertex to remove
     * @return the number of attached edges removed
     * @throws LogicalError if any of the following conditions occur:
     *   - if the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - if the vertex id does not exist
     */
    uint64_t remove_vertex(uint64_t vertex_id);

    /**
     * Insert an edge in the graph
     * @param source the source vertex in the graph
     * @param destination the destination vertex in the graph
     * @param weight the weight associated to the edge
     * @throws TransactionConflict if the record to alter in the storage is
     *   currently locked by another pending transaction
     * @throws LogicalError if any of the following conditions occur:
     *   - if the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - if either the source or destination vertices do not already exist
     *   - if the edge being inserted already exists
     */
    void insert_edge(uint64_t source, uint64_t destination, double weight);

    /**
     * Check whether the given vertex is already present in the graph
     */
    bool has_edge(uint64_t source, uint64_t destination) const;

    /**
     * Retrieve the weight (payload) associated to the given edge
     */
    double get_weight(uint64_t source, uint64_t destination) const;

    /**
     * Remove an edge from the graph
     * @param source the source vertex in the graph
     * @param destination the destination vertex in the graph
     * @throws TransactionConflict if the record to alter in the storage is
     *   currently locked by another pending transaction
     * @throws LogicalError if any of the following conditions occur:
     *   - if the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - if the edge to remove does not exist
     */
    void remove_edge(uint64_t source, uint64_t destination);

    /**
     * Retrieve the number of vertices in the graph
     */
    uint64_t num_vertices() const;

    /**
     * Retrieve the number of edges in the graph
     */
    uint64_t num_edges() const;

    /**
     * Retrieve the logical ID of the given vertex. That is its rank among all vertices,
     * @return a value in [0, num_vertices)
     */
    uint64_t logical_id(uint64_t vertex_id) const;

    /**
     * Retrieve the vertex ID from the given logical ID
     */
    uint64_t vertex_id(uint64_t logical_id) const;

    /**
     * Check whether this transaction is read only
     */
    bool is_read_only() const;

    /**
     * Commit the transaction
     */
    void commit();

    /**
     * Roll back the transaction
     */
    void rollback();

    /**
     * Create a new iterator
     */
    Iterator iterator();

    /**
     * Opaque reference to the implementation handle, only for debugging purposes
     */
    void* handle_impl();
};

/*****************************************************************************
 *                                                                           *
 *   An instance of the database                                             *
 *                                                                           *
 *****************************************************************************/

class Teseo {
private:
    void* m_pImpl; // opaque pointer to the implementation

public:
    Teseo();

    ~Teseo();

    /**
     * Before creating any new transaction, any other thread besides the one that firstly created the
     * Teseo instance needs to register itself. Any thread can be only registered to a single Teseo
     * instance.
     */
    void register_thread();

    /**
     * And, of course, before terminating, any thread besides the main one, needs to unregister itself.
     */
    void unregister_thread();

    /**
     * Start a new transaction
     * @param read_only if true, all update methods (insert/remove vertex/edge) are disabled.
     */
    Transaction start_transaction(bool read_only = false);

    /**
     * Opaque reference to the implementation handle, only for debugging purposes
     */
    void* handle_impl();
};

} // namespace

// Implementation details, define the templates for the method Iterator#scan()
#if !defined(_TESEO_INTERNAL)
#include "teseo/interface/iterator.hpp"
#endif
