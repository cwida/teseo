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
 * All exceptions thrown by Teseo are instances of this class or one of its subclasses.
 */
class Exception : public std::runtime_error {
    const std::string m_class; // The actual class of this exception
    const std::string m_file; // The file path where the exception was thrown
    int m_line; // The line in the file where the exception was thrown
    const std::string m_function; // The function where the exception originated

public:
    /**
     * Initialise a generic Exception, for internal usage
     */
    Exception(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);

    /**
     * Get the line in the source code where the exception was thrown
     */
    int line() const;

    /**
     * Get the file name of the source code where this exception was thrown
     */
    const std::string& file() const;

    /**
     * Get the name of the function in the source code where this exception was thrown
     */
    const std::string& function() const;

    /**
     * Get the name of the subclass of this exception
     */
    const std::string& exception_class() const;
};

/**
 * A logical error, due to the incorrect usage of the API or an inconsistent state of the transaction.
 */
class LogicalError : public Exception {
public:
    LogicalError(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);
};

/**
 * A logical error related to a vertex. It is raised when attempting to refer to a non existing vertex or to re-insert a vertex that already exists.
 */
class VertexError: public LogicalError {
    const uint64_t m_vertex; // the vertex being referred

public:
    VertexError(uint64_t vertex, const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);

    // Retrieve the vertex being referred
    uint64_t vertex() const noexcept;
};

/**
 * A logical error related to an edge. It is thrown when attempting to remove a non existing edge or to re-insert an edge that already exists.
 */
class EdgeError: public LogicalError {
    const uint64_t m_source; // the source vertex of the edge
    const uint64_t m_destination; // the destination vertex of the edge

public:
    EdgeError(uint64_t source, uint64_t destination, const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);

    // Retrieve the source vertex of the edge
    uint64_t source() const noexcept;

    // Retrieve the destination vertex of the edge
    uint64_t destination() const noexcept;
};

/**
 * An exception raised when attempting to alter a record currently locked by another pending transaction, that is, a conflict.
 */
class TransactionConflict : public Exception {
public:
    TransactionConflict(const std::string& exc_class, const std::string& message, const std::string& file, int line, const std::string& function);
};

/**
 * Print to the output stream a description of the given error.
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
    void* m_cursor_state; // opaque pointer to its state
    bool m_is_open; // keep track whether this iterator is still active
    mutable int m_num_alive; // number of cursors currently active, by means of nesting, spawned by this iterator

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
     * @param logical whether the param vertex is a rank, in [0, num_vertices), among all vertices,
     *        rather than an actual vertex identifier. If set, also the destination identifiers in
     *        the callback will refer to logical vertices.
     * @param cb a function with any of the following signatures:
     *        a. bool fn(uint64_t destination)
     *        b. void fn(uint64_t destination)
     *        c. bool fn(uint64_t destination, double weight)
     *        d. void fn(uint64_t destination, double weight)
     *        Versions a. and b. only fetch the destination vertex, versions c. and d. retrieve both
     *        the vertex and the associated weight. Versions a. and c. must return a boolean to either
     *        fetch the next vertex (true) or stop the iterator (false), versions b. and d. always
     *        retrieve all edges attached to the source vertex.
     */
    template<typename Callback>
    void edges(uint64_t vertex, bool logical, Callback&& cb) const;

    /**
     * Check whether this iterator is still active
     */
    bool is_open() const noexcept;

    /**
     * Explicitly close this instance.
     * If the iterator was already previously closed, this operation becomes a nop.
     */
    void close();

    /**
     * Opaque pointer to its internal state. This is only used for testing purposes.
     */
    void* state_impl();
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
     *   - the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - the vertex being inserted already exists
     */
    void insert_vertex(uint64_t vertex_id);

    /**
     * Check whether the given vertex is already present in the graph
     */
    bool has_vertex(uint64_t vertex_id) const;

    /**
     * Retrieve the number of edges attached to the given vertex
     * @param id either a vertex id or a logical id
     * @param logical whether vertex_id refers to an actual vertex ID (false) or
     *        to its rank [0, num_vertices) in the transaction
     */
    uint64_t degree(uint64_t id, bool logical = false) const;

    /**
     * Remove the given vertex and all its attached edges
     * @param vertex_id the identifier of the vertex to remove
     * @return the number of attached edges removed
     * @throws LogicalError if any of the following conditions occur:
     *   - the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - the vertex id does not exist
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
     *   - the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - either the source or destination vertices do not already exist
     *   - the edge being inserted already exists
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
     *   - the transaction was created in read only mode
     *   - the transaction has already been terminated, by roll back or commit
     *   - the edge to remove does not exist
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
     * Opaque reference to the implementation handle. This is only used for testing purposes.
     */
    void* handle_impl();
};

/*****************************************************************************
 *                                                                           *
 *   An instance of the database                                             *
 *                                                                           *
 *****************************************************************************/

/**
 * A global instance of the Database
 */
class Teseo {
private:
    void* m_pImpl; // opaque pointer to the implementation

public:
    // Initialise the database
    Teseo();

    // Destructor
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

// Implementation details. It defines the templates for the method Iterator#scan()
#if !defined(_TESEO_INTERNAL)
#include "teseo/interface/iterator.hpp"
#endif
