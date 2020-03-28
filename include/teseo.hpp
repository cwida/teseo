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


#include <stdexcept>
#include <string>

namespace teseo {

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
 * A logical error, due to the incorrect usage of the API or an incosistent state of the transaction
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
     * Retrieve the number of vertices in the graph
     */
    uint64_t num_vertices() const;

    /**
     * Retrieve the number of edges in the graph
     */
    uint64_t num_edges() const;

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
