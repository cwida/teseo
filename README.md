---
Teseo
---

This is the source code of Teseo, the system described in the paper [D. De Leo, P. Boncz, Teseo and the Analysis of Structural Dynamic Graphs, VLDB 2021](http://vldb.org/pvldb/vol14/p1053-leo.pdf) (+ [Errata](http://www.vldb.org/pvldb/vol14/p2271-leo.pdf)).
To repeat the experiments in the paper, refer to the [GFE driver](https://github.com/cwida/gfe_driver).
This page describes how to build and use the library, a documentation promised to the final reviewers of the paper :-)

For any question, you can reach me at github[at]whatsthecraic[dot]net . 


### Build

#### Prerequisites 
- Only Linux is supported.
- Autotools, [Autoconf 2.69+](https://www.gnu.org/software/autoconf/)
- A C++17 compiler. We relied on both Clang 10 and GCC 10 for the experiments in the paper.
- libnuma 2.0 +
- [libevent 2.1.1+](https://libevent.org/)

#### Build instructions 

Only the first time, generate the configure script from the root directory with `./autoreconf -iv`, this is where Autotools is required. To build the library, create a build directory and, from there, run the `configure` script spawned by Autotools. There are mainly three kinds of builds: 

1. `./configure` (without parameters): DEBUG build with debug symbols, assertions and other additional debug code enabled. Suitable for development.
1. `./configure --enable-test`, similar to a DEBUG build, but with the length of the data structures significantly smaller. Suitable for unit testing (see below).
1. `./configure --enable-optimize --disable-debug`: RELEASE build, it passes to the compiler the flags `-O3 -march=native -mtune=native -fno-stack-protector`. It does not contain  debug symbol, assertions and the extra debug code. This is the configuration used for the experiments of the paper.

Type `./configure --help` for the whole set of options.
Finally, to build the library, run `make`. The following is the complete snippet to create a RELEASE build: 

```
./autoreconf -iv
mkdir build && cd build
../configure --enable-optimize --disable-debug
make -j
```

Note that there is not a `make install` target, in contrast to what commonly found in other configure/make scripts.

#### Linking

For the compiler, include (option -I) both the directories `<root_dir>/include` AND `<build_dir>/include`. For the linker, include the static library `<build_dir>/libteseo.a` and its dependencies libevent and libnuma. For instance, a sample `Makefile` for a custom `hello_world.cpp` project with GCC could look like this:

```
all: hello_world

# Executable
hello_world: hello_world.o
	g++ -std=c++17 $^ -L/path/to/teseo/build -lteseo -lnuma -levent_pthreads -levent_core -lpthread -o $@

# Compiled source
hello_world.o: hello_world.cpp
	g++ -std=c++17 -c $^ -I/path/to/teseo/include -I/path/to/teseo/build/include -o $@ 
```

Assuming that the library was built in the directory `/path/to/teseo/build`.

#### Tests

To be able to run the unit tests, the configure script must be first invoked with the option `--enable-test`. 
Afterward, the test-suite can be created with `make testsuite`. The complete snippet of commands is:
 
```
# starting from the root directory of this repository
./autoreconf -iv # if this has not been issued before
mkdir build_unit_testing && cd build_unit_testing
../configure --enable-test
./make testsuite -j
./testsuite
```

The whole test suite should complete in ~30 minutes, using as reference an AMD 3950X with 16 cores. 

To selectively execute only a specific test set, use `./testsuite <testname>`. The list of available tests can be retrieved with `./testsuite -l`. The library used for unit testing is [Catch](https://github.com/catchorg/Catch2), refer to its documentation for the other command line options.


### Usage

In your application, include the header [teseo.hpp](https://github.com/cwida/teseo/blob/master/include/teseo.hpp), which acts as interface to the library and all exposed methods, part of the API, are documented. 
The library only supports undirected weighted graphs. Vertices are identified by 64-bit integers in \[0, 2^64 -2\]. Edges must be simple, without self-loops. There cannot be multiple edges between the same vertices. _Hyperedges_ , edges connecting more than two vertices, are, as you have already likely imagined, not supported. Weights must be 64-bit floating point numbers, that is `double`s. These settings are sufficient to implement the kernels of the [LDBC Graphalytics benchmark](https://graphalytics.org/). 

#### Basic interface

The first step is to create an in-memory database. This is achieved by `teseo::Teseo database`, there is only one empty constructor. Databases cannot be serialized or saved to disk, when the application is restarted you need to initialise a new database.

Modifying the content of the database can only be achieved through a `teseo::Transaction` instance. There are two types of transactions: read-write (default) and read-only. The difference is that with a read-only transaction, while the underlying graph cannot be modified, it is generally faster to examine its content. The major penalty of read-write transactions is with scans involving logical vertex identifiers (see below). A transaction instance is created through the method `Teseo::start_transaction(bool read_only = false)`.

To change the content of the graph, the instance methods of a `Transaction` object are `insert_vertex(uint64_t vertex_id)`, `remove_vertex(uint64_t vertex_id)`, `insert_edge(uint64_t source, uint64_t destination, double weight)` and `remove_edge(uint64_t source, uint64_t destination)`. These methods  return nothing, but `remove_vertex`, which implicitly removes all edges attached to the vertex being deleted and the method returns
the number of removed edges. As graphs are undirected, it is equivalent to refer to an edge as a -> b and b -> a, that is, `insert_edge(a, b, w)` and `insert_edge(b, a, w)` have the same outcome.

Changes performed by a read-write transaction `t` are not saved in the database and will not be visible by later transactions until `t.commit()` is invoked. Pending changes can also be discarded by invoking `t.roll_back()`. Once one of these two methods is called, the transaction instance is _consumed_ and cannot be further
utilised to modify or inspect the content of the graph.  A read-only transaction can be terminated by both `commit()` and `roll_back()`, the final effect is the same, but it is still important to terminate it to release the acquired resources. When a transaction object goes out of scope, if it has not been explicitly terminated, the system automatically issues a `roll_back()`.

The following snippet illustrates the methods mentioned to create a graph, insert and remove a few vertices and edges:

```
#include "teseo.hpp"

int main(int argc, const char* argv[]){
    teseo::Teseo database;

    auto tx1 = database.start_transaction();

    // Create the vertices 10, 20, 30, 40
    tx1.insert_vertex(10);
    tx1.insert_vertex(20);
    tx1.insert_vertex(30);
    tx1.insert_vertex(40);

    // Create the edges 10 - 20, 10 - 30 and 10 - 40
    tx1.insert_edge(10, 20, /* weight */ 1.0);
    tx1.insert_edge(10, 30, /* weight */ 1.5);
    tx1.insert_edge(10, 40, /* weight */ 2.5);

    // Remove an edge 10 - 40
    // As the graph is undirected, an edge can be equivalently addressed as 10 - 40 and 40 - 10.
    tx1.remove_edge(40, 10);

    // Remove a vertex
    tx1.remove_vertex(20);

    // Save the changes
    tx1.commit();
    
    return 0;
}

```

#### Exploring the graph

Differently from the other systems compared in the paper, Teseo stores the vertices internally using the identifiers set by the user, these are called  _user identifiers_  or  _real identifiers_  . The alternative approach, used by the other systems and named  _logical identifiers_ , is to preemptively translate the vertices into a logical dense domain, in `[0, N)`, where `N` are the total number of vertices. In Teseo, vertices can be retrieved using both the  _user identifiers_  and the  _logical identifiers_ . 

The following snippet shows how to find all vertices in the graph:

```
auto tx = database.start_transaction(/* read only ? */ true);
cout << "Number of vertices: " << tx.num_vertices() << "\n";
for(uint64_t i = 0; i < tx.num_vertices(); i++){
    uint64_t vertex_id = tx.vertex_id(i);
    cout << "[" << i << "] vertex: " << vertex_id << "\n";
}
tx.commit();
```

The method `uint64_t vertex_id(uint64_t logical_id)` translates a logical identifier into its associated user identifier. The opposite translation is given by the method `uint64_t logical_id(uint64_t vertex_id)`, which translates a user identifier into its logical association.

The method `uint64_t degree(uint64_t id, bool logical = false)` returns the number of edges attached to a given vertex. The flag `logical` determines whether the given vertex identifier is logical (`logical = true`) or real (`logical = false`).

The edges attached to a vertex can be accessed through an iterator, created with the method `Transaction::iterator()`. Rather than continuously invoking a function such as `next()` or `*iterator`, in Teseo, an iterator takes as input a  _callback_  or a lambda function, which is invoked every time an edge is fetched from the storage. The callback must have one of the following signatures:

1. `bool (*) (uint64_t destination, double weight)`: receive both the destination and the weight of an edge, use the return value to stop the iterator;
2. `bool (*) (uint64_t destination)`: receive only the destination of an edge, use the return value to stop the iterator;
3. `void (*) (uint64_t destination, double weight)`: receive both the destination and the weight of an edge, the iterator will fetch all edges associated to the vertex;
4. `void (*) (uint64_t destination)`: receive only the destination of an edge, the iterator will fetch all edges associated to the vertex.

The return value of the callback can be used to stop the iterator (`return false`) before exhausting all edges associated to a vertex. A callback is registered with the method `Iterator::edges(uint64_t vertex, bool logical, Callback&& cb)`.

Note that, before terminating a transaction, whether read-write or read-only, all created iterators must be first either explicitly closed, by invoking the method `Iterator::close()`, or implicitly closed, by leaving the variable go out of scope and let the destructor close the iterator.

The following example implements a sequential BFS starting from the vertex 10:

```
uint64_t root = 10; // source of the BFS

auto tx = database.start_transaction(/* read only ? */ true);
auto iter = tx.iterator();

// initialise the distances
struct Entry {
    uint64_t m_parent; // parent node in the shortest path
    uint64_t m_distance; // distance from the root
};
std::unordered_map<uint64_t, Entry> distances;
distances.reserve(tx.num_vertices());
distances[root] = Entry{0, 0};

// initialise the FIFO queue
std::queue<uint64_t> Q;
Q.push(root);

while(!Q.empty()){ // bulk of the BFS
    // extract the next element from the queue
    uint64_t source = Q.front();
    Q.pop();

    uint64_t d = distances[source].m_distance;

    iter.edges(source, /* logical ? */ false, [&](uint64_t destination){
        // insert the distance of destination if it was not already registered
        auto res = distances.insert(std::make_pair(destination, Entry{source, d+1}));
        if (res.second) { // check whether the insertion in the hash table succeeded
            Q.push(destination);
        }
    });
}

// release the iterator and the transaction
iter.close();
tx.commit();
```

A more optimised version of the BFS (and other kernels) is implemented in the [GFE driver](https://github.com/cwida/gfe_driver/blob/master/library/teseo/teseo_driver.cpp).

#### Concurrency

Teseo is meant to be thread-safe and scalable, but it does require some discipline in the usage of the API.

Application threads, besides the thread that creates the database, must be registered with `Teseo::register_thread()` ahead of operating on transactions. This is because
Teseo employs a custom epoch-based non-blocking Garbage Collector (GC) together with optimistic latches (ref. [paper](http://vldb.org/pvldb/vol14/p1053-leo.pdf)). Before released data structures can be deallocated from memory, the GC must keep track of what each thread can access, and for that, it must know all available threads in the first place. Similarly, before a thread terminates, a call to `Teseo::unregister_thread()` must be executed. Avoiding this will cause the database to hang when closing/destroying it, as Teseo will expect that some other thread is still operating on the database itself. Calls to `register_thread` and `unregister_thread` are implicit on the same thread that creates the database and, therefore, can be avoided. Finally, it's always safe to invoke `register_thread` multiple times without pairing it with `unregister_thread` on the same thread, the only strict requirement is that a call to `unregister_thread` eventually follows before the thread terminates.

`Transaction` objects are thread-safe, for both read-only and read-write transactions, as far as the copy constructor/assignment is used to pass the same instance around different threads. A `Transaction` object is already internally a custom smart pointer, which relies on reference counting to know when a transaction becomes inaccessible even though it was not explicitly committed or aborted. Furthermore, there is an internal optimization that, if the reference count never increases 1, an internal latch for the transaction state is elided.

Multiple transactions can operate on the same database concurrently, both from the same thread, interleaved, or from different threads. When attempting to perform a write operation, a transaction can fail due to a conflict with another transaction. This can occur when altering a  _locked_  data item, Teseo relies on pessimistic locking,
or modifying a data item that has already been changed by a newer, more recent, transaction. The API will raise a `TransactionConflict` exception in case a conflict is detected. A common strategy to work around this issue is to restart the transaction and try again:

```
// Insert vertex 42
bool done = false;
do {
    try {
        teseo::Transaction tx = database.start_transaction();
        tx.insert_vertex(42); // this could fail with a TransactionConflict
        tx.commit();
        done = true;
    } catch(teseo::TransactionConflict&){
        // nop, try again..
    }
} while(!done);
```


As usual for snapshot isolation, read operations never fail due to transaction conflicts. 

Iterators/cursors, created by `Transaction::iterator()`, are not thread-safe. Rather, the related `Transaction` instance needs to be shared among different threads and, from each thread, a separate iterator can be initialised. Note that, a transaction cannot be terminated, by either `commit` or `rollback`, if all the created iterators are not first closed, implicitly by their destructor, or explicitly by invoking `Iterator::close()`.

#### OpenMP

OpenMP can be used to implement graph algorithms for Teseo, but it demands even further care. OpenMP threads must also be registered/unregistered when used with Teseo, before any of the functions from the Teseo API can be called. Similarly, as described in the previous section, transaction objects must be copied in each thread and a new iterator should be created from each thread local copy. A solution is to rely on the OpenMP [firstprivate](https://www.openmp.org/spec-html/5.1/openmpsu116.html#x151-1690002.21.4.4) clause to implement a RAII object to register/unregister a thread with Teseo:

```
#include <omp.h> # omp_get_thread_num

// RAII object to register/unregister an OpenMP thread
class RegisterThread {
    teseo::Teseo& m_database;

public:
    // Constructor from the master thread
    RegisterThread(teseo::Teseo& database) : m_database(database){ }

    // Copy constructor, from the worker threads
    RegisterThread(const RegisterThread& rt) : m_database(rt.m_database) {
        if(omp_get_thread_num() > 0){
            m_database.register_thread();
        }
    }

    // Destructor
    ~RegisterThread(){
        if(omp_get_thread_num() > 0){
            m_database.unregister_thread();
        }
    }
};
```


The main thread will be always identified with the number 0 and there is no need to register/unregister it. A similar approach can be used to copy the transaction object and create a local iterator. The following snippet shows a possible BFS implementation with OpenMP. A more optimised version is present in the [GFE driver](https://github.com/cwida/gfe_driver/blob/master/library/teseo/teseo_driver.cpp).

```
#include <atomic>
#include <cstdint>
#include <iostream>
#include <omp.h>

#include "teseo.hpp"

using namespace std;

// RAII object to register/unregister an OpenMP thread
class RegisterThread {
    teseo::Teseo& m_database;

public:
    // Constructor from the master thread
    RegisterThread(teseo::Teseo& database) : m_database(database){ }

    // Copy constructor, from the worker threads
    RegisterThread(const RegisterThread& rt) : m_database(rt.m_database) {
        if(omp_get_thread_num() > 0){
            m_database.register_thread();
        }
    }

    // Destructor
    ~RegisterThread(){
        if(omp_get_thread_num() > 0){
            m_database.unregister_thread();
        }
    }
};

// OpenMP state
class OpenMP {
    RegisterThread m_registrator;
    teseo::Transaction m_transaction;
    teseo::Iterator m_iterator;

public:
    // Constructor from the master thread
    OpenMP(teseo::Teseo& database) : m_registrator(database), m_transaction(database.start_transaction(/* read only ? */ true)), m_iterator(m_transaction.iterator()) {

    }

    // Copy constructor for the worker threads
    OpenMP(const OpenMP& original): m_registrator(original.m_registrator), m_transaction(original.m_transaction), m_iterator(m_transaction.iterator()){

    }

    // Accessors
    teseo::Transaction& transaction() { return m_transaction; }
    teseo::Iterator& iterator(){ return m_iterator; }
};

// Perform a BFS starting from the vertex "root"
void parallel_bfs(teseo::Teseo& database, uint64_t root){
    OpenMP openmp(database); // init the OpenMP state
    auto tx = openmp.transaction();

    // initialize the distances & parents
    int32_t distances[tx.num_vertices()]; // O(N) space
    uint64_t parents[tx.num_vertices()]; // O(N) space
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        distances[i] = -1; // -1 => node unreachable
    }

    // initialise the frontier
    unique_ptr<uint64_t[]> mem0 { new uint64_t[tx.num_vertices()] }; // avoid leaks in case of exceptions
    unique_ptr<uint64_t[]> mem1 { new uint64_t[tx.num_vertices()] }; // as above
    uint64_t* current = mem0.get(); // nodes to examine at the current iteration
    uint64_t* next = mem1.get(); // nodes to examine at the next iteration
    uint64_t current_sz = 0; // number of nodes in `current`
    atomic<uint64_t> next_sz = 0; // number of nodes in `next`

    // first iteration
    current[0] = tx.logical_id(root);
    parents[tx.logical_id(root)] = tx.logical_id(root);
    distances[tx.logical_id(root)] = 0;
    current_sz = 1;

    while(current_sz > 0){ // are there nodes in the frontier?
        // examine in parallel all nodes in the frontier
        #pragma omp parallel for firstprivate(openmp)
        for(uint64_t i = 0; i < current_sz; i++){
            auto tx = openmp.transaction();
            auto iterator = openmp.iterator();

            uint64_t source = current[i];
            int32_t distance = distances[source];

            // visit the neighbours of source
            iterator.edges(source, /* logical ? */ true, [&, source, distance](uint64_t destination){
                int32_t val = distances[destination];
                if (val == -1 || (distance +1 < val)){ // check
                    int32_t new_val = distance +1;
                    if(__atomic_compare_exchange_n(distances + destination, &val, new_val, /* ignore the rest of params */ false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)){ // update
                        parents[destination] = source;
                        next[next_sz++] = destination; // examine destination at the next step
                    }
                }
            });
        }


        // next iteration
        std::swap(current, next);
        current_sz = next_sz;
        next_sz = 0;
    }

    // print the results
    for(uint64_t i = 0; i < tx.num_vertices(); i++){
        cout << "[" << i << "] vertex: " << tx.vertex_id(i) << ", ";
        if(distances[i] == -1){
            cout << "unreachable\n";
        } else {
            cout << "distance: " << distances[i] << ", parent: " << tx.vertex_id(parents[i]) << "\n";
        }
    }
}
```

#### Dump

For debug reasons, it can be useful to dump the content of the fat tree to the console (stdout). The fat tree is the main data structure used to store the graph and it is described in the [paper](http://vldb.org/pvldb/vol14/p1053-leo.pdf). It consists of a large B^+tree, with leaves of a few megabytes, indexed by an [ART trie](https://db.in.tum.de/~leis/papers/ART.pdf). A dump can be performed by invoking `teseo::context::global_context()->memstore()->dump()`, the header `teseo/memstore/memstore.hpp` must be first included in the source file. 

For instance, immediately dumping the graph created with the snippet of the section  __Basic interface__  would lead to the following output, assuming that the build has been generated through `./configure --enable-test` (without `--enable-test` the size of the fat tree is significantly larger) :

```
# Before GC

[Memstore] directed: false, max num segments per leaf: 4, segment size: 34 qwords
Index: 
 Node: 0x55f65c171010, key level: 0, type: N256 (3)
 Prefix, length: 0
 Children: 1, {byte:0, pointer:0x800055f65c172de0}
    Leaf: 0x800055f65c172de0, key: 0 -> 0, value: leaf: 0x55f65c171830, segment_id: 0

Leaves: 
[LEAF] 0x55f65c171830, num segments: 4, fence keys: [0 -> 0, 18446744073709551615 -> 18446744073709551615), rebalancer active: false, reference count: 1
  +-- [SEGMENT #0] 0x55f65c171880, state: FREE, latch: {version: 14}, rebalance requested, used space: 24 qwords, fence keys = [0 -> 0, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c1719c0, versions1: 14, empty1: 24, empty2: 31, versions2: 31, free space: 7 qwords, used space: 24 qwords, pivot: 18446744073709551615 -> 18446744073709551615
    Left hand side: 
      [0] Vertex 11 [first], edge count: 3, [version present] type: insert, back pointer: 0, chain length: 1, undo pointer: 0x55f65c174258
          0. 0x55f65c174258, version (+inf, 1], update: {Remove vertex 11}, next: 0
      [1] Edge 11 -> 21, weight: 0, [version present] type: remove, back pointer: 1, chain length: 2, undo pointer: 0x55f65c174f70
          0. 0x55f65c174f70, version (+inf, 1], update: {Insert edge 11 -> 21 (weight: 1)}, next: 0x55f65c1751f0
          1. 0x55f65c1751f0, version (1, 1], update: {Remove edge 11 -> 21 (weight: 0)}, next: 0
      [2] Edge 11 -> 31, weight: 1.5, [version present] type: insert, back pointer: 2, chain length: 1, undo pointer: 0x55f65c175170
          0. 0x55f65c175170, version (+inf, 1], update: {Remove edge 11 -> 31 (weight: 0)}, next: 0
      [3] Edge 11 -> 41, weight: 0, [version present] type: remove, back pointer: 3, chain length: 2, undo pointer: 0x55f65c175030
          0. 0x55f65c175030, version (+inf, 1], update: {Insert edge 11 -> 41 (weight: 2.5)}, next: 0x55f65c1750f0
          1. 0x55f65c1750f0, version (1, 1], update: {Remove edge 11 -> 41 (weight: 0)}, next: 0
      [4] Vertex 21 [first], edge count: 1, [version present] type: remove, back pointer: 4, chain length: 2, undo pointer: 0x55f65c174ff0
          0. 0x55f65c174ff0, version (+inf, 1], update: {Insert vertex 21}, next: 0x55f65c174218
          1. 0x55f65c174218, version (1, 1], update: {Remove vertex 21}, next: 0
      [5] Edge 21 -> 11, weight: 1, [version present] type: remove, back pointer: 5, chain length: 2, undo pointer: 0x55f65c174fb0
          0. 0x55f65c174fb0, version (+inf, 1], update: {Insert edge 21 -> 11 (weight: 1)}, next: 0x55f65c1751b0
          1. 0x55f65c1751b0, version (1, 1], update: {Remove edge 21 -> 11 (weight: 0)}, next: 0
      [6] Vertex 31 [first], edge count: 1, [version present] type: insert, back pointer: 6, chain length: 1, undo pointer: 0x55f65c175270
          0. 0x55f65c175270, version (+inf, 1], update: {Remove vertex 31}, next: 0
      [7] Edge 31 -> 11, weight: 1.5, [version present] type: insert, back pointer: 7, chain length: 1, undo pointer: 0x55f65c175130
          0. 0x55f65c175130, version (+inf, 1], update: {Remove edge 31 -> 11 (weight: 0)}, next: 0
      [8] Vertex 41 [first], edge count: 1, [version present] type: insert, back pointer: 8, chain length: 1, undo pointer: 0x55f65c175230
          0. 0x55f65c175230, version (+inf, 1], update: {Remove vertex 41}, next: 0
      [9] Edge 41 -> 11, weight: 0, [version present] type: remove, back pointer: 9, chain length: 2, undo pointer: 0x55f65c175070
          0. 0x55f65c175070, version (+inf, 1], update: {Insert edge 41 -> 11 (weight: 2.5)}, next: 0x55f65c1750b0
          1. 0x55f65c1750b0, version (1, 1], update: {Remove edge 41 -> 11 (weight: 0)}, next: 0
    Right hand side: empty
  +-- [SEGMENT #1] 0x55f65c1718d0, state: FREE, latch: {version: 0}, used space: 0 qwords, fence keys = [18446744073709551615 -> 18446744073709551615, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c171ad0, versions1: 0, empty1: 0, empty2: 31, versions2: 31, free space: 31 qwords, used space: 0 qwords, pivot: 18446744073709551615 -> 18446744073709551615, empty
  +-- [SEGMENT #2] 0x55f65c171920, state: FREE, latch: {version: 0}, used space: 0 qwords, fence keys = [18446744073709551615 -> 18446744073709551615, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c171be0, versions1: 0, empty1: 0, empty2: 31, versions2: 31, free space: 31 qwords, used space: 0 qwords, pivot: 18446744073709551615 -> 18446744073709551615, empty
  +-- [SEGMENT #3] 0x55f65c171970, state: FREE, latch: {version: 0}, used space: 0 qwords, fence keys = [18446744073709551615 -> 18446744073709551615, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c171cf0, versions1: 0, empty1: 0, empty2: 31, versions2: 31, free space: 31 qwords, used space: 0 qwords, pivot: 18446744073709551615 -> 18446744073709551615, empty
Number of visited leaves: 1

```

The fat tree contains multiple versions of the same data item, that is, the empty graph without elements and the elements created by `tx1`. The Garbage Collector (GC) automatically removes all inaccessible versions after some time and the elements could be moved to different segments\leaves for balancing reasons. For instance, by executing a new dump of the fat tree after one second leads to the following output:

```
# After GC

[Memstore] directed: false, max num segments per leaf: 4, segment size: 34 qwords
Index: 
 Node: 0x55f65c171010, key level: 0, type: N256 (3)
 Prefix, length: 0
 Children: 1, {byte:0, pointer:0x7f9d1000d580}
    Node: 0x7f9d1000d580, key level: 1, type: N4 (0)
    Prefix, length: 6, 0: 0x0, 1: 0x0, 2: 0x0, 3: 0x0, 4: 0x0, 5: 0x0
    Children: 2, {byte:0, pointer:0x800055f65c172de0}, {byte:31, pointer:0x80007f9d1000d560}
        Leaf: 0x800055f65c172de0, key: 0 -> 0, value: leaf: 0x55f65c171830, segment_id: 0
        Leaf: 0x80007f9d1000d560, key: 31 -> 0, value: leaf: 0x55f65c171830, segment_id: 1

Leaves: 
[LEAF] 0x55f65c171830, num segments: 4, fence keys: [0 -> 0, 18446744073709551615 -> 18446744073709551615), rebalancer active: false, reference count: 1
  +-- [SEGMENT #0] 0x55f65c171880, state: FREE, latch: {version: 15}, used space: 3 qwords, fence keys = [0 -> 0, 31 -> 0) 
    Sparse File @ 0x55f65c1719c0, versions1: 3, empty1: 3, empty2: 31, versions2: 31, free space: 28 qwords, used space: 3 qwords, pivot: 18446744073709551615 -> 18446744073709551615
    Left hand side: 
      [0] Vertex 11 [first], edge count: 1
      [1] Edge 11 -> 31, weight: 1.5
    Right hand side: empty
  +-- [SEGMENT #1] 0x55f65c1718d0, state: FREE, latch: {version: 1}, used space: 5 qwords, fence keys = [31 -> 0, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c171ad0, versions1: 3, empty1: 3, empty2: 29, versions2: 29, free space: 26 qwords, used space: 5 qwords, pivot: 41 -> 0
    Left hand side: 
      [0] Vertex 31 [first], edge count: 1
      [1] Edge 31 -> 11, weight: 1.5
    Right hand side: 
      [0] Vertex 41 [first], edge count: 0
  +-- [SEGMENT #2] 0x55f65c171920, state: FREE, latch: {version: 0}, used space: 0 qwords, fence keys = [18446744073709551615 -> 18446744073709551615, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c171be0, versions1: 0, empty1: 0, empty2: 31, versions2: 31, free space: 31 qwords, used space: 0 qwords, pivot: 18446744073709551615 -> 18446744073709551615, empty
  +-- [SEGMENT #3] 0x55f65c171970, state: FREE, latch: {version: 0}, used space: 0 qwords, fence keys = [18446744073709551615 -> 18446744073709551615, 18446744073709551615 -> 18446744073709551615) 
    Sparse File @ 0x55f65c171cf0, versions1: 0, empty1: 0, empty2: 31, versions2: 31, free space: 31 qwords, used space: 0 qwords, pivot: 18446744073709551615 -> 18446744073709551615, empty
Number of visited leaves: 1
```

The first line of the dump shows some basic properties of the fat tree, internally named _Memstore_: 

* directed: it refers to whether the graph is directed or undirected and it is always false.
* max num segments per leaf: a leaf can contain a variable number of segments, between the given limit and half of it.
* segment size: amount of space in a segment, in qwords (quadwords). A quadword is 8 bytes.

The second part of the dump is the ART trie. After the GC executed, the content of the graph is stored in two segments indexed by two associated search keys ("leaves") in the trie. The trie contains two levels, the root, with type N256, and its direct child, with type N4. Refer to the [ART paper](http://vldb.org/pvldb/vol14/p1053-leo.pdf) for the meaning of these node types. The property `key level` is the byte position to which a node of the trie refers to, while the prefix are the bytes shared by all descendants of that node. The line `Leaf: 0x80007f9d1000d560, key: 31 -> 0, value: leaf: 0x55f65c171830, segment_id: 1` represents a search key for the vertex 30, allocated in an element of the trie at address `0x80007f9d1000d560`, the value associated to the search key is the leaf of the fat tree at address `0x55f65c171830` and segment 1. All vertices are internally shifted by 1, thus the value 31 represents the vertex 30. In the trie, both vertices and edges are implemented as a fixed-size 16-byte pair <source, destination>. For a vertex, the destination is always 0 (this is the reason elements are shifted by +1 internally).

The second part of the dump contains the content of the leaves of the fat tree. In this case, there is only one leaf. In the first dump, all elements are stored in the first segment, whereas in the second dump, after both a rebalance and a pass of the GC took place, the elements are split in the first two segments. The header of a segment shows its memory address, the state of the latch (FREE/READ/WRITE/REBAL) and its version, used by optimistic readers, the amount of space consumed, in quadwords, and the minimum fence key. A segment can either be a  _Sparse File_ , named Read-Optimised Segment (ROS) in the paper, or a  _Dense File_ , named Write-Optimised Segment (WOS) in the paper, again refer to the paper for their description. In a Sparse File/ROS, the content of the segment can be placed at both the left- and the right-hand side, while the middle is unused space, available for new insertions. The header of a Sparse File/ROS contains the properties `versions1`, `empty1`, `empty2`, `versions2`, which define the start and end position of the left- and right-hand side of the segment and the presence of versions for the data items. The property `pivot` is simply the first element in the right-hand side of the Sparse File/ROS. The rest of the lines in the dump display the elements stored in the segments. For instance, considering the following entry in the first dump:

```
[1] Edge 11 -> 21, weight: 0, [version present] type: remove, back pointer: 1, chain length: 2, undo pointer: 0x55f65c174f70
          0. 0x55f65c174f70, version (+inf, 1], update: {Insert edge 11 -> 21 (weight: 1)}, next: 0x55f65c1751f0
          1. 0x55f65c1751f0, version (1, 1], update: {Remove edge 11 -> 21 (weight: 0)}, next: 0
```

This is the edge 10 -> 20 implicitly deleted when the vertex 20 was removed by issuing `tx1.remove_vertex(20)` and it is valid for all transactions whose start_time is >= 1. The back pointer refers to its position in the version area of the segment. There are two other versions (`chain length: 2`) of this data item, logically stored in a linked list. The first entry is `{Insert edge 11 -> 21 (weight: 1)}`, valid for the transactions with start_time [1, 1), therefore, this version is never accessible. The following entry is `{Remove edge 11 -> 21 (weight: 0)}`, which implies that the edge 10 -> 20 does not exist and it is valid for the transactions with start_time in [0, 1). 
