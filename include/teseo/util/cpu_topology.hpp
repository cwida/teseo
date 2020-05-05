/*
 * cpu_topology.hpp
 *
 *  Created on: 27 Sep 2012
 *      Author: Dean De Leo (hello@whatsthecraic.net)
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
 * along with the program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <stdexcept> // runtime_error
#include <string>
#include <vector>

namespace teseo::util {

// classes defined in this header
class cpu_topology;

class cpu_topology_exception;

/**
 * Retrieves an instance of the cpu_topology;
 */
cpu_topology &get_cpu_topology();

/**
 * A class to retrieve information regarding the cpu topology and the cache hierarchy for
 * the underlying platform.
 *
 * Dublin, 04/ May /2014
 * After two years of when I wrote the class, this is my understanding of an exemplifying
 * internal representation:
 *
 *  nodes ----->                    node_t: id = 0                    ... node_t: id = 1 ...
 *                                 /              \
 *                          core_t: id = 0   core_t: id = 1
 *                                |                |
 *                        thread_t: id = 0    thread_t: id = 1
 *                         /            \     /             \
 *  caches ------>     cache_t          cache_t           cache_t      ... other caches ....
 *                    level: L1        level: L2         level: L1
 *
 *
 * nodes: this is a list of the nodes atop of the hierarchy.
 * caches: a list of  all  registered cache nodes, useful to deallocate the caches in the dtor
 *      avoiding the reference counting.
 * threads: an array that works as index, given the thread id, it returns the related node. It is
 *      expected by os_init() to init this array as the common init does not know how many physical
 *      threads are available in the underlying architecture.
 *
 * Terminology:
 * node: this is a (potentially NUMA) node of the underlying architecture.
 * core: this is a physical core, a group of physical processors in the same unit.
 * thread: a physical thread or virtual processor. It does make sense only for architectures that make
 *      use of simultaneous multi-threading (SMT) such as the Intel HyperThreading. Otherwise the rela-
 *      tionship between a thread and a core is 1:1.
 *
 * Well, I recognise this is not easiest nor most convenient way to represent such information, but
 * here we are stuck ;-)
 *
 * A point regarding the virtual methods: I guess initially I thought it was a good idea to have an
 * abstract class or interface and then provide an implementation for each operating system where
 * the class was meant to be ported. However eventually I am finding easier to combine the implementa~
 * tion for the different platforms in the same class, so I guess the rationale behind the virtual
 * methods is not anymore valid.
 */
class cpu_topology {
protected:
    // defined classes
    struct t_node;
    struct t_core;
    struct t_thread;
    struct t_cache;

    // iterators
    typedef std::vector<t_node *>::iterator t_node_it;
    typedef std::vector<t_core *>::iterator t_core_it;
    typedef std::vector<t_thread *>::iterator t_thread_it;
    typedef std::vector<t_cache *>::iterator t_cache_it;
    typedef std::vector<t_node *>::const_iterator t_node_const_it;
    typedef std::vector<t_core *>::const_iterator t_core_const_it;
    typedef std::vector<t_thread *>::const_iterator t_thread_const_it;
    typedef std::vector<t_cache *>::const_iterator t_cache_const_it;

    // bitset for shared_cpu_map
    typedef std::vector<int32_t> shared_cpu_map_t;

    struct t_node {
        int id;
        std::vector<t_core *> cores;
    };

    struct t_core {
        int id;
        t_node *node;
        std::vector<t_thread *> threads;
    };

    struct t_thread {
        int id;
        t_core *owner;
        std::vector<t_cache *> caches;
    };

    struct t_cache {
        shared_cpu_map_t shared_cpu_map;
        int level;
        std::size_t size;
        std::size_t line;
        std::vector<t_thread *> threads;
    };

    // sorting functions
    static int compare_nodes(t_node *n0, t_node *n1);

    static int compare_cores(t_core *c0, t_core *c1);

    static int compare_threads(t_thread *t0, t_thread *t1);

    /**
     * Create the topology for this system
     */
    void init();

    virtual void os_init(); // operating system dependent

    /**
     * Retrieves the cpu id for the thread currently in execution
     */
    int get_cpuid() const { return os_get_cpuid(); }; // alias
    virtual int os_get_cpuid() const; // operating system dependent


    /**
     * If not already present, insert a triple <node_id, core_id, thread_id> onto the topology tree.
     * @return the created t_thread data structure
     */
    t_thread *topo_add(int node_id, int core_id, int thread_id);

    /**
     * Retrieves the record for the specified cpu_id
     */
    t_thread *topo_cpu_get(int cpu_id) const;


    /**
     * Find the given node in the set of the registered nodes
     */
    t_node *topo_node_get(int node_id) const;


    /**
     * Delete the created topology, releasing involved resources
     */
    void topo_delete();


    /**
     * Search for the specified cache into the cache list
     */
    t_cache *cache_find(const shared_cpu_map_t &shared_cpu_map, int level);

    /**
     * Adds a cache into the list, if not already present
     */
    t_cache *cache_add(shared_cpu_map_t &shared_cpu_map, int level, std::size_t size, std::size_t coherency_line_size);

    /**
     * Relates a cache to a cpu and viceversa
     */
    void cache_link(t_cache *ca, t_thread *cpu);

    /**
     * Delete the information saved for the caches
     */
    void cache_delete();


    std::vector<t_node *> nodes;
    std::vector<t_cache *> caches;
    t_thread **threads; //indexer for threads
    std::size_t threads_size;
public:

    /**
     * Creates a new instance of the cpu_topology. The information from the platform and
     * operating system are retrieved during the construction of the object and cached
     * to fulfill the following requests.
     */
    cpu_topology();

    /**
     * Destructor, it does what you would expect
     */
    virtual ~cpu_topology();

    /**
     * Retrieves the cache line size for the given cpu and level
     * @param cpu the id for the virtual processor (or physical thread)
     * @param level the cache level
     * @return the size of a line for the given cache, in bytes
     */
    virtual std::size_t get_cache_line(int cpu, int level) const;

    /**
     * Retrieves the cache size for the given cpu and level
     * @param cpu the id for the virtual processor (or physical thread)
     * @param level the cache level
     * @return the size of the given cache, in bytes
     */
    virtual std::size_t get_cache_size(int cpu, int level) const;

    /**
     * Get the line size for the cache L1 for the current thread
     * @return the line size of the cache L1, in bytes
     */
    virtual std::size_t get_firstcache_line() const;

    /**
    * Get the size of the cache L1 for the current thread
    * @return the size of the cache L1, in bytes
    */
    virtual std::size_t get_firstcache_size() const;

    /**
     * Get the line size for the last level (usually L2 or L3) in the cache hierarchy
     * for the current thread
     * @return the line size of the cache in the last level of the hierarchy, in bytes
     */
    virtual std::size_t get_lastcache_line() const;

    /**
     * Get the size of the cache in the last level (usually L2 or L3) of the cache hierarchy
     * for the current thread
     * @return the size of the cache in the last level of the hierarchy, in bytes
     */
    virtual std::size_t get_lastcache_size() const;

    /**
     * Get the number of levels in the cache hierarchy for the given cpu
     * @param cpu [optional]: the cpu to take into account, or if not specified for the cpu
     * executing the current thread
     * @return the number of levels in the hierarchy
     */
    virtual std::size_t get_cache_levels(int cpu = -1) const;

    /**
     * Retrieves the node ID for the cpu executing the current thread
     * @return the node ID for the current thread
     */
    virtual int get_node() const;

    /**
     * Retrieves the node ID for the given cpu id
     * @return the node ID for the given cpu
     */
    virtual int get_node(int cpu) const;


    /**
     * Retrieves the list of nodes in the current platform
     * @param result [out]: a vector to hold the list of nodes for the current platform
     */
    virtual void get_nodes(std::vector<int> &result) const;

    /**
     * Retrieves the number of nodes in the current platform
     * @return the number of nodes in the current platform
     */
    virtual std::size_t get_node_count() const;

    /**
    * Get the list of physical threads for the given NUMA node
    * @param node_id the node to examine
    * @param include_smt whether to include multiple physical threads sharing the same core
    */
    virtual std::vector<int> get_threads_on_node(int node_id, bool include_smt) const;

    /**
    * Get the list of all physical threads in the system
    * @param interleaved true: report the list of threads interleaved one by one for each NUMA node, or
    *                    false: report the list by sequence: first all threads of socket 0, then all threads
    *                    of socket 1, etc..
    * @param include_smt whether to include multiple physical physical threads sharing the same core
    */
    virtual std::vector<int> get_threads(bool interleaved, bool include_smt) const;

    /**
     * Retrieves the list of physical threads that share the same cache level <L> with the thread <T>
     * @param thread_id the id of the physical thread <T> to consider
     * @param cache_level the cache level <L>
     * @param result [out]: a vector to hold the final list of threads
     */
    virtual void get_sharing_threads(int thread_id, int cache_level, std::vector<int> &result) const;

    /**
     * Verifies if the given physical thread shares the requested cache level with any other thread
     * in the list of candidates
     * @param thread_id the id of the physical thread to consider
     * @param cache_level the cache level to take into account
     * @param candidates the list of the other threads to check
     */
    virtual bool share_cache_with_any(int thread_id, int cache_level, std::vector<int> &candidates) const;

    /**
     * Prints into the stdout the current topology
     */
    void topo_show() const;
};

/**
 * Topology related exceptions
 */
class cpu_topology_exception : public std::runtime_error {
private:
    std::string message;

public:
    cpu_topology_exception(const std::string &msg) throw();

    ~cpu_topology_exception() throw() {}

    const char *what() const throw(); // contract - it overloads the method from class exception
};

} // namespace
