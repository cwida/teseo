/*
 * cpu_topology.cpp
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
#include "teseo/util/cpu_topology.hpp"

#include <algorithm> // sort
#include <cassert>
#include <iostream>
#include <sstream> // conversion
#include <string>
#include <vector>

using std::cout;
using std::endl;
using std::string;

namespace teseo::util {

/*************************************************************************************************
 *                                                                                               *
 *   SINGLETON INTERFACE                                                                         *
 *                                                                                               *
 *************************************************************************************************/
static cpu_topology *cpu_topology_instance(NULL);

cpu_topology &get_cpu_topology() {
    if (!cpu_topology_instance) cpu_topology_instance = new cpu_topology();
    return *cpu_topology_instance;
}

/*************************************************************************************************
 *                                                                                               *
 *   INITIALISER                                                                                 *
 *                                                                                               *
 *************************************************************************************************/

cpu_topology::cpu_topology() {
    threads = NULL;
    threads_size = 0;
    init();
}

cpu_topology::~cpu_topology() {
    topo_delete();
    cache_delete();
}


void cpu_topology::init() {
    // operating system dependent
    os_init();

    // sort the nodes, caches, etc.
    std::sort(nodes.begin(), nodes.end(), compare_nodes);
    for (t_node_it node_it = nodes.begin(); node_it != nodes.end(); node_it++) {
        t_node *node = *node_it;
        std::sort(node->cores.begin(), node->cores.end(), compare_cores);
        for (t_core_it core_it = node->cores.begin(); core_it != node->cores.end(); core_it++) {
            t_core *core = *core_it;
            std::sort(core->threads.begin(), core->threads.end(), compare_threads);
        }
    }
    for (t_cache_it cache_it = caches.begin(); cache_it != caches.end(); cache_it++) {
        t_cache *cache = *cache_it;
        std::sort(cache->threads.begin(), cache->threads.end(), compare_threads);
    }


    //topo_show();
}

/*************************************************************************************************
 *                                                                                               *
 *   INTERFACE                                                                                   *
 *                                                                                               *
 *************************************************************************************************/

std::size_t cpu_topology::get_cache_line(int cpu, int level) const {
    t_thread *thread = topo_cpu_get(cpu);
    if (thread == NULL)
        throw cpu_topology_exception("[get_cache_line] invalid cpu_id");
    if (level <= 0)
        throw cpu_topology_exception("[get_cache_line] argument level is negative or zero");
    if (thread->caches.size() < static_cast<std::size_t>(level))
        throw cpu_topology_exception("[get_cache_line] invalid level");

    return thread->caches[level - 1]->line;
}

std::size_t cpu_topology::get_cache_size(int cpu, int level) const {
    t_thread *thread = topo_cpu_get(cpu);
    if (thread == NULL)
        throw cpu_topology_exception("[get_cache_size] invalid cpu_id");
    if (level <= 0)
        throw cpu_topology_exception("[get_cache_line] argument level is negative or zero");
    if (thread->caches.size() < static_cast<std::size_t>(level))
        throw cpu_topology_exception("[get_cache_size] invalid level");

    return thread->caches[level - 1]->size;
}


std::size_t cpu_topology::get_cache_levels(int cpu_id) const {
    t_thread *thread = topo_cpu_get(cpu_id == -1 ? get_cpuid() : cpu_id);
    if (thread == NULL) { throw cpu_topology_exception("[get_cache_levels] invalid cpu_id"); }
    return thread->caches.size();
}

std::size_t cpu_topology::get_firstcache_line() const { return get_cache_line(get_cpuid(), 1); }

std::size_t cpu_topology::get_firstcache_size() const { return get_cache_size(get_cpuid(), 1); }

std::size_t cpu_topology::get_lastcache_line() const {
    int cpu_id = get_cpuid();
    return get_cache_line(cpu_id, (int) get_cache_levels(cpu_id));
}

std::size_t cpu_topology::get_lastcache_size() const {
    int cpu_id = get_cpuid();
    return get_cache_size(cpu_id, (int) get_cache_levels(cpu_id));
}

int cpu_topology::get_node() const { return get_node(get_cpuid()); }

int cpu_topology::get_node(int cpu_id) const {
    if (cpu_id < 0 || ((std::size_t) cpu_id) >= threads_size)
        throw cpu_topology_exception("[cpu_topology::get_node] invalid cpu_id");
    return threads[cpu_id]->owner->node->id;
}

void cpu_topology::get_nodes(std::vector<int> &output) const {
    output.resize(nodes.size());
    int i = 0;
    for (t_node_const_it it = nodes.begin(); it != nodes.end(); it++) {
        output[i++] = (*it)->id;
    }
}

std::size_t cpu_topology::get_node_count() const { return nodes.size(); }

std::vector<int> cpu_topology::get_threads_on_node(int node_id, bool include_smt) const {
    t_node *node = topo_node_get(node_id);
    if (!node) { throw cpu_topology_exception("[cpu_topology::get_threads_on_node] node does not exist"); }

    std::vector<int> result;
    for (auto core : node->cores) {
        if (include_smt) {
            for (auto thread : core->threads) {
                result.push_back(thread->id);
            }
        } else {
            assert(core->threads.size() >= 1);
            result.push_back(core->threads.at(0)->id);
        }
    }

    return result;
}

std::vector<int> cpu_topology::get_threads(bool interleaved, bool include_smt) const {
    std::vector<int> result;

    if (interleaved) {
        std::vector<std::vector<int>> threads;
        for (size_t i = 0; i < get_node_count(); i++) {
            threads.push_back(get_threads_on_node(i, include_smt));
        }

        // check all nodes have the same size
        assert(!threads.empty());
        for (size_t i = 1; i < threads.size(); i++) {
            if (threads[i].size() != threads[0].size()) {
                throw cpu_topology_exception("[cpu_topology::get_threads] nodes with different number of threads?");
            }
        }

        for (size_t j = 0; j < threads[0].size(); j++) {
            for (size_t i = 0; i < get_node_count(); i++) {
                result.push_back(threads[/* node = */i][/* thread = */ j]);
            }
        }
    } else {
        for (size_t i = 0; i < get_node_count(); i++) {
            auto threads = get_threads_on_node(i, include_smt);
            for (size_t j = 0; j < threads.size(); j++) {
                result.push_back(threads[j]);
            }
        }
    }

    return result;
}


void cpu_topology::get_sharing_threads(int thread_id_, int cache_level_, std::vector<int> &result_) const {
    t_thread *thread = topo_cpu_get(thread_id_);
    if (!thread) throw cpu_topology_exception("[cpu_topology::get_sharing_threads] thread_id does not exist");

    // search for the related cache
    t_cache *cache = 0;
    { // local scope
        t_cache_it cache_it = thread->caches.begin();
        t_cache_it cache_end = thread->caches.end();
        while (!cache && cache_it != cache_end) {
            if ((*cache_it)->level == cache_level_) {
                cache = *cache_it;
            } else {
                cache_it++;
            }
        }
    } // end local scope
    if (!cache) { throw cpu_topology_exception("[cpu_topology::get_sharing_threads] cache level not found"); }

    // add threads
    result_.clear();
    if (cache->threads.size() > 1) {
        for (t_thread_it thread_it = cache->threads.begin(); thread_it != cache->threads.end(); thread_it++) {
            int ret_thread_id = (*thread_it)->id;
            if (ret_thread_id != thread_id_) { result_.push_back(ret_thread_id); }
        }
    }
}

bool cpu_topology::share_cache_with_any(int thread_id, int cache_level, std::vector<int> &candidates) const {
    std::vector<int> thread_shareset;
    get_sharing_threads(thread_id, cache_level, thread_shareset);
    if (thread_shareset.empty()) return false;

    std::sort(candidates.begin(), candidates.end());
    std::vector<int>::const_iterator candidate_it = candidates.begin();
    std::vector<int>::const_iterator shareset_it = thread_shareset.begin();
    while (candidate_it != candidates.end() || shareset_it != thread_shareset.end()) {
        int c_id = (*candidate_it);
        int s_id = (*shareset_it);
        if (c_id == s_id) return true;
        else if (c_id < s_id) candidate_it++;
        else shareset_it++;
    }

    return false;
}

/*************************************************************************************************
 *                                                                                               *
 *   TOPOLOGY                                                                                    *
 *                                                                                               *
 *************************************************************************************************/
cpu_topology::t_thread *cpu_topology::topo_add(const int node_id, const int core_id, const int thread_id) {
    // search for the node
    t_node *node = 0;
    t_node_it node_it = nodes.begin();
    while (!node && node_it != nodes.end()) {
        if ((*node_it)->id == node_id) {
            node = *node_it;
        } else {
            node_it++;
        }
    }
    if (!node) { // the node does not already exist
        node = new t_node();
        node->id = node_id;
        nodes.push_back(node);
        //cout << "added node " << node_id << endl;
    }

    // add the core
    t_core *core = 0;
    t_core_it core_it = node->cores.begin();
    while (!core && core_it != node->cores.end()) {
        if ((*core_it)->id == core_id) {
            core = *core_it;
        } else {
            core_it++;
        }
    }
    if (!core) {
        core = new t_core();
        core->id = core_id;
        core->node = node;
        node->cores.push_back(core);
        //cout << "added core " << core_id << " for node " << node_id << endl;
    }

    // add the thread
    t_thread *thread = 0;
    t_thread_it thread_it = core->threads.begin();
    while (!thread && thread_it != core->threads.end()) {
        if (((*thread_it)->id) == thread_id) {
            thread = *thread_it;
        } else {
            thread_it++;
        }
    }
    if (!thread) {
        thread = new t_thread();
        thread->id = thread_id;
        thread->owner = core;
        core->threads.push_back(thread);

        if (static_cast<size_t>(thread_id) < threads_size) {
            threads[thread_id] = thread;
        } else {
            std::stringstream errormsg;
            (errormsg << "[cpu_topology::topo_add] index overflow: " << thread_id << "; size is: " << threads_size);
            throw cpu_topology_exception(errormsg.str());
        }
        //cout << "added thread " << thread_id << endl;
    }

    return thread;
}

cpu_topology::t_thread *cpu_topology::topo_cpu_get(int cpu_id) const {
    if (cpu_id < 0 || cpu_id > (int) threads_size)
        return NULL;
    else
        return threads[cpu_id];
}

cpu_topology::t_node *cpu_topology::topo_node_get(int node_id) const {
    if (nodes.empty()) { return NULL; }
    for (t_node_const_it it = nodes.begin(); it != nodes.end(); it++) {
        int cur_node_id = (*it)->id;
        if (cur_node_id == node_id) return *it;
        else if (node_id < cur_node_id) return NULL; // nodes are ordered
    }

    return NULL;
}


void cpu_topology::topo_delete() {
    // move by nodes
    for (t_node_it node_it = nodes.begin(); node_it != nodes.end(); ++node_it) {
        t_node *node = *node_it;

        // move by cores
        for (t_core_it core_it = node->cores.begin(); core_it != node->cores.end(); ++core_it) {
            t_core *core = *core_it;

            // move by threads
            for (t_thread_it thread_it = core->threads.begin(); thread_it != core->threads.end(); ++thread_it) {
                t_thread *thread = *thread_it;
                delete thread;
                *thread_it = 0;
            }

            delete core;
            *core_it = 0;
        }

        delete node;
        *node_it = 0;
    }

    nodes.clear();

    delete[] threads;
    threads = 0;
    threads_size = 0;
}

void cpu_topology::topo_show() const {

    // move by nodes
    for (t_node_const_it node_it = nodes.begin(); node_it != nodes.end(); node_it++) {
        t_node *node = *node_it;
        cout << "+ Node: " << node->id << endl;

        // move by cores
        for (t_core_const_it core_it = node->cores.begin(); core_it != node->cores.end(); core_it++) {
            t_core *core = *core_it;
            cout << "-+ Core: " << core->id << endl;

            // move by threads
            for (t_thread_const_it thread_it = core->threads.begin(); thread_it != core->threads.end(); thread_it++) {
                t_thread *thread = *thread_it;
                cout << "--+ Thread: " << thread->id << " [+Cache] ";

                // move by caches
                for (t_cache_const_it cache_it = thread->caches.begin(); cache_it != thread->caches.end(); cache_it++) {
                    t_cache *cache = *cache_it;
                    cout << "L" << cache->level << ": " << (cache->size / 1024) << "K ";

                    // is this cache shared?
                    if (cache->threads.size() > 1) {
                        cout << "(shared with:";

                        for (t_thread_it cache_thread_it = cache->threads.begin(); cache_thread_it != cache->threads.end(); cache_thread_it++) {
                            if ((*cache_thread_it)->id != thread->id) {
                                cout << ' ' << (*cache_thread_it)->id;
                            }
                        }
                        cout << ") ";
                    }
                }
                cout << endl;
            }
        }

    }
}

/*************************************************************************************************
 *                                                                                               *
 *   CACHES                                                                                      *
 *                                                                                               *
 *************************************************************************************************/
cpu_topology::t_cache *cpu_topology::cache_find(const shared_cpu_map_t &shared_cpu_map, int level) {
    if (caches.empty()) return NULL;

    t_cache_it cache_it = caches.begin();
    while (cache_it != caches.end()) {
        if ((*cache_it)->level == level && // same level
            (*cache_it)->shared_cpu_map.size() == shared_cpu_map.size() && // same shared cpu map size
            equal(begin((*cache_it)->shared_cpu_map), end((*cache_it)->shared_cpu_map), begin(shared_cpu_map))
                )
            return *cache_it;
        else
            ++cache_it;
    }

    return NULL;
}

cpu_topology::t_cache *cpu_topology::cache_add(shared_cpu_map_t &shared_cpu_map, int level, std::size_t size, std::size_t coherency_line_size) {
    // remove elements = 0 at the tail
    bool remove_tail = true;
    while (remove_tail && !shared_cpu_map.empty()) {
        remove_tail = shared_cpu_map.back() == 0;
        if (remove_tail) shared_cpu_map.pop_back();
    }

    // verify if not already present
    t_cache *cache = cache_find(shared_cpu_map, level);
    if (cache != NULL) return cache;

    cache = new t_cache();
    cache->shared_cpu_map = shared_cpu_map;
    cache->level = level;
    cache->size = size;
    cache->line = coherency_line_size;

    caches.push_back(cache);

    return cache;
}

void cpu_topology::cache_link(t_cache *c, t_thread *t) {
//      std::cout << "Linking cpu id " << t->id << " with cache L" << c->level << ", map " << c->shared_cpu_map << endl;

    c->threads.push_back(t);

    if (static_cast<std::size_t>(c->level) > t->caches.size()) {
        t->caches.insert(t->caches.end(), c->level - t->caches.size(), (t_cache *) NULL);
    }
    t->caches[c->level - 1] = c; // levels start from 1
}

void cpu_topology::cache_delete() {
    for (t_cache_it cache_it = caches.begin(); cache_it != caches.end(); cache_it++) {
        delete *cache_it;
        *cache_it = 0;
    }
    caches.clear();
}

/*************************************************************************************************
 *                                                                                               *
 *  SORTING FUNCTIONS                                                                            *
 *                                                                                               *
 *************************************************************************************************/
int cpu_topology::compare_nodes(t_node *n0, t_node *n1) { return n0->id < n1->id; }

int cpu_topology::compare_cores(t_core *c0, t_core *c1) { return c0->id < c1->id; }

int cpu_topology::compare_threads(t_thread *t0, t_thread *t1) { return t0->id < t1->id; }


} // namespace

#if defined(__linux__)
/*************************************************************************************************
 *                                                                                               *
 *  OS LINUX                                                                                     *
 *  27 Sep 2012                                                                                  *
 *                                                                                               *
 *************************************************************************************************/

#include <cctype> // isdigit
#include <cstdlib> // atoi
#include <fstream>
#include <list>

// current cpu where the thread is in execution
#include <sched.h>
#include <sys/syscall.h>
#include <sys/stat.h>

// GNU interface to access the filesystem
#include <dirent.h>
#include <unistd.h>

namespace teseo::util {

// Defined functions
namespace { //namespace to avoid potential conflicts
int get_directories(const std::string &path, std::list<std::string> &output);

int get_dirfflag(const std::string &path, std::list<std::string> &output, int flag);

std::string get_file_content(const std::string &path);

int hex2int(const std::string &strhex);

bool file_exists(const std::string &path);
}

/*************************************************************************************************
 *                                                                                               *
 *  OS INIT                                                                                      *
 *                                                                                               *
 *************************************************************************************************/
void cpu_topology::os_init() {
    // retrieve all the directory for the cpu path
    std::list<string> cpus;
    string cpu_path = "/sys/devices/system/cpu";
    get_directories(cpu_path, cpus);

    // filter out non relevant entries
    for (std::list<string>::iterator it = cpus.begin(); it != cpus.end(); it++) {
        string &directory = *it;
        if (!(directory.length() > 3 && directory.substr(0, 3) == "cpu" && isdigit(directory.at(3)))) {
            cpus.erase(it++);
        }
    }

    // threads index
    threads = new t_thread *[cpus.size()]();
    threads_size = cpus.size();

    // create the node topology
    for (std::list<string>::iterator it = cpus.begin(); it != cpus.end(); it++) {
        string &cpu_complete_name = *it;
        string str_cpu_id = cpu_complete_name.substr(3);
        string current_cpu_path = cpu_path + '/' + cpu_complete_name;
        string str_core_id = get_file_content(current_cpu_path + "/topology/core_id");
        string str_node_id = get_file_content(current_cpu_path + "/topology/physical_package_id");

        // convert retrieved ids into integers
        int node_id = atoi(str_node_id.c_str());
        int core_id = atoi(str_core_id.c_str());
        int thread_id = atoi(str_cpu_id.c_str());

        t_thread *cpu = topo_add(node_id, core_id, thread_id);

        //cout << "Node: " << node_id << ", core: " << core_id << ", thread: " << thread_id << endl;
        std::list<string> list_caches;
        get_directories(current_cpu_path + "/cache", list_caches);
        for (std::list<string>::iterator it_cacheindx = list_caches.begin(); it_cacheindx != list_caches.end(); it_cacheindx++) {
            string current_cache_path = current_cpu_path + "/cache/" + *it_cacheindx;

            string path_type = current_cache_path + "/type";
            if (!file_exists(path_type)) continue;
            string str_type = get_file_content(path_type);
            if (str_type != "Instruction") { // skip instruction caches
                string str_size = get_file_content(current_cache_path + "/size");
                string str_level = get_file_content(current_cache_path + "/level");
                string str_coherency_line_size = get_file_content(current_cache_path + "/coherency_line_size");
                string str_shared_cpu_map = get_file_content(current_cache_path + "/shared_cpu_map"); //hex value

                // process the shared_cpu_map field
                shared_cpu_map_t shared_cpu_map;
                bool shared_cpu_map_sepfound = true;
                size_t endpos = str_shared_cpu_map.length();
                do {
                    size_t startpos = str_shared_cpu_map.rfind(',', endpos - 1);
                    shared_cpu_map_sepfound = startpos != string::npos;
                    string segment;
                    if (shared_cpu_map_sepfound) {
                        segment = str_shared_cpu_map.substr(startpos + 1, endpos - startpos - 1);
                        endpos = startpos;
                    } else {
                        segment = str_shared_cpu_map.substr(0, endpos);
                    }
                    shared_cpu_map.push_back(hex2int(segment));
                } while (shared_cpu_map_sepfound);

                // convert retrieved data into int/size_t
                int cache_level = atoi(str_level.c_str());
                std::size_t cache_size = (std::size_t) atoi(str_size.c_str()) * 1024;
                std::size_t cache_line = (std::size_t) atoi(str_coherency_line_size.c_str());


                t_cache *cache = cache_add(shared_cpu_map, cache_level, cache_size, cache_line);

                cache_link(cache, cpu);
                //cout << "ID : " << cache_id << "; Level: " << cache_level  << "; Type: " << str_type << "; Size: " << cache_size << "; Line: " << cache_line << endl;
            }

        }
    }
}


/*************************************************************************************************
 *                                                                                               *
 *  GNU FILESYSTEM                                                                               *
 *                                                                                               *
 *************************************************************************************************/
namespace {

int get_directories(const std::string &path, std::list<std::string> &output) {
    return get_dirfflag(path, output, DT_DIR);
}

int get_dirfflag(const std::string &path, std::list<std::string> &output, int flag) {
    DIR *directory = opendir(path.c_str());
    if (!directory) return -1;

    struct dirent *directoryContent;
    while ((directoryContent = readdir(directory)) != NULL) {
        if (directoryContent->d_type == flag && directoryContent->d_name[0] != '.') {
            output.push_back(std::string(directoryContent->d_name));
        }
    }
    closedir(directory);

    return 0;
}

std::string get_file_content(const std::string &path) {
    std::fstream stream;
    std::string content;
    stream.open(path.c_str(), std::fstream::in);
    stream >> content;
    stream.close();
    return content;
}

bool file_exists(const std::string &path) {
    struct stat statbuf = {0};
    int rc = stat(path.c_str(), &statbuf);
    return rc == 0 && S_ISREG(statbuf.st_mode);
}

} // namespace anon

/*************************************************************************************************
 *                                                                                               *
 *  SUPPORT FUNCTIONS                                                                            *
 *                                                                                               *
 *************************************************************************************************/
namespace {
int hex2int(const std::string &strhex) {
    int x;
    std::stringstream ss;
    ss << std::hex << strhex;
    ss >> x;
    return x;
}
} // namespace anon

/**
 * Retrieves the cpu_id for the thread in execution
 */
int cpu_topology::os_get_cpuid() const {
    // sched_getcpu requires glibc 2.6+
    // andromeda is glibc 2.5
    //return sched_getcpu();

#ifdef SYS_getcpu
    int cpu_id;
    // SYS_getcpu exists from kernel 2.6.19, andromeda is 2.6.18 o.O
    int ret_value = syscall(SYS_getcpu, &cpu_id, NULL, NULL);
    return (ret_value == -1) ? ret_value : cpu_id;
#else // for kernel < 2.6.19
    pid_t pid = syscall(SYS_getpid);
    std::stringstream stringbuffer;
    int cpu_id;
    stringbuffer << "cat /proc/" << pid << "/stat | awk '{print $39}'";
    FILE* file = popen(stringbuffer.str().c_str(), "r");
    if (fscanf(file, "%d", &cpu_id) == EOF){
        cpu_id = -1;
    }
    pclose(file);

    return cpu_id;
#endif

}

} // namespace teseo::util

#elif defined(_WIN32) || defined(__CYGWIN__)
/*************************************************************************************************
 *                                                                                               *
 *  OS Windows                                                                                   *
 *  03 May 2014                                                                                  *
 *                                                                                               *
 *************************************************************************************************/
#include <cstdlib> // malloc
#include <limits> // numeric_limits
#define NOMINMAX // ask windows.h to not define the macros min & max
#include <windows.h>

namespace teseo::util {

// Defined functions
namespace { //namespace to avoid potential conflicts
    void _throw_exception_glpi(const std::string& prefix); // throw an exception for a failure of GetLogicalProcessorInformation
    void _mask2vector(std::vector<int>& v, ULONG_PTR mask); // transform the bitmask into a sequence of integers
    void _mask2vector(std::vector<int>& v, const std::vector<int32_t>& shared_cpu_map); // alias for the new API
    int _mask_slpi(std::vector< SYSTEM_LOGICAL_PROCESSOR_INFORMATION* >& v, ULONG_PTR); // find to which entry this mask is compatible
    int _mask_slpi(std::vector< SYSTEM_LOGICAL_PROCESSOR_INFORMATION* >& v, const std::vector<int32_t>& shared_cpu_map); // alias for the new the new API
    bool _mask_is_subset_of(ULONG_PTR a, ULONG_PTR b); // returns true if a is subset of b, in terms of bits
    ULONG_PTR _convert_cpumap(const std::vector<int32_t>& shared_cpu_map); // convert the field to a single int64_t value

    // avoid the memory leak on the buffer when returning from an exception
    class bufferptr {
        PSYSTEM_LOGICAL_PROCESSOR_INFORMATION ptr;
    public:
        bufferptr(PSYSTEM_LOGICAL_PROCESSOR_INFORMATION ptr_ = NULL) : ptr(ptr_) {}
        ~bufferptr() { free(ptr); ptr = NULL; };
        void reset(std::size_t sz) {
            free(ptr);
            ptr = static_cast<PSYSTEM_LOGICAL_PROCESSOR_INFORMATION> (malloc(sz));
            if (!ptr) throw cpu_topology_exception("[cpu_topology::os_init] memory allocation failure"); // it should not happen
        }
        PSYSTEM_LOGICAL_PROCESSOR_INFORMATION operator* () const { return ptr; };
    };


}

/*************************************************************************************************
 *                                                                                               *
 *  OS Init                                                                                      *                                                                                  *
 *                                                                                               *
 *************************************************************************************************/
void cpu_topology::os_init() {
    using std::size_t;

    // Use the function GetLogicalProcessorInformation from the Windows API to retrieve the information
    // regarding
    // see example on http://msdn.microsoft.com/en-us/library/windows/desktop/ms683194%28v=vs.85%29.aspx
    bufferptr buffer;
    DWORD bufferLength(0);

    // dummy invocation to discover the required size of the buffer
    BOOL rc = GetLogicalProcessorInformation(*buffer, &bufferLength);

    if (!rc && GetLastError() == ERROR_INSUFFICIENT_BUFFER) { //ok
        buffer.reset(bufferLength);

        // try again to get the information from the winzozz
        rc = GetLogicalProcessorInformation(*buffer, &bufferLength);

        // another error?
        if (!rc) _throw_exception_glpi("failure in the second invocation of the GetLogicalProcessorInformation API: ");
    }
    else { // error, but not the one we were expecting...
        _throw_exception_glpi("failure with the dummy invocation of GetLogicalProcessorInformation API: ");
    }

    // allocate the indexer for the (physical) threads
    SYSTEM_INFO sysInfo;  // usual windows mess ;-)
    GetSystemInfo(&sysInfo);
    threads_size = static_cast<size_t>(sysInfo.dwNumberOfProcessors);
    threads = new t_thread*[threads_size]();

    // iterate over the returned information
    // for a description of SYSTEM_LOGICAL_PROCESSOR_INFORMATION, see
    // http://msdn.microsoft.com/en-us/library/windows/desktop/ms686694%28v=vs.85%29.aspx
    PSYSTEM_LOGICAL_PROCESSOR_INFORMATION array_procinfo = *buffer;
    size_t array_procinfo_sz = bufferLength / sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);

    std::vector< SYSTEM_LOGICAL_PROCESSOR_INFORMATION* > tmp_nodes;
    std::vector< SYSTEM_LOGICAL_PROCESSOR_INFORMATION* > tmp_cores;

    for (size_t i = 0; i < array_procinfo_sz; i++) {
        SYSTEM_LOGICAL_PROCESSOR_INFORMATION& info = array_procinfo[i];
        switch (info.Relationship) {
        case RelationNumaNode:
        {
            tmp_nodes.push_back(&info);
            break;
        }
        case RelationProcessorCore:
        {
            tmp_cores.push_back(&info);
            break;
        }
        case RelationCache:
        {
            PROCESSOR_CACHE_TYPE type = info.Cache.Type;
            // the cache instruction gives a further interesting perspective of the underlying architecture, nevertheless
            // is outside the scope of this class to retrieve such information
            if (type != CacheInstruction && type != CacheTrace) {
                int64_t processorMask = static_cast<int64_t>(info.ProcessorMask); // FIXME: processorMask is 64 bit, it may cause truncation
                int level = static_cast<int>(info.Cache.Level);
                size_t linesize = static_cast<size_t>(info.Cache.LineSize);
                size_t size = static_cast<size_t>(info.Cache.Size);

                int32_t procmask_lo = static_cast<int32_t>(processorMask & std::numeric_limits<uint32_t>::max());
                int32_t procmask_hi = static_cast<int32_t>(processorMask >> 32);
                shared_cpu_map_t shared_cpu_map;
                shared_cpu_map.push_back(procmask_lo);
                shared_cpu_map.push_back(procmask_hi);

                cache_add(shared_cpu_map, level, size, linesize);

                //cout << "[" << i << "][CACHE] processor: " << info.ProcessorMask << " [" << processorMask << "] , level: "  << level <<
                //", LineSize: " << linesize << ", Size: " << size << ", Line: " << linesize << endl;
            }

            break;
        }
        default:
        {
            // ignore
        }
        }
    }

    // now iterate over the caches of level 1 to discover the physical threads
    std::vector<int> maskvector;
    maskvector.reserve(64);
    for (size_t i = 0; i < caches.size(); i++) {
        if (caches[i]->level == 1) {
            int node_id = tmp_nodes[_mask_slpi(tmp_nodes, caches[i]->shared_cpu_map)]->NumaNode.NodeNumber;
            int core_id = _mask_slpi(tmp_cores, caches[i]->shared_cpu_map);

            maskvector.clear();
            _mask2vector(maskvector, caches[i]->shared_cpu_map);
            for (size_t j = 0; j < maskvector.size(); j++) {
                int thread_id = maskvector[j];
                //cout << "cache_id:" << i << ", node: " << node_id << ", core: " << core_id << ", thread: " << thread_id << endl; // debug

                if (static_cast<size_t>(thread_id) > threads_size)
                    throw cpu_topology_exception("[cpu_topology::os_init] [Windows] thread id is equal or greater than thread size");

                t_thread* thread = topo_add(node_id, core_id, thread_id);
                threads[thread_id] = thread;

                cache_link(caches[i], thread);
            }
        }
    }

    // link the rest of the caches
    // we cannot link these caches before because there is no guarantee that all involved threads were allocated
    for (size_t i = 0; i < caches.size(); i++) {
        if (caches[i]->level > 1) {
            maskvector.clear();
            _mask2vector(maskvector, caches[i]->shared_cpu_map);
            for (size_t j = 0; j < maskvector.size(); j++) {
                int thread_id = maskvector[j];
                if (static_cast<size_t>(thread_id) > threads_size)
                    throw cpu_topology_exception("[cpu_topology::os_init] [Windows] thread id is equal or greater than thread size");
                cache_link(caches[i], threads[thread_id]);
            }
        }
    }

    //    topo_show(); // debug
}

int cpu_topology::os_get_cpuid() const {
    return static_cast<int>(GetCurrentProcessorNumber());
}

/*************************************************************************************************
 *                                                                                               *
 *  OS Support functions                                                                         *                                                                                  *
 *                                                                                               *
 *************************************************************************************************/
namespace{

    void _throw_exception_glpi(const std::string& prefix) {
        std::stringstream errormsg;
        errormsg << "[cpu_topology::os_init] " << prefix << GetLastError();
        throw cpu_topology_exception(errormsg.str());
    }

    // remind to reset the vector before invoking this function!
    void _mask2vector(std::vector<int>& v, ULONG_PTR mask) {
        int value = 0;
        while (mask != 0) {
            if (mask % 2 == 1) v.push_back(value);
            mask /= 2;
            value++;
        }
    }

    void _mask2vector(std::vector<int>& v, const std::vector<int32_t>& shared_cpu_map) {
        _mask2vector(v, _convert_cpumap(shared_cpu_map));
    }

    int _mask_slpi(std::vector< SYSTEM_LOGICAL_PROCESSOR_INFORMATION* >& v, ULONG_PTR a) {
        for (std::size_t i = 0; i < v.size(); i++) {
            ULONG_PTR b = v[i]->ProcessorMask;
            if (_mask_is_subset_of(a, b)) return static_cast<int>(i);
        }

        // not found !
        throw cpu_topology_exception("[_mask_slpi] unable to match the given mask");
    }

    int _mask_slpi(std::vector< SYSTEM_LOGICAL_PROCESSOR_INFORMATION* >& v, const std::vector<int32_t>& shared_cpu_map) {
        return _mask_slpi(v, _convert_cpumap(shared_cpu_map));
    }

    bool _mask_is_subset_of(ULONG_PTR a, ULONG_PTR b) {
        while (a != 0) {
            if (a % 2 == 1 && b % 2 == 0)
                return false;

            // move on
            a /= 2;
            b /= 2;
        }

        return true;
    }

    ULONG_PTR _convert_cpumap(const std::vector<int32_t>& shared_cpu_map) {
        ULONG_PTR result = 0;
        if (shared_cpu_map.size() > 0) result += shared_cpu_map[0];
        if (shared_cpu_map.size() > 1) result += ((int64_t)shared_cpu_map[1]) << 32;
        if (shared_cpu_map.size() > 2) throw cpu_topology_exception("Field shared_cpu_map too big");
        return result;
    }
}

} // namespace teseo::util
#endif

/*************************************************************************************************
 *                                                                                               *
 *  EXCEPTIONS                                                                                   *
 *                                                                                               *
 *************************************************************************************************/
namespace teseo::util {

cpu_topology_exception::cpu_topology_exception(const std::string &msg) throw() : runtime_error(msg), message(msg) {}

const char *cpu_topology_exception::what() const throw() { return message.c_str(); }

} // namespace
