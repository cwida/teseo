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

#include "catch.hpp"

#if defined(HAVE_NUMA)
#include <numa.h>
#include <numaif.h>
#endif
#include <sched.h>
#include <thread>

#include "teseo/aux/view.hpp"
//#include "teseo/aux/builder.hpp"
//#include "teseo/aux/item.hpp"
//#include "teseo/aux/partial_result.hpp"
//#include "teseo/aux/static_view.hpp"
#include "teseo/context/global_context.hpp"
//#include "teseo/context/scoped_epoch.hpp"
#include "teseo/context/static_configuration.hpp"
//#include "teseo/memstore/context.hpp"
//#include "teseo/memstore/key.hpp"
//#include "teseo/memstore/index.hpp"
//#include "teseo/memstore/leaf.hpp"
#include "teseo/memstore/memstore.hpp"
//#include "teseo/memstore/segment.hpp"
//#include "teseo/runtime/runtime.hpp"
#include "teseo/transaction/transaction_impl.hpp"
#include "teseo/util/thread.hpp"
#include "teseo.hpp"

using namespace std;
using namespace teseo;

#if defined(HAVE_NUMA)

/**
 * On a machine with two NUMA nodes, check whether the aux views are allocated in their local memory
 */
TEST_CASE("numa_aux_view", "[numa]") {
    if(context::StaticConfiguration::numa_num_nodes >= 2){
        Teseo teseo;
        [[maybe_unused]] auto memstore = context::global_context()->memstore();

        // add a few items in the storage
        auto tx = teseo.start_transaction();
        tx.insert_vertex(10);
        tx.insert_vertex(20);
        tx.insert_vertex(30);
        tx.insert_vertex(40);
        tx.insert_edge(10, 20, 1020);
        tx.insert_edge(10, 30, 1030);
        tx.insert_edge(10, 40, 1040);
        tx.commit();


        tx = teseo.start_transaction(/* read only ? */ true);
        aux::StaticView* views[2];


        auto init_view = [&teseo, &views](Transaction tx, int node){
            teseo.register_thread();

            // pin the thread to the given NUMA node
            auto numa_bitmask = numa_allocate_cpumask();
            int rc = numa_node_to_cpus(node, numa_bitmask);
            REQUIRE(rc == 0);
            cpu_set_t cpu_bitmask;
            CPU_ZERO(&cpu_bitmask);
            for(unsigned int i = 0, end = numa_num_possible_cpus(); i < end; i++){
                if(numa_bitmask_isbitset(numa_bitmask, i)){
                    CPU_SET(i, &cpu_bitmask);
                }
            }
            rc = sched_setaffinity(/* calling process */ 0, sizeof(cpu_set_t), &cpu_bitmask);
            REQUIRE(rc == 0);

            REQUIRE(util::Thread::get_numa_id() == node);
            numa_free_cpumask(numa_bitmask);

            auto tx_impl = reinterpret_cast<transaction::TransactionImpl*>(tx.handle_impl());
            auto view = static_cast<aux::StaticView*>(tx_impl->aux_view(/* numa aware ? */ true));

            // check that the view has indeed been allocated to the local memory
            int mem_node = -1;
            rc = get_mempolicy(&mem_node, /* node mask */ nullptr, /* node mask size */ 0, /* address */ view, MPOL_F_NODE | MPOL_F_ADDR);
            REQUIRE(rc == 0);
            REQUIRE(mem_node == node);

            views[node] = view;

            teseo.unregister_thread();
        };

        thread(init_view, tx, 0).join();
        thread(init_view, tx, 1).join();

        // check the content of the two views is equal
        for(uint64_t i = 0, N = tx.num_vertices(); i < N; i++){
            REQUIRE(views[0]->vertex_id(i) == views[1]->vertex_id(i));
            REQUIRE(views[0]->degree(i, /* logical */ true) == views[1]->degree(i, /* logical */ true));
            uint64_t vertex_id = views[0]->vertex_id(i);
            REQUIRE(views[0]->logical_id(vertex_id) == i);
            REQUIRE(views[0]->logical_id(vertex_id) == views[1]->logical_id(vertex_id));
            REQUIRE(views[0]->degree(vertex_id, /* logical */ false) == views[1]->degree(vertex_id, /* logical */ false));
        }
    }
}

#endif
