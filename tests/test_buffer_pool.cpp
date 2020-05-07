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

#include <iostream>

#include "teseo/bp/buffer_pool.hpp"
#include "teseo/bp/physical_memory.hpp"

using namespace std;
using namespace teseo;

/**
 * Check whether the BP is able to infer a limit for the max logical memory that can be reserved in advance
 */
TEST_CASE("bp_get_max_logical_memory", "[bp]") {
    uint64_t limit = bp::PhysicalMemory::get_max_logical_memory();
    REQUIRE(limit >= (1ull<<30));
}

/**
 * Check whether we're able to init & destroy the physical memory, without raising an error
 */
TEST_CASE("bp_physical_memory", "[bp]") {
    bp::PhysicalMemory pm { /* num pages */ 4 } ;
    REQUIRE(pm.get_num_allocated_pages() == 4);
    REQUIRE(pm.get_start_address() != nullptr);
    REQUIRE(pm.get_allocated_memory() == pm.page_size() * 4);
    pm.extend(2);
    REQUIRE(pm.get_num_allocated_pages() == 6);
    REQUIRE(pm.get_allocated_memory() == pm.page_size() * 6);
    pm.shrink(3);
    REQUIRE(pm.get_num_allocated_pages() == 3);
    REQUIRE(pm.get_allocated_memory() == pm.page_size() * 3);
    pm.shrink(3);
    REQUIRE(pm.get_num_allocated_pages() == 0);
    REQUIRE(pm.get_allocated_memory() == pm.page_size() * 0);
    pm.extend(12);
    REQUIRE(pm.get_num_allocated_pages() == 12);
    REQUIRE(pm.get_allocated_memory() == pm.page_size() * 12);
}

TEST_CASE("bp_allocate_page", "[bp]"){
    bp::BufferPool bp;
    uint64_t* page1 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    *page1 = 1;

    uint64_t* page2 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    *page2 = 2;

    uint64_t* page3 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    *page3 = 3;

    uint64_t* page4 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    *page4 = 4;

    uint64_t* page5 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    *page5 = 5;

    REQUIRE(*page1 == 1);
    REQUIRE(*page2 == 2);
    REQUIRE(*page3 == 3);
    REQUIRE(*page4 == 4);
    REQUIRE(*page5 == 5);


    bp.deallocate_page(page1);
    bp.deallocate_page(page2);
    bp.deallocate_page(page3);
    bp.deallocate_page(page4);
    bp.deallocate_page(page5);
}

TEST_CASE("bp_free_list1", "[bp]"){
    bp::BufferPool bp;
    uint64_t* page0 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page1 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page2 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page3 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page4 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page5 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page6 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page7 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    uint64_t* page8 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(bp.get_num_available_pages() == 3);
    bp.deallocate_page(page8);
    REQUIRE(bp.get_num_available_pages() == 4);
    bp.deallocate_page(page7);
    REQUIRE(bp.get_num_available_pages() == 5);
    bp.deallocate_page(page6);
    REQUIRE(bp.get_num_available_pages() == 6);

    bp.rebuild_free_list();

    REQUIRE(bp.get_num_available_pages() == 2);
    uint64_t* page6b = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page6 == page6b);
    uint64_t* page7b = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page7 == page7b);

    bp.deallocate_page(page0);
    bp.deallocate_page(page1);
    bp.deallocate_page(page2);
    bp.deallocate_page(page3);
    bp.deallocate_page(page4);
    bp.deallocate_page(page5);
    bp.deallocate_page(page6);
    bp.deallocate_page(page7);
}

TEST_CASE("bp_free_list2", "[bp]"){
    bp::BufferPool bp;
    vector<void*> allocated_pages;
    for(uint64_t i = 0; i < 13; i++){
        allocated_pages.push_back(bp.allocate_page());
    }

    for(uint64_t i = 0; i < 13; i++){
        bp.deallocate_page(allocated_pages[i]);
    }

    bp.rebuild_free_list();
    REQUIRE(bp.get_num_available_pages() == 4);
    uint64_t* page0 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page0 == allocated_pages[0]);
    uint64_t* page1 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page1 == allocated_pages[1]);
    uint64_t* page2 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page2 == allocated_pages[2]);
    uint64_t* page3 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page3 == allocated_pages[3]);

    bp.deallocate_page(page0);
    bp.deallocate_page(page1);
    bp.deallocate_page(page2);
    bp.deallocate_page(page3);
}

TEST_CASE("bp_free_list3", "[bp]"){
    bp::BufferPool bp;
    vector<void*> allocated_pages;
    for(uint64_t i = 0; i < 16; i++){
        allocated_pages.push_back(bp.allocate_page());
    }

    bp.deallocate_page(allocated_pages[15]);
    bp.deallocate_page(allocated_pages[13]);

    REQUIRE(bp.get_num_available_pages() == 2);
    bp.rebuild_free_list();
    REQUIRE(bp.get_num_available_pages() == 2);

    uint64_t* page13 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page13 == allocated_pages[13]);
    uint64_t* page15 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page15 == allocated_pages[15]);
    REQUIRE(bp.get_num_available_pages() == 0);

    for(uint64_t i = 0; i < allocated_pages.size(); i++){
        bp.deallocate_page(allocated_pages[i]);
    }
}

TEST_CASE("bp_free_list4", "[bp]"){
    bp::BufferPool bp;
    vector<void*> allocated_pages;
    for(uint64_t i = 0; i < 16; i++){
        allocated_pages.push_back(bp.allocate_page());
    }

    bp.deallocate_page(allocated_pages[14]);
    bp.deallocate_page(allocated_pages[13]);
    bp.deallocate_page(allocated_pages[15]);
    bp.deallocate_page(allocated_pages[11]);

    REQUIRE(bp.get_num_available_pages() == 4);
    bp.rebuild_free_list();
    REQUIRE(bp.get_num_available_pages() == 4);

    uint64_t* page11 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page11 == allocated_pages[11]);
    uint64_t* page13 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page13 == allocated_pages[13]);
    uint64_t* page14 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page14 == allocated_pages[14]);
    uint64_t* page15 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page15 == allocated_pages[15]);
    REQUIRE(bp.get_num_available_pages() == 0);

    for(uint64_t i = 0; i < allocated_pages.size(); i++){
        bp.deallocate_page(allocated_pages[i]);
    }
}

TEST_CASE("bp_free_list5", "[bp]"){
    bp::BufferPool bp;
    vector<void*> allocated_pages;
    for(uint64_t i = 0; i < 16; i++){
        allocated_pages.push_back(bp.allocate_page());
    }

    bp.deallocate_page(allocated_pages[14]);
    bp.deallocate_page(allocated_pages[13]);
    bp.deallocate_page(allocated_pages[15]);
    bp.deallocate_page(allocated_pages[11]);
    bp.deallocate_page(allocated_pages[10]);
    bp.deallocate_page(allocated_pages[9]);

    REQUIRE(bp.get_num_available_pages() == 6);
    bp.rebuild_free_list();
    REQUIRE(bp.get_num_available_pages() == 6);

    uint64_t* page9 = reinterpret_cast<uint64_t*>(bp.allocate_page());
    REQUIRE(page9 == allocated_pages[9]);
    bp.deallocate_page(allocated_pages[12]);
    REQUIRE(bp.get_num_available_pages() == 6);

    bp.rebuild_free_list();
    REQUIRE(bp.get_num_available_pages() == 2);


    for(uint64_t i = 0; i < 10; i++){
        bp.deallocate_page(allocated_pages[i]);
    }
}


