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

#include "teseo/profiler/save_to_disk.hpp"

#include <fstream>
#include <iostream>
#include <string>

#include "teseo/profiler/event_global.hpp"
#include "teseo/profiler/rebal_global_list.hpp"
#include "teseo/util/chrono.hpp"
#include "teseo/util/thread.hpp"

using namespace std;

namespace teseo::profiler {

void save_to_disk(EventGlobal* global_events, GlobalRebalanceList* rebalance_events){
    // where to save the content
    string path = "/tmp/teseo-profdata-";
    path += to_string(util::Thread::get_process_id());
    path += ".json";

    fstream out(path, ios::out);
    bool first = true;

    out << "{";

    if(global_events != nullptr){
        if(!first) { out << ", "; }

        out << "\"profiler\":";
        global_events->to_json(out);

        first = false;
    }

   if(rebalance_events != nullptr){
       if(!first) { out << ", "; }

       out << "\"rebalancer\":";
       rebalance_events->to_json(out);

       first = false;
   }

    out << "}";

    out.close();

    cout << "[TESEO] Profiler data saved to: " << path << endl;
}

} // namespace



