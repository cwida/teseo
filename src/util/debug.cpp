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

#include "teseo/util/debug.hpp"

using namespace std;

namespace debug::util {

mutex g_debugging_mutex;

string debug_clean_name(const char* pretty_function){
    // skip the type
    int pos_end = 0;
    bool stop = false;
    do {
        switch(pretty_function[pos_end]){
        case '\0':
        case ' ':
            stop = true;
            break;
        default:
            /* nop */ ;
        }
        pos_end++;
    } while(!stop);


    // fetch the class and the method name
    int pos_start = pos_end;
    int pos_intermediate = 0;
    stop = false;
    bool ignore_colon = false;
    do {
        switch(pretty_function[pos_end]){
        case ':':
            if(!ignore_colon){
                if(pos_intermediate > 0){
                    pos_start = pos_intermediate +1;
                }
                pos_intermediate = pos_end +1;
            }
            ignore_colon = !ignore_colon;
            pos_end++;
            break;
        case '(':
        case '\0':
            stop = true;
            break;
        default:
            pos_end++;
        }
    } while(!stop);

    return string(pretty_function + pos_start, pos_end - pos_start);
}

} // namespace


