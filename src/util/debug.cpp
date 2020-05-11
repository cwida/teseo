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

namespace teseo::util {

mutex g_debugging_mutex;

string debug_function_name(const char* pretty_function){
    // skip the type
    bool stop = false;
    int pos_end = 0;
    int num_templates = 0;
    do {
        switch(pretty_function[pos_end]){
        case '<':
            num_templates++;
            break;
        case '>':
            num_templates--;
            break;
        case '(':
            if(num_templates == 0){
                pos_end = -1;
                stop = true;
            }
            break;
        case ' ':
            if(num_templates == 0){
                stop = true;
            }
            break;
        case '\0': // how is this possible, it should always print some kind of parentheses for a method...
            stop = true;
            break;
        default:
            /* nop */ ;
        }
        pos_end++;
    } while(!stop);

    // fetch the class and the method name
    int pos_start = pos_end;
    int pos_first_uppercase = -1;
    int pos_intermediate = 0;
    stop = false;
    bool ignore_colon = false;
    num_templates = 0;
    do {
        char character = pretty_function[pos_end];
        if(character == '<'){
            num_templates++;
            pos_end++;
        } else if (character == '>'){
            num_templates--;
            pos_end++;
        } else if (character == '\0'){
            stop = true;
        } else if(num_templates == 0){
            if((character >= 'A' && character <= 'Z') && (pos_end == pos_start || pos_end == pos_intermediate +1)){
                if(pos_first_uppercase == -1){
                    pos_first_uppercase = pos_end;
                }
                pos_end ++;
            } else {
                switch(character){
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
                    stop = true;
                    break;
                default:
                    pos_end++;
                }
            }
        } else {
            pos_end++;
        }
    } while(!stop);

    if(pos_first_uppercase != -1){
        pos_start = pos_first_uppercase;
    }

    string result ( pretty_function + pos_start , pos_end - pos_start );

    // append the parentheses ( ) if the function is "operator"
    if(pos_intermediate != 0){
        string method_name ( pretty_function + pos_intermediate +1, pos_end - pos_intermediate  -1);
        if(method_name == "operator"){ result += "()"; }
    }

    return result;
}

} // namespace


