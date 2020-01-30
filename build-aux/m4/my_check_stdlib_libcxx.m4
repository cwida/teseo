# Check whether the option -stdlib=libc++ is available and the program
# can be compiled, linked and executed.
# libc++ is the STL replacement from the LLVM community:
#   https://libcxx.llvm.org/
# 
# serial 1

# MY_CHECK_STDLIB_LIBCXX([action-if-true], [action-if-false])
# -------------------------------------------
#
# Checks whether the current compiler can compile, link and execute
# programs linked with the option -stdlib=libc++.
# In case of success, it executes the `action-if-true', otherwise
# `action-if-false'
AC_DEFUN([MY_CHECK_STDLIB_LIBCXX], [
  AS_VAR_PUSHDEF([CACHEVAR],[my_cv_check_stdlib_libcxx])
  AC_MSG_CHECKING([whether ${_AC_CC} supports the libc++ replacement])
  my_check_save_flags=$[]_AC_LANG_PREFIX[]FLAGS
  _AC_LANG_PREFIX[]FLAGS="$[]_AC_LANG_PREFIX[]FLAGS -stdlib=libc++"
  AC_RUN_IFELSE([AC_LANG_SOURCE([
        #include <iostream>
        #include <string>
        
        using namespace std;
        
        int main(int argc, const char* argv@<:@@:>@){
          cout << "yes" << endl;
          return 0;
        }
    ])],
    [AS_VAR_SET(CACHEVAR,[yes]);], dnl The result is written by the cout program
    [AS_VAR_SET(CACHEVAR,[no]); AC_MSG_RESULT([no])],
    [AS_VAR_SET(CACHEVAR,[maybe]); AC_MSG_RESULT([maybe, cross compiling...])]
  )
  _AC_LANG_PREFIX[]FLAGS=${my_check_save_flags}
  AS_VAR_IF(CACHEVAR, yes, [m4_default([$1], :)], [m4_default([$2], :)])
  AS_VAR_POPDEF([CACHEVAR])
])



