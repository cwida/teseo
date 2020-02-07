#############################################################################
# Initialization
AC_PREREQ([2.69])
AC_INIT([teseo], [0.1])
AC_CONFIG_AUX_DIR([build-aux])
AC_PRESERVE_HELP_ORDER
AC_LANG([C++])

#############################################################################
# Main makefile
AC_CONFIG_FILES([Makefile])

#############################################################################
# Set the compiler $CC and $CXX. Prefer clang over gcc
old_CFLAGS="${CFLAGS}"; old_CXXFLAGS="${CXXFLAGS}" dnl Do not automatically set -g -O2
AC_PROG_CC([clang gcc icc cc])
AC_PROG_CXX([clang++ g++ icpc c++ cxx])
CFLAGS="${old_CFLAGS}"; unset old_CFLAGS; CXXFLAGS="${old_CXXFLAGS}"; unset old_CXXFLAGS;
AX_CXX_COMPILE_STDCXX_17 dnl Ask for C++17, thanks!

#############################################################################
# Check whether the user has explicitly set CPPFLAGS, CFLAGS and CXXFLAGS. If 
# so we try to avoid polluting these flags and respect the user setting
m4_divert_push([INIT_PREPARE]) 
if test "x${CPPFLAGS}" != "x"; then ac_user_cppflags="yes"; fi
if test "x${CFLAGS}" != "x"; then ac_user_cflags="yes"; fi
if test "x${CXXFLAGS}" != "x"; then ac_user_cxxflags="yes"; fi
m4_divert_pop([INIT_PREPARE])

#############################################################################
# pthreads
AC_SEARCH_LIBS([pthread_create], [pthread], [],
    [ AC_MSG_ERROR([missing prerequisite: this program requires pthreads to work (dependency for sqlite3)]) ])
    
#############################################################################
# libnuma
have_libnuma="yes"
AC_CHECK_HEADERS([numaif.h numa.h], [], [have_libnuma="no"; break;], [ [/* avoid default includes */] ])
AS_IF([test x"${have_libnuma}" == x"yes"], [AC_SEARCH_LIBS([numa_available], [numa], [], [have_libnuma="no"])])
if test x"${have_libnuma}" == x"yes"; then
    AC_MSG_NOTICE([libnuma support enabled...])
    CPPFLAGS="${CPPFLAGS} -DHAVE_LIBNUMA";
else
    AC_MSG_WARN([libnuma support disabled...])
fi

#############################################################################
# libpapi (profiler support)
have_libpapi="yes"
AC_CHECK_HEADERS([papi.h], [], [have_libpapi="no"; break;], [ [/* avoid default includes */] ])
AS_IF([test x"${have_libpapi}" == x"yes"], [AC_SEARCH_LIBS([PAPI_library_init], [papi], [], [have_libpapi="no"])])
if test x"${have_libpapi}" == x"yes"; then
    AC_MSG_NOTICE([libpapi support enabled...])
    CPPFLAGS="${CPPFLAGS} -DHAVE_LIBPAPI";
else
    AC_MSG_WARN([libpapi support disabled...])
fi

#############################################################################
# Position independent code (PIC)
MY_ARG_ENABLE([debug], 
    [Whether to enable the debug flags], 
    [yes no], [yes]) 
dnl first argument is the variable with the flags, the second argument is the language
m4_defun([_my_set_pic], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    AC_LANG_PUSH([$1])
    MY_SET_CC_FLAG([_FLAGS], [-fPIC])
    AC_LANG_POP([$1])
    m4_popdef([_FLAGS])
])

_my_set_pic([C])
_my_set_pic([C++])
m4_undefine([_my_set_pic])

#############################################################################
# Debug flags (-g)
MY_ARG_ENABLE([debug], 
    [Whether to enable the debug flags], 
    [yes no], [yes]) 
dnl first argument is the variable with the flags, the second argument is the language
m4_defun([_my_set_debug_flags], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    [if test -n "${ac_user_]m4_tolower(_FLAGS)[}"; then]
         AC_MSG_WARN([Action --enable-debug ignored as _FLAGS has been explicitly set through command line])
    else
        # Append either -g or -g3
        AC_LANG_PUSH([$1])
        AX_CHECK_COMPILE_FLAG([[-g3]], [AS_VAR_APPEND([_FLAGS], " -g3")], [AS_VAR_APPEND([_FLAGS], " -g")] )

        # Force clang to emit the whole debug information
        MY_SET_CC_FLAG([_FLAGS], [-fno-limit-debug-info])
        MY_SET_CC_FLAG([_FLAGS], [-fno-omit-frame-pointer])
        AC_LANG_POP([$1])
    fi
    m4_popdef([_FLAGS])
])
if( test x"${enable_debug}" = x"yes" ); then
    _my_set_debug_flags([C])
    _my_set_debug_flags([C++])
fi
m4_undefine([_my_set_debug_flags])

#############################################################################
# Warning flags (-Wall)
MY_ARG_ENABLE([warnings], 
    [Whether to enable all warnings (-Wall)], 
    [yes no], [yes])
m4_defun([_my_set_warnings], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    [if test -n "${ac_user_]m4_tolower(_FLAGS)[}"; then]
        AC_MSG_WARN([Action --enable-warnings ignored as _FLAGS has been explicitly set through command line])
    [else]
        AS_VAR_APPEND([_FLAGS], [" -Wall"])
    [fi]
    m4_popdef([_FLAGS])
])
if( test x"${enable_warnings}" = x"yes" ); then
    _my_set_warnings([C])
    _my_set_warnings([CXX])
fi
m4_undefine([_my_set_warnings])

#############################################################################
# Optimization flags (-O3)
MY_ARG_ENABLE([optimize], [Whether to enable the optimization flags], [yes no], [no])

m4_defun([_my_set_optimization_flags], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    [if test -n "${ac_user_]m4_tolower(_FLAGS)[}"; then]
        AC_MSG_WARN([Action --enable-optimize ignored as _FLAGS has been explicitly set through command line])
    [else]
        if( test x"${enable_optimize}" = x"yes" ); then
            AS_VAR_APPEND([_FLAGS], [[" -O3"]])
            AC_LANG_PUSH([$1])
            MY_SET_CC_FLAG([_FLAGS], [-march=native])
            MY_SET_CC_FLAG([_FLAGS], [-mtune=native])
            MY_SET_CC_FLAG([_FLAGS], [-fno-stack-protector])
            AC_LANG_POP([$1])
        else
            AS_VAR_APPEND([_FLAGS], [[" -O0"]])
        fi
    [fi]
    m4_popdef([_FLAGS])
])
_my_set_optimization_flags([C])
_my_set_optimization_flags([C++])
m4_undefine([_my_set_optimization_flags])

#############################################################################
# Assertions. Possible values:
# yes => nop
# no => CPPFLAGS += -DNDEBUG
# auto => yes if the optimize mode is not enabled, no otherwise
MY_ARG_ENABLE([assert], 
    [Whether to enable assertions. The option 'auto' implies the assertions are enabled when --enable-optimize is not specified], 
    [yes no auto], [auto])
    
if (test x"${enable_assert}" = x"auto"); then
    if (test x"${enable_optimize}" != x"yes"); then
        enable_assert=yes
    else
        enable_assert=no
    fi
fi
if (test x"${enable_assert}" = x"yes"); then
    : ; # nop
elif (test x"${enable_assert}" = x"no"); then
    CPPFLAGS="${CPPFLAGS} -DNDEBUG"
else
    AC_MSG_ERROR([Invalid value for --enable-assert: ${enable_assert}])
fi

#############################################################################
# Remove extra blanks from our variables
EXTRA_CPPFLAGS=$(echo ${EXTRA_CPPFLAGS} | xargs)
CPPFLAGS=$(echo ${CPPFLAGS} | xargs);
CFLAGS=$(echo ${CFLAGS} | xargs);
EXTRA_CFLAGS=$(echo ${EXTRA_CFLAGS} | xargs);
CXXFLAGS=$(echo ${CXXFLAGS} | xargs);
EXTRA_CXXFLAGS=$(echo ${EXTRA_CXXFLAGS} | xargs);
EXTRA_LDFLAGS=$(echo ${EXTRA_LDFLAGS} | xargs);
# these two variables are only for presentation, overriding won't achieve much
ALL_CFLAGS=$(echo ${EXTRA_CPPFLAGS} ${CPPFLAGS} ${EXTRA_CFLAGS} ${CFLAGS} | xargs)
ALL_CXXFLAGS=$(echo ${EXTRA_CPPFLAGS} ${CPPFLAGS} ${EXTRA_CXXFLAGS} ${CXXFLAGS} | xargs)
LDFLAGS="${LDFLAGS} ${EXTRA_LDFLAGS} ${LIBS}"

#############################################################################
# CC, CXX and linker additional output variables
AC_SUBST([EXTRA_CPPFLAGS])
AC_SUBST([EXTRA_CFLAGS])
AC_SUBST([EXTRA_CXXFLAGS])

#############################################################################
# Create the configure script
AC_OUTPUT

#############################################################################
# Final summary
echo \
"-------------------------------------------------
${PACKAGE_NAME} version ${PACKAGE_VERSION}
Compiler C..........: ${CC} ${ALL_CFLAGS}
Compiler C++........: ${CXX} ${ALL_CXXFLAGS}
Linker..............: ${CXX} ${LDFLAGS}
Enable assertions...: ${enable_assert}
Enable debug........: ${enable_debug}
Enable optimize.....: ${enable_optimize}

Now type 'make -j'
--------------------------------------------------"