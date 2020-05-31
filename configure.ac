#############################################################################
# Initialization
AC_PREREQ([2.69])
AC_INIT([teseo], [0.3])
AC_CONFIG_AUX_DIR([build-aux])
AC_PRESERVE_HELP_ORDER
AC_LANG([C++])

#############################################################################
# Main makefile
AC_CONFIG_FILES([Makefile include/teseo/context/static_configuration.hpp])

#############################################################################
# Set the compiler $CC and $CXX. 
old_CFLAGS="${CFLAGS}"; old_CXXFLAGS="${CXXFLAGS}" dnl Do not automatically set -g -O2
AC_PROG_CC([gcc cc clang icc])
AC_PROG_CXX([g++ cxx clang++ icpc c++])
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
    [ AC_MSG_ERROR([missing prerequisite: this program requires pthreads to work]) ])
    
#############################################################################
# NUMA support
AC_ARG_ENABLE([numa], AS_HELP_STRING([--enable-numa], [Whether to enable support for NUMA. It requires a dependency on libnuma.]))
    
if test x"${enable_numa}" = x"" -o x"${enable_numa}" = x"yes" ; then
    have_libnuma="yes"
    AC_CHECK_HEADERS([numaif.h numa.h], [], [have_libnuma="no"; break;], [ [/* avoid default includes */] ])
    AS_IF([test x"${have_libnuma}" == x"yes"], [AC_SEARCH_LIBS([numa_available], [numa], [], [have_libnuma="no"])])
    AS_IF([test x"${have_libnuma}" == x"yes"], [
        AC_RUN_IFELSE([dnl
            AC_LANG_PROGRAM(
                [
                    #include <fstream>
                    #include <numa.h>
                    #include <iostream>
                    using namespace std;
                ],
                [ 
                    // first check, numa_available() must return a value >= 0, otherwise any call to libnuma is invalid
                    if(numa_available() < 0) {
                        cerr << "NUMA ERROR: numa_available() returned: " << numa_available() << ", expected a non negative value.\n";
                        return 1;
                    }
                    
                    // second check, the nodes available must be a contiguous sequence in @<:@0,... N). Arbitrary node IDs 
                    // and gaps are simply not supported by the current implementation.
                    struct bitmask* bitmask = numa_get_mems_allowed();
                    int num_nodes = 0;
                    for(int i = 0, end = numa_num_possible_nodes(); i < end; i++){
                        if(numa_bitmask_isbitset(bitmask, i)){
                            if(i != num_nodes){
                                cerr << "NUMA ERROR: NUMA support for arbitrary node IDs is not implemented. Node ID: " << i << ", expected: " << num_nodes << "\n";
                                return 1;
                            }
                            
                            num_nodes++;
                        }
                    }
                    numa_free_nodemask(bitmask);
                    
                    // now that we got that figured out, pass to the caller the actual number of nodes available in the system
                    fstream f(".biscotto", ios::out);
                    f << num_nodes;
                    f.close();
                                                            
                    return 0;
                ]
            )
        ], [conf_numa_num_nodes="$(cat .biscotto)"; rm .biscotto;], [have_libnuma="no"; AC_MSG_WARN([libnuma test failed, check config.log for the string "NUMA ERROR:"])])
    ])
fi


if test x"${have_libnuma}" == x"yes"; then
    AC_MSG_NOTICE([NUMA support enabled...])
    CPPFLAGS="${CPPFLAGS} -DHAVE_NUMA";
    conf_numa_enabled="true"
    info_numa_support="yes, ${conf_numa_num_nodes} node(s) detected";
elif test x"${enable_numa}" == x"yes"; then
    AC_MSG_ERROR([unable to support NUMA in this system]); 
else
    AC_MSG_WARN([libnuma support disabled...])
    conf_numa_enabled="false"
    conf_numa_num_nodes="1";
    info_numa_support="no";
fi
    
#############################################################################
# libevent >2.1.1
for header in "event2/event.h" "event2/thread.h"; do
    AC_CHECK_HEADER([${header}], [],
        [AC_MSG_ERROR([missing prerequisite: this program requires the headers for libevent v2.1.x with support for pthreads]) ],
        [ [/* avoid default includes */] ])
done
AC_SEARCH_LIBS([libevent_global_shutdown], [event_core event], [],
    [ AC_MSG_ERROR([missing prerequisite: this program requires libevent]) ])
AC_SEARCH_LIBS([evthread_use_pthreads], [event_pthreads event], [],
    [ AC_MSG_ERROR([missing prerequisite: this program requires libevent with support for pthreads]) ])

#############################################################################
# Position independent code (PIC)
dnl first argument is the variable with the flags, the second argument is the language
m4_defun([_my_set_pic], [
    m4_pushdef([_FLAGS], [m4_translit([$1], [+], [X])FLAGS]) dnl => CFLAGS, CXXFLAGS
    AC_LANG_PUSH([$1])
    MY_SET_CC_FLAG([_FLAGS], [-fPIC])
    AC_LANG_POP([$1])
    m4_popdef([_FLAGS])
])

#_my_set_pic([C]) # 24/Apr/2020 disable PIC
#_my_set_pic([C++])
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
# Enable one of the compiler sanitizer, by default the address sanitizer (ASAN). Possible values:
# "" => CPPFLAGS += -fsanitize=address
# <value> => CPPFLAGS += -fsanitize=<value>
AC_ARG_ENABLE([sanitize],
	AS_HELP_STRING([--enable-sanitize@<:@=ARG@:>@], [Instruments the code with either the given 
	sanitizer `ARG' or the default address sanitizer (ASAN) when one is not explicitly provided.]),
	[sanitize="${enableval}"],
	[sanitize=""] 
)
if test x"${sanitize}" != x"" -a x"${sanitize}" != x"no" ; then
	if test x"${sanitize}" == x"yes"; then
		sanitize="address"
	fi		
	CPPFLAGS="${CPPFLAGS} -fsanitize=${sanitize}"
	LDFLAGS="${LDFLAGS} -fsanitize=${sanitize}"
fi

#############################################################################
# Test mode
MY_ARG_ENABLE([test],
    [Create a static configuration of the memstore suitable for testing],
    [yes no], [no])
if test x"${enable_test}" == x"yes"; then
    test_mode="true";
    conf_async_num_threads="2";
    conf_aux_counting_tree_capacity_inodes="5";
    conf_aux_counting_tree_capacity_leaves="4";
    conf_bp_max_logical_memory="1<<30"; # 1 Gb
    conf_bp_min_num_pages="4";
    conf_bp_page_size="(1<<12)"; # 4Kb
    conf_crawler_calibrator_tree_height="0";
    conf_memstore_num_segments_per_leaf="4";
    conf_memstore_segment_size="34"; # 1 for the header, 2 for the cached pivot, 31 for the content
    conf_runtime_num_threads="2";
    conf_runtime_txnlist_refresh="1"; # millisecs
    conf_tclist_initial_capacity="4";
    conf_transaction_memory_pool_size="8";
else
    test_mode="false";
    conf_async_num_threads="8"; 
    conf_aux_counting_tree_capacity_inodes="63";
    conf_aux_counting_tree_capacity_leaves="64";
    conf_bp_max_logical_memory=$(perl -e '
        open(my $fh, "<", "/proc/meminfo"); # fetch /proc/meminfo
        while(<$fh>){ 
            if($_ =~ /^MemTotal:\s*(\d+)\s*kB/){ # grep for MemTotal:
                $n = int($1 * 1024 * 0.8); # 80 % of the physical memory
                $mod = $n % (2**21); if($mod > 0){ $n += (2**21) - $mod; }; # round to a multiple of 2MB
                print($n . "ull"); 
             } 
        };
        close($fh); # not really necessary
    ');
    conf_bp_min_num_pages="128";
    conf_bp_page_size="(1<<21)"; # 2Mb
    conf_crawler_calibrator_tree_height="20";
    if test x"${enable_assert}" == x"yes"; then
        # we don't have enough space for 512 segments to fit into a 2 Mb page
        conf_memstore_num_segments_per_leaf="508";
    else
        conf_memstore_num_segments_per_leaf="512";
    fi 
    conf_memstore_segment_size="503"; # 9 words are lost in the header :(
    conf_runtime_num_threads="0";
    conf_runtime_txnlist_refresh="60"; # millisecs
    conf_tclist_initial_capacity="128";
    conf_transaction_memory_pool_size="1<<16";
fi


# Display the message for the maximum size of the buffer pool
if test x"${conf_bp_max_logical_memory}" == x"0"; then
    info_bp_max_size="0 (determined at runtime)";
else
    # e.g. 53930734387ull -> 50.23 GB 
    info_bp_max_size=$(echo "${conf_bp_max_logical_memory}" | perl -e 'chomp($n=<>); $n =~ s/ull//; printf("%.2f GB", eval($n) / (2**30));');
fi
AC_MSG_NOTICE([maximum size for the buffer pool configured to ${info_bp_max_size}]);

#############################################################################
# Support for huge pages
MY_ARG_ENABLE([huge-pages],
    [Enable the support for huge pages (2MB)],
    [yes no], [no])
if test x"${enable_huge_pages}" == x"yes"; then
    conf_huge_pages="true";
    info_huge_pages="yes, up to ${info_bp_max_size}";
else
    conf_huge_pages="false";
    info_huge_pages="no";
fi


#############################################################################
# Profile the execution:
AC_ARG_ENABLE([profile], AS_HELP_STRING([--enable-profile], [Whether to profile the execution [Default: no]]), [CPPFLAGS="${CPPFLAGS} -DHAVE_PROFILER"])

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

# Static configuration
AC_SUBST([test_mode])
AC_SUBST([conf_async_num_threads])
AC_SUBST([conf_aux_counting_tree_capacity_inodes])
AC_SUBST([conf_aux_counting_tree_capacity_leaves])
AC_SUBST([conf_bp_max_logical_memory])
AC_SUBST([conf_bp_min_num_pages])
AC_SUBST([conf_bp_page_size])
AC_SUBST([conf_crawler_calibrator_tree_height])
AC_SUBST([conf_huge_pages])
AC_SUBST([conf_memstore_num_segments_per_leaf])
AC_SUBST([conf_memstore_segment_size])
AC_SUBST([conf_numa_enabled])
AC_SUBST([conf_numa_num_nodes])
AC_SUBST([conf_runtime_num_threads])
AC_SUBST([conf_runtime_txnlist_refresh])
AC_SUBST([conf_tclist_initial_capacity])
AC_SUBST([conf_transaction_memory_pool_size])

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
Enable NUMA support.: ${info_numa_support}
Enable huge pages...: ${info_huge_pages}

Now type 'make -j'
--------------------------------------------------"
