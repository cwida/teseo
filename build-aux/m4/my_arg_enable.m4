# Commodity wrapper for AC_ARG_ENABLE to enable/disable options.
# There should be no external macro dependencies.
# serial 2

# _MY_SHELL_TEST_NE(value)
# -------------------------------------------
#
# Wraps <value> inside the shell construct `test x"<_MY_OPTION_CURRENT>" != x"<value>"'
# It assumes the macro _MY_OPTION_CURRENT already exists
AC_DEFUN([_MY_SHELL_TEST_NE], [[test x"]_MY_OPTION_CURRENT[" != x"$1"]])

# _MY_VALIDATE_OPTIONS(option_name, variable, list_options)
# -------------------------------------------
#
# Check whether <variable> contains one of the options of the value in
# <list_options>, otherwise it terminates raising an error.
AC_DEFUN([_MY_VALIDATE_OPTIONS], [dnl
  m4_pushdef([_MY_OPTION_CURRENT], [[$2]])dnl
if m4_map_sep([_MY_SHELL_TEST_NE], [ && ],  m4_split([$3])); then
  AC_MSG_ERROR([invalid value for the parameter $1. Possible choices are: m4_map_sep([m4_echo], [, ], m4_split([$3])). Given value: '$2'])
fi
  m4_popdef([_MY_OPTION_CURRENT])dnl
])

# MY_VALIDATE_OPTIONS(option_name, variable, list_options)
# -------------------------------------------
#
# Check whether <variable> contains one of the options of the value in
# <list_options>, otherwise it terminates raising an error. The test is
# performed immediately after the command line argument have been parsed
AC_DEFUN([MY_VALIDATE_OPTIONS], [dnl
  m4_divert_push([INIT_PREPARE])dnl
  _MY_VALIDATE_OPTIONS([$1], [$2], [$3])dnl
  m4_divert_pop([INIT_PREPARE])dnl
])

# _MY_PRINT_CHOICE(arg)
# -------------------------------------------
#
# Echo the given argument
AC_DEFUN([_MY_PRINT_CHOICE], [$1])

# MY_ARG_ENABLE(option_name, help_description, list_options, default)
# -------------------------------------------
#
# Wrapper for AC_ARG_ENABLE. Advertises the option --enable-<option_name> and validates
# the input choice among the possibilities <list_options>. If the user does not
# provide any value, it sets enable_<option_name> to <default>
AC_DEFUN([MY_ARG_ENABLE], [dnl
  dnl Set the default value first
  m4_pushdef([enable_variable], [enable_$1])
  m4_divert_push([DEFAULTS])
  AS_VAR_SET([enable_variable], ["$4"])
  m4_divert_pop([DEFAULTS])
  m4_pushdef([_MY_POSSIBLE_CHOICES], [m4_map_sep([_MY_PRINT_CHOICE], [, ], m4_split([$3]))])
  AC_ARG_ENABLE([$1], AS_HELP_STRING([--m4_if([$4], [yes], [disable], [enable])-$1], 
    [$2. Possible choices are: _MY_POSSIBLE_CHOICES. @<:@Default: $4@:>@]))    
  m4_popdef([_MY_POSSIBLE_CHOICES])
  dnl Finally, validate the option given from the user
  MY_VALIDATE_OPTIONS([--enable-$1], ${enable_variable}, [$3])
  m4_popdef([enable_variable])
])
