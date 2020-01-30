# Set the current compiler option if the given flag is supported
# serial 1

# MY_SET_CC_FLAG(var_cflags, flag)
# -------------------------------------------
#
# Test whether <flag> is accepted by the current compiler. If so, it appends
# it to the shell variable <var_cflags>.
# The macro depends on AX_CHECK_COMPILE_FLAG from the Autoarchive.
AC_DEFUN([MY_SET_CC_FLAG], [ AX_CHECK_COMPILE_FLAG([$2], [AS_VAR_APPEND([$1], " $2")]) ])