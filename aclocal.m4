#############################################################################
# Include all m4 files from ./build-aux/m4
m4_pushdef([DIR_BUNDLE_M4], [[build-aux/m4]]) 
m4_foreach_w([i], m4_esyscmd_s([ls ']DIR_BUNDLE_M4[' | egrep '*.m4$']), [m4_include(DIR_BUNDLE_M4/i)])
m4_popdef([DIR_BUNDLE_M4]) 