require "mkmf"

dir_config('frontbase')
abort 'FBCAccess cant be found' unless have_library('FBCAccess')
create_makefile("frontbase")

