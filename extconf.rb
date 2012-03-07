require "mkmf"

dir_config('frontbase')
have_library('FBCAccess')
create_makefile("frontbase")

