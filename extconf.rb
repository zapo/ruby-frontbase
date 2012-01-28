require "mkmf"

dir_config('frontbase')

$CPPFLAGS = "-I/usr/local/FrontBase/include"
$LDFLAGS = "-L/usr/local/FrontBase/lib"
$libs = " -lFBCAccess "
create_makefile("frontbase")

