#!/usr/bin/env python

import eslib.debug as debug
import eslib.time as libtime
import datetime
import eslib.prog

from eslib.Controller import Controller

c = Controller()

mem = debug.get_memory_used()
bss = debug.byte_size_string(mem, 2)

print "Memory used:", bss

then = libtime.ago2date("1w", datetime.datetime.now())
now = datetime.datetime.utcnow()

print "One week ago was:", then

print "Time between then and now:", libtime.duration_string(now - then)
print "Hours in a week:", str(24*7)

print "This program is:", eslib.prog.progname()

eslib.prog.initlogs()
