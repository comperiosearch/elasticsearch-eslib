#!/usr/bin/env python

from eslib.procs import HttpMonitor, FileWriter

h = HttpMonitor() # defaults to localhost port 4000
w = FileWriter()
w.subscribe(h)
h.start()

try:
    h.wait()
except KeyboardInterrupt:
    h.stop()
