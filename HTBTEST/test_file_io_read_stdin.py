#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from eslib.procs import FileReader


#for line in sys.stdin:
#    print "LINE=%s" % line
#print "DONE"
#sys.exit()

def callback(doc):
    print "INCOMING: %s" % doc

r = FileReader(raw_lines=True)
r.add_callback(callback)

r.start()
try:
    r.wait()
except KeyboardInterrupt:
    r.stop()
    print "STOPPED DUE TO KBDINT"
print "DONE"
