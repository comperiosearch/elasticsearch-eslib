#!/usr/bin/env python

import eslib
import logging
#LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.CRITICAL, format=LOG_FORMAT)

console = logging.StreamHandler()
console.setLevel(logging.TRACE)
console.setFormatter(logging.Formatter("%(levelname) -10s %(message)s"))

proclog = logging.getLogger("proclog")
proclog.setLevel(logging.TRACE)
proclog.addHandler(console)



from eslib.procs import TcpWriter

w = TcpWriter(
    reuse_address = True,
    #hostname="localhost",
    port=4000
)

import time

w.start()
time.sleep(1)
w.put("hei u'\u2013'")

try:
    w.wait()
except KeyboardInterrupt:
    w.stop()
