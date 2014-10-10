#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from eslib.procs import RabbitmqMonitor

LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#LOG_FORMAT = ('%(lastname) -10s %(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
#              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

def callback(document):
    print "Received doc of type '%s'." % type(document)
    print document


r = RabbitmqMonitor(
    host = "nets.comperio.no",
    username = "nets",
    password = "nets",
    virtual_host = "dev",
    queue = "default"
)
r.add_callback(callback)

r.start()

try:
    r.wait()  # Will never finish
except KeyboardInterrupt:
    r.stop()
