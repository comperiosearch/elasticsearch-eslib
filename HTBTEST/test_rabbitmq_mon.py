#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

import logging
from eslib2.procs import RabbitmqWriter, RabbitmqMonitor
import time, sys

LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#LOG_FORMAT = ('%(lastname) -10s %(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
#              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

def callback(document):
    print "Received doc of type '%s'." % type(document)
    print document

r = RabbitmqMonitor()
r.config.host = "nets.comperio.no"
r.config.username = "nets"
r.config.password = "nets"
r.config.virtual_host = "dev"
r.config.queue = "default"

r.add_callback(callback)

r.start()

#time.sleep(10000)
r.wait()  # Will never finish
