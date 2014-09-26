#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

import logging
from eslib2.procs import ElasticsearchReader
from eslib2.procs import RabbitmqWriter
import time, sys

LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

r = ElasticsearchReader()
r.config.index = "family"
#r.config.limit = 2

w = RabbitmqWriter()
w.config.host = "nets.comperio.no"
w.config.username = "nets"
w.config.password = "nets"
w.config.virtual_host = "dev"
w.config.exchange = "test_exchange"
w.config.routing_key = "default"
w.config.queue = "default"

w.subscribe(r)

w.start()
w.wait()
