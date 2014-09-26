#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from eslib.procs import RabbitmqWriter

LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

w = RabbitmqWriter()
w.config.host = "nets.comperio.no"
w.config.username = "nets"
w.config.password = "nets"
w.config.virtual_host = "dev"
w.config.exchange = "test_exchange"
w.config.routing_key = "default"
w.config.queue = "default"

w.start()
w.wait()
