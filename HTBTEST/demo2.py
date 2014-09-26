#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib.procs import RabbitmqWriter

w = RabbitmqWriter()
w.config.host = "nets.comperio.no"
w.config.username = "nets"
w.config.password = "nets"
w.config.virtual_host = "dev"
w.config.exchange = "test_exchange"
w.config.routing_key = "default"
w.config.queue = "default"

w.DUMP_QUEUES()

w.start()

w.put("hallo")
w.put("hei på deg HT")
