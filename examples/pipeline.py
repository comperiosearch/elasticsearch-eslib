#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib.procs.CLIReader import CLIReader
from eslib.procs.JSONArraysToURLRequest import JSONArraysToURLRequest
from eslib.procs.WebGetter import WebGetter


# For this example to run you should set up a simple echo server listening on localhost:3000,
# or modify the input so that it can resolve some other URI


def listener(document):
    print (document)

r = CLIReader()
p = JSONArraysToURLRequest()
w = WebGetter()

r.config.cmd = r"cat resources/output.json"
r.config.interval = 120

p.config.prepend = "http://localhost:3000/"
p.subscribe(r)




w.subscribe(p)
w.config.domains = [{"domain_id": "http://localhost", "url_prefix":"http://localhost"}]
w.add_callback(listener, socket_name="output")
r.start()

try:
    r.wait()
except KeyboardInterrupt:
    r.stop()
