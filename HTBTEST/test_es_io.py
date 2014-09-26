#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib import Processor
from eslib.procs import ElasticsearchReader
from eslib.procs import ElasticsearchWriter
import json

def listener(document):
    print "*** Writer completed doc with id '%s'" % document["_id"]
    #print json.dumps(document, indent=2)

#def mymethod(document):
#    print "Full name: %s %s" % (document["_source"]["fname"], document["_source"]["lname"])

r = ElasticsearchReader()
r.config.index = "family"

#p = Processor("ppp")
#p.create_connector(mymethod, "myname")
#p.subscribe(r)

w = ElasticsearchWriter()
w.config.index = "family_copy"
#w.config.hosts = ["eslab.comperio.no"]

w.subscribe(r)
w.add_callback(listener)

r.start()
w.wait()
