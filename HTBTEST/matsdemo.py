#!/usr/bin/env python

from eslib2 import *
from eslib2.procs import *

r = ElasticsearchReader()
r.config.hosts = [ "eslab.comperio.no" ]
r.config.index = "htbtest"


w2 = ElasticsearchWriter()
w2.config.hosts = [ "eslab.comperio.no" ]
w2.config.index = "htbtest2"

w = FileWriter()
#w.config.filename = "myfile.txt"

w.subscribe(r)
w2.subscribe(r)

#def my_spy(doc):
#    print "doc id is: %s" % doc["_id"]

#p = Processor("my_spy")
#p.create_connector(my_spy, "input")
#p.subscribe(r)#, socket_name="output", connector_name="input")

r.start()
