#!/usr/bin/env python

from eslib2.procs import *
from eslib2 import *

def my_method(document):
    print document["_id"]

p = Processor("proc")
p.create_connector(my_method)

er1 = ElasticsearchReader()
er1.config.index = "family"

er2 = ElasticsearchReader()
er2.config.index = "karate"

fw = FileWriter()
fw.subscribe(er1)
fw.subscribe(er2)
fw.config.filename = "out.json"
fw.keepalive = True

ew = ElasticsearchWriter()
ew.config.hosts = ["eslab.comperio.no"]
ew.config.index = "htbdemo"
ew.subscribe(er1)
ew.subscribe(er2)
ew.keepalive = True

p.subscribe(ew)

er1.start()
er2.start()

er1.wait()
er2.wait()
fw.stop()
ew.stop()


