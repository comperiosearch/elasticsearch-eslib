#!/usr/bin/env python

from eslib import *
from eslib.procs import *


r = ElasticsearchReader()
r.config.index = "family"

#w = FileWriter()

#w2 = ElasticsearchWriter()
#w2.config.index="family3"

#w.subscribe(r)
#w2.subscribe(r)

#=============

class MyProcessor(Processor):
    def __init__(self, name=None):
        super(MyProcessor, self).__init__(name)
        self.create_connector(self.myproc, "input")
        self.create_socket("id", "docid")
        self.create_socket("output", "esdoc")

    def myproc(self, document):
        self.sockets["id"].send(document["_id"])
        self.sockets["output"].send(document)



def mycallback(document):
    print document

p = MyProcessor()
p.subscribe(r)

p.add_callback(mycallback, "output")

#============

r.start()


