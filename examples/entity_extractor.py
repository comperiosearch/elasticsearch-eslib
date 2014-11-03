#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib.procs.FileReader import FileReader
from eslib.procs.EntityExtractor import EntityExtractor


# For this example to run you should set up a simple echo server listening on localhost:3000,
# or modify the input so that it can resolve some other URI


def listener(document):
    print (document)

r = FileReader()
p = EntityExtractor()

r.config.filenames = r"resources/tweet.json"
entities = [{
                "category": "location",
                "name": "place",
                "match": [
                    { "type": "exact", "value": "Convention" }
                    #{ "type": "iprange", "value": "81.27.32.186/16" }
                ]
            },
            {
                "category": "agent",
                "name": "user",
                "match": [
                    { "type": "exact", "value": "Jacob" }
                    #{ "type": "iprange", "value": "81.27.32.186/16" }
                ]
            },
            {
                "category": "agent",
                "name": "user",
                "match": [
                    { "type": "exact", "value": "stellachuuuuu" }
                    #{ "type": "iprange", "value": "81.27.32.186/16" }
                ]
            }
            ]

p.config.entities = entities
p.subscribe(r)


p.add_callback(listener, socket_name="output")
r.start()

try:
    r.wait()
except KeyboardInterrupt:
    r.stop()

