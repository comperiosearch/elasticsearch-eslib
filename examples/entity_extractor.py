#!/usr/bin/env python
# -*- coding: utf-8 -*-


from eslib.procs import FileReader, FileWriter, EntityExtractor

def listener(document):
    print document["_source"]["extracted"]

entities = [
    {
        "category": "location",
        "name": "place",
        "match": [
            { "type": "exact", "pattern": "Convention" }
            #{ "type": "iprange", "value": "81.27.32.186/16" }
        ]
    },
    {
        "category": "agent",
        "name": "user",
        "match": [
            { "type": "exact", "pattern": "Jacob" }
            #{ "type": "iprange", "value": "81.27.32.186/16" }
        ]
    },
    {
        "category": "agent",
        "name": "user",
        "match": [
            { "type": "exact", "pattern": "stellachuuuuu" }
            #{ "type": "iprange", "value": "81.27.32.186/16" }
        ]
    }
]


r = FileReader(filename = "resources/tweet.json")
p = EntityExtractor(fields=["text"], target="extracted", entities=entities)
w = FileWriter()

p.subscribe(r)
w.subscribe(p, "entities")

r.start()
w.wait() # Will finish once the reader is finished.
