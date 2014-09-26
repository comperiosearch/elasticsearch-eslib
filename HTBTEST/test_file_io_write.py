#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib.procs import ElasticsearchReader
from eslib.procs import FileWriter

r = ElasticsearchReader()
r.config.index = "family"
r.config.limit = 2

w = FileWriter()
#w.config.filename = "out.json"

w.subscribe(r)

r.start()
w.wait()

print r.get_open_contexts()  # Is 0... which is kind of unexpected as long as the release scroll bug exists... shouldnt this be 1?

