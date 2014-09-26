#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib2.procs import FileReader

def callback(doc):
    print "-------\n", doc

r = FileReader()
r.config.filenames = ["a.txt", "b.txt", "c.txt"]
r.config.document_per_file = True
r.config.comment_prefix = "rem"
r.config.raw_lines = False
r.add_callback(callback)

r.start()
r.wait()
