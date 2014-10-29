#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from __future__ import absolute_import

from eslib.procs import FileReader

def callback(doc):
    print "-------\n", doc

r = FileReader(raw_lines=True)
r.config.filenames = ["files/a.txt", "files/b.txt", "files/c.txt"]
r.config.document_per_file = True
r.config.comment_prefix = "rem"
r.add_callback(callback)

r.start()
r.wait()
