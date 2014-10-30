#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv, codecs

uline = u"ole,kristian,villabø"
raw_line = codecs.encode(uline, "UTF-8")
csvrow = csv.reader([raw_line], delimiter=",").next()
ucsvrow = [codecs.decode(x, "UTF-8") for x in csvrow]

print csvrow
