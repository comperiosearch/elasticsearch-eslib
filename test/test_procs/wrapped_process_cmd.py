#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, codecs


print "INNER/STARTING"

print "INNER/" + u" ".join([codecs.decode(x, "UTF-8") for x in sys.argv[1:]])

print "INNER/EXITING"
