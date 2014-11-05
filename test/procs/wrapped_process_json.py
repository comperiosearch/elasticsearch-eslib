#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, select, json

def send(s):
    print json.dumps({"inner": s})


try:
    while True:
        r,w,e = select.select([sys.stdin],[],[],0)
        if r:
            line = sys.stdin.readline()
            if line:
                dd = json.loads(line)
                s = dd.get("outer")
                if s:
                    send("echo: %s" % s)
            else:
                send("stdin was hung up")
                break
except KeyboardInterrupt:
    send("interrupted")
send("finished")
