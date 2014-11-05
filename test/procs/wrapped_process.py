#!/usr/bin/env python

import time, sys, signal
from select import select

def _signal_handler(signal, frame):
    print "INNER/RECEIVED SIGNAL: "

signal.signal(signal.SIGINT, _signal_handler)

print "INNER/HERE WE GO"
print "INNER/SOME MORE"
time.sleep(1)
print "INNER/I JUST WOKE UP"

while True:
    r,w,e = select([sys.stdin],[],[],0)
    if r:
        line = sys.stdin.readline()
        line = line.strip()
        if line:
            print "INNER/ECHO:", line
            if line == "*HANGUP*":
                print "INNER/HANGING UP ON *HANGUP* REQUEST"
                break
        else:
            print "INNER/GOOD BYE"
            break

print "INNER/A FINAL WORD"



# TODO: Handle exceptions
# TODO: Handle SIGHUP
