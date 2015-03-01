#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, sys, signal
from select import select

#region Signal handling

def _handler_SIGINT(signal, frame):
    print "INNER/RECEIVED SIGINT -- ignoring"

def _handler_SIGTERM(signal, frame):
    global running
    print "INNER/RECEIVED SIGTERM -- terminating"
    running = False

def _handler_SIGHUP(signal, frame):
    print "INNER/RECEIVED SIGHUP -- ignoring"

signal.signal(signal.SIGINT , _handler_SIGINT )
signal.signal(signal.SIGTERM, _handler_SIGTERM)
signal.signal(signal.SIGHUP , _handler_SIGHUP )

#endregion Signal handling

running = True

print "INNER/STARTING"

while running:
    r,w,e = select([sys.stdin],[],[],0)
    if r:
        line = sys.stdin.readline()
        line = line.strip()
        if line:
            print "INNER/ECHO:", line
            if line == "*HANGUP*":
                print "INNER/HANGING UP ON *HANGUP* REQUEST"
                running = False
            elif line == "*RAISE*":
                raise Exception("INNER/RAISED EXCEPTION UPON *RAISE* REQUEST")
        else:
            print "INNER/STDIN WAS HUNG UP -- GOOD BYE"
            running = False

print "INNER/EXITING"
