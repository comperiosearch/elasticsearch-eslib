#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

# Show progress, dump number of items that have passed through the stream


import datetime
import eslib.PipelineStage, eslib.time


class ShowProgress(eslib.PipelineStage):

    def __init__(self, name):
        eslib.PipelineStage.__init__(self, name)

        self.started = datetime.datetime.utcnow() # Though not really started yet... just to prevent potential errors
        self.count = 0

        self.DEBUG = True # This is a debug stage, by nature


    def elapsed(self):
        return datetime.datetime.utcnow() - self.started


    def start(self):
        self.started = datetime.datetime.utcnow()
        self.count = 0


    def convert(self, line):
        return line # This is a pure pass-through


    def process(self, line):
        self.count += 1
        if self.frequency and self.count % self.frequency == 0:
            durationString = eslib.time.durationString(self.elapsed())
            self.dout("count: %7d, duration: %10s" % (self.count, durationString))
        return line # .. so it will be written to output


    def finish(self):
        durationString = eslib.time.durationString(self.elapsed())
        self.dout("count: %7d, duration: %10s (finished)" % (self.count, durationString))


# ============================================================================
# For running as a script
# ============================================================================

import sys, getopt
from eslib.prog import progname


OUT = sys.stderr


def usage(err = None, rich= False):
    if err:
        print >>OUT, "Argument error: %s" % err

    p = progname()
    print >>OUT, "Usage:"
    print >>OUT, "  %s -h" % p
    print >>OUT, "  %s [-f <frequency>] [--terminal]" % p

    if rich:
        pass

    if err:
        sys.exit(-1)
    else:
        sys.exit(0)


def main():

    # Default values
    frequency = 1000
    terminal = False

    # Parse command line input
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], ':s:f:t:h', ["terminal"])
    except:
        usage()
    for (o, a) in optlist:
        if   o == "-h": usage(rich=True)
        elif o == "-f": frequency = int(a)
        elif o == --"terminal" : terminal = True
    filenames = args

    # Set up and run this processor
    dp = ShowProgress(progname())
    dp.frequency = frequency
    dp.terminal = terminal

    dp.run()


if __name__ == "__main__": main()

