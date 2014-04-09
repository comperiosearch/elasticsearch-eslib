#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Show progress, dump number of items that have passed through the stream


import datetime
import eslib.PipelineStage, eslib.time, eslib.debug


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
            memString = eslib.debug.byteSizeString(eslib.debug.getMemoryUsed())
            self.console.debug("count: %7d, duration: %10s, memory: %10s" % (self.count, durationString, memString))

        yield line # .. so it will be written to output


    def finish(self):
        durationString = eslib.time.durationString(self.elapsed())
        memString = eslib.debug.byteSizeString(eslib.debug.getMemoryUsed())
        self.console.debug("count: %7d, duration: %10s, memory: %10s (finished)" % (self.count, durationString, memString))


# ============================================================================
# For running as a script
# ============================================================================

import argparse, sys
from eslib.prog import progname

def main():
    help_f = "How often should progress get printed. Default: 1000 (i.e. per 1000 items.)"
    parser = argparse.ArgumentParser(usage="\n %(prog)s [-f frequency]")
    parser._actions[0].help = argparse.SUPPRESS
    parser.add_argument("-f", "--frequency", type=int, default=1000, help=help_f)
    parser.add_argument(      "--terminal" , action="store_true")
    parser.add_argument(      "--name"     , help="Process name.", default=None)

    args = parser.parse_args()

    # Set up and run this processor
    dp = ShowProgress(args.name or progname())
    dp.frequency = args.frequency
    dp.terminal = args.terminal

    dp.run()


if __name__ == "__main__": main()
