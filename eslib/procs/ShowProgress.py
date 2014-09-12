#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Show progress, dump number of items that have passed through the stream


import datetime
import eslib.PipelineStage, eslib.time, eslib.debug


class ShowProgress(eslib.PipelineStage):

    def __init__(self, name):
        super().__init__(name)

        self.started = datetime.datetime.utcnow() # Though not really started yet... just to prevent potential errors
        self.count = 0


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
            self.print("count: %7d, duration: %10s, memory: %10s" % (self.count, durationString, memString))

        yield line # .. so it will be written to output


    def finish(self):
        durationString = eslib.time.durationString(self.elapsed())
        memString = eslib.debug.byteSizeString(eslib.debug.getMemoryUsed())
        self.print("count: %7d, duration: %10s, memory: %10s (finished)" % (self.count, durationString, memString))
