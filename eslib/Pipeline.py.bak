# -*- coding: utf-8 -*-

# ============================================================================
# Multi-threaded pipeline runner.
# ============================================================================

from __future__ import absolute_import

import threading, Queue
import time
def _sleep():
    time.sleep(0.001)


class Pipeline(object):
    "A multi-threaded pipeline runner for document processors for Elasticsearch."

    def __init__(self, name, processors, takes_input = False):
        """
        :arg processors: Assumes a list of instances of :class:`~eslib.PipelineStage`.
        """

        self.processors = processors
        self._takes_input = takes_input
        self._input_queue = Queue.Queue()
        self._input_ended = False
        self._done = False
        # ... well.. if the pipeline has not been started, we could also consider all
        # that is queued (i.e. nothing) to be done and input ended.


    def _handle(self, proc, item):
        processed = proc.process(item)
        proc.output_queue.put(processed)


    def _run(self, proc, feeder):

        proc.start()

        if self._takes_input and not feeder:
            # This is the first processor, and it expects input from the pipeline's own input queue
            while not self._input_ended:
                while not self._input_ended and self._input_queue.empty(): _sleep()
                while not self._input_queue.empty():
                    item = self._input_queue.get()
                    self._handle(proc, item)
        elif not feeder: # This is a generator
            for item in proc.read(None):
                self._handle(proc, item)
        else: # This is a normal processor consuming items from the previous stage, the 'feeder'
            while not feeder.done:
                if not feeder.done and feeder.output_queue.empty(): _sleep()
                # Read all items and process
                while not feeder.output_queue.empty():
                    item = feeder.output_queue.get()
                    self._handle(proc, item)

        proc.finish()

        proc.done = True
        if proc == self.processors[-1]: self._done = True


    def start(self):
        """
        Start all processing stages as own threads. Prepare for external input or start generating
        documents (if the first stage is a generator/reader, i.e. pipeline was created with
        takes_input=False.
        """

        self._input_ended = False
        self._done = False

        # Set up processors
        for proc in self.processors:
            proc.configure()
            proc.load()
            proc.done = False
            proc.output_queue = Queue.Queue()

        # Start all threads
        terminal_thread = None
        for i, proc in enumerate(self.processors):
            feeder = None
            if i > 0: feeder = self.processors[i-1]
            terminal_thread = t = threading.Thread(target=self._run, args=(proc, feeder))
            t.start()

        return terminal_thread


    def end(self):
        """
        Tell the pipeline that there will be no more input, and it should complete once
        the pipeline is empty. This method does NOT block and wait for the pipeline to finish.
        """
        self._input_ended = True

    def poll(self):
        return not self.processors[-1].output_queue.empty()

    def put(self, item): # This expects a json document in for now
        "Add an item to the pipeline. This is only used by pipelines started with takes_input=True."
        if not self._takes_input:
            raise Exception("Trying to put item into a pipeline thiat does not take input.")
        if self._input_ended:
            raise Exception("Trying to put item into pipeline after call to end().")
        self._input_queue.put(item)

    def get(self): # OBS: Blocking
        "Get and remove an item from the pipeline's ouput queue."
        if not self.processors: return None
        return self.processors[-1].output_queue.get()

    def count(self):
        max_count = 0
        for proc in self.processors:
            max_count = max(max_count, proc.output_queue.qsize())

    @property
    def done(self):
        "Reports True if the final pipeline stage has completed all processing."
        return self._done

