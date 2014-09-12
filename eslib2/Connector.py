# -*- coding: utf-8 -*-

from .Terminal import Terminal
import Queue
import threading
import time


class Connector(Terminal):

    def __init__(self, name, protocol=None, method=None):
        self.sleep = 0.001

        super(Connector, self).__init__(name, protocol)
        self.type = Connector
        self.queue = Queue.Queue()
        self.method = method

        # Execution control status
        self.thread = threading.Thread(target=self._run)
        self.accepting = False
        self.stopping = False
        self.running = False
        self.suspended = False
        self.aborted = False

    #region Queue management

    def _clear(self):
        "Clear the queue."
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()

    def pending(self):
        "Report number of pending items in queue."
        return self.queue.qsize()

    def _process(self):
        "Grab item from queue and call the pre-registered method on it."
        if not self.queue.empty():
            document = self.queue.get_nowait()
            self.queue.task_done()
            if document:
                if self.method:
                    self.method(document)

    def receive(self, document):
        "Put document on the incoming queue for this connector. Called by sockets."
        if self.accepting:
            self.queue.put(document)  # Infinite queue, so it should never block

    #endregion Queue management

    #region Operation management

    def _run(self):
        while self.running:
            if self.sleep:
                time.sleep(self.sleep)
            if self.stopping and (self.suspended or self.queue.empty()):
                # Notify owner that we are finished stopping
                self.owner.production_stopped()
                # Now we can finally stop
                self.stopping = False
                self.running = False
            elif not self.suspended:
                self._process()

        # Clean out the queue (in case we just aborted)
        self._clear()
        self.stopping = False  # In case we were stopping while aborted

    # Note: The reason for the split of run() and accept_incoming():
    #       The entire system should first be accepting data before the individual
    #       components start processing. When processing, a document is passed on
    #       through sockets to listening connectors. If those connectors are not yet
    #       accepting new items on their queues, incoming items will be dropped (i.e.
    #       not put on the queue, and we would potentially lose the first items
    #       during start-up.

    def run(self):
        "Should be called after all connectors in the system accept incoming data."
        if self.running:
            raise Exception("Connector is already running.")
        if not self.accepting:
            raise Exception("Connector is not accepting input before call to run(). Call accept_incoming() on all connectors in the system first.")

        self.aborted = False
        self.stopping = False
        self.suspended = False
        self.running = True

        self.thread.start()

    def accept_incoming(self):
        "Should be called for all connectors in the system before processes start running and processing!"
        if self.stopping:
            raise Exception("Connector is stopping. Refusing to accept new incoming again until fully stopped.")
        self.accepting = True

    def stop(self):
        self.accepting = False
        self.stopping = True  # We must wait for items in the queue to be processed before we finally stop running
        self.thread.join()

    def abort(self):
        self.aborted = True
        self.accepting = False
        self.running = False  # Run loop will stop immediately

    def suspend(self):
        self.suspended = True

    def resume(self):
        self.suspended = False

    #endregion Operation management
