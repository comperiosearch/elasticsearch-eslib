from __future__ import absolute_import

__author__ = 'Hans Terje Bakke'

# TODO: Logging

import time
from ..Configurable import Configurable


class Controller(Configurable):
    def __init__(self, **kwargs):
        super(Controller, self).__init__(**kwargs)

        self._running = False
        self._registered_procs = []

    #region Debugging

    def DUMP(self):
        fmt = "%-15s %-15s %-7s %-8s %-9s %-7s %-9s %-4s %-4s %-5s %-5s"
        print fmt % ("Type", "Name", "Running", "Stopping", "Accepting", "Aborted", "Suspended", "Ins", "Outs", "Keep", "Count")
        for item in self._registered_procs:
            producers = 0
            subscribers = 0
            for p in item.connectors.itervalues():
                producers += len(p.connections)
            for p in item.sockets.itervalues():
                subscribers += len(p.connections)
            print fmt % (item.__class__.__name__, item.name, item.running, item.stopping, item.accepting, item.aborted, item.suspended, producers, subscribers, item.keepalive, item.count)

    def register(self, *procs):
        "Register a processor as part of the controller. (For debugging.)"
        for proc in procs:
            self._registered_procs.append(proc)
        return len(procs)

    def unregister(self, *procs):
        "Unregister a processor as part of the controller. (For debugging.)"
        registered = 0
        for proc in procs:
            if proc in self._registered_procs:
                self._registered_procs.append(proc)
                registered += 1
        return registered

    #endregion Debugging

    #region Controller management commands

    def run(self, wait=False):
        "Start running the controller itself. (Not the document processors.)"
        self._running = True
        if wait:
            while self._running:
                time.sleep(0.1)
        return True  # Started; and potentially stopped again, in case we waited here

    def shutdown(self):
        "Shut down the controller, including document processors."
        if not self._running:
            return False  # Not shut down; was not running
        self._stop()
        self._running = False
        return True  # Shut down successfully

    def wait(self):
        "Wait until controller is shut down."
        if not self._running:
            return False  # Not shut down; was not running
        while self._running:
            time.sleep(0.1)
        return True  # Shut down successfully

    #endregion Controller management commands

    #region Pipeline management commands

    def start(self):
        return self.on_start()

    def restart(self):
        return self.on_restart()

    def stop(self):
        return self._stop()

    def _stop(self):
        return self.on_stop()

    def abort(self):
        return self.on_abort()

    def suspend(self):
        return self.on_suspend()

    def resume(self):
        return self.on_resume()

    def configure(self, config, restart=True):
        return self.on_configure(config)

    def status(self, *procs):
        return self.on_status()

    #endregion Pipeline management commands

    #region Event handlers for override

    def on_status(self):
        return "none"
    def on_start(self):
        return False
    def on_restart(self):
        return False
    def on_stop(self):
        return False
    def on_abort(self):
        return False
    def on_suspend(self):
        return False
    def on_resume(self):
        return False
    def on_configure(self, config):
        return False

    #endregion Event handlers for override


