from __future__ import absolute_import

# TODO: Logging

from .Configurable import Configurable
from .procs import HttpMonitor
import json
import time


class Controller(Configurable):
    def __init__(self, **kwargs):
        self.super(Controller, self).__init__(**kwargs)

        self._running = False
        self._registered_procs = []

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

    def register(self, proc):
        "Register a processor as part of the controller. (For debugging.)"
        self._registered_procs.append(proc)

    def unregister(self, proc):
        "Unregister a processor as part of the controller. (For debugging.)"
        if proc in self._registered_procs:
            self._registered_procs.append(proc)

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

    def start(self):
        self.on_start()

    def restart(self):
        self.on_restart()

    def stop(self):
        self.on_stop()

    def _stop(self):
        pass

    def abort(self):
        self.on_abort()

    def suspend(self):
        self.on_suspend()

    def resume(self):
        self.on_resume()

    def configure(self, config, restart=True):
        self.on_configure(config)

    def status(self, *procs):
        pass  # TODO

    def on_status(self):
        pass
    def on_start(self):
        pass
    def on_restart(self):
        pass
    def on_stop(self):
        pass
    def on_abort(self):
        pass
    def on_suspend(self):
        pass
    def on_resume(self):
        pass
    def on_configure(self, config):
        pass

class ManagedController(Controller):

    def __init__(self, **kwargs):
        self.super(ManagedController, self).__init__(**kwargs)

        self.config.set_default(
            mgmt_host = "localhost",
            mgmt_port = "4444"
        )

        self._receiver = None
        self._routes = {}

        # Add default management routes to functions
        self.add_mgmt_route("POST"    , "/shutdown" , self._mgmt_shutdown)
        self.add_mgmt_route("GET"     , "/status"   , self._mgmt_status)
        self.add_mgmt_route("POST"    , "/configure", self._mgmt_configure)
        self.add_mgmt_route("GET|POST", "/start"    , self._mgmtt_start)
        self.add_mgmt_route("GET|POST", "/stop"     , self._mgmt_stop)
        self.add_mgmt_route("GET|POST", "/abort"    , self._mgmt_abort)

        self._receiver = HttpMonitor("receiver", hook=self._hook, host=self.config.mgmt_host, port=self.config.mgmt_port)
        self.register(self._receiver)

    def DUMP_ROUTES(self):
        for route in self._routes.keys():
            print route

    def run(self, wait=False):
        "Start running the controller itself. (Not the document processors.)"
        if self._running:
            return False  # Not started; was already running, or failed
        self._receiver.config.host = self.config.mgmt_host
        self._receiver.config.port = self.config.mgmt_port
        try:
            self._receiver.start()
            self._running = True
        except Exception as e:
            return False  # Not started; failed

        if wait:
            self._receiver.wait()
        return True  # Started; and potentially stopped again, in case we waited here

    def shutdown(self):
        "Shut down the controller, including document processors."
        if not self._running:
            return False  # Not shut down; was not running
        self._stop()
        self._receiver.stop()
        self._receiver.wait()
        self._running = False
        return True

    def wait(self):
        "Wait until controller is shut down."
        if not self._running:
            return
        self._receiver.wait()

    def add_route(self, verbs, path, func):
        "Add a route from an incoming REST request to a function. Multiple verbs can be specified with pipe character."
        for verb in verbs.split("|"):
            key = "%s_%s" % (verb.strip().upper(), path.lower())
            self._routes[key] = func

    def _hook(self, verb, path, data, format="application/json"):
        key = "%s_%s" % (verb.upper(), path.lower())

        func = self._routes.get(key)
        # TODO: BETTER MATCHING AND PARSING INTO *args and **kwargs
        args = []
        kwargs = {}

        if not func:
            return {"error": "No route for '%s'." % key}
        payload = None
        if data and format == "application/json":
            payload = json.loads(data)
        else:
            payload = data
        try:
            res = func(payload, args, kwargs)
            return res
        except Exception as e:
            return {"error": "Unhandled exception: %s: %s" % (e.__class__.__name__, e)}


    def _mgmt_shutdown(self):
        # TODO: Make this shutdown version non-blocking
        if self._receiver.shutdown():
            return {"message": "Shut down."}
        else:
            return {"warning": "Not shut down."}

    def _mgmt_status(self):
        return {"status": self.status()}

    def _mgmt_start(self):
        if self.start():
            return {"message": "Processing started."}
        else:
            return {"warning": "Processing was not started."}

    def _mgmt_restart(self):
        if self.restart():
            return {"message": "Processing restarted."}
        else:
            return {"warning": "Processing not started."}

    def _mgmt_stop(self):
        if self.stop():
            return {"message": "Processing stopped."}
        else:
            return {"warning": "Processing was not stopped."}

    def _mgmt_abort(self):
        if self.abort():
            return {"message": "Processing aborted."}
        else:
            return {"warning": "Processing was not aborted."}

    def _mgmt_suspend(self):
        if self.suspend():
            return {"message": "Processing suspended."}
        else:
            return {"warning": "Processing was not suspended."}

    def _mgmt_resume(self):
        if self.resume():
            return {"message": "Processing resumed."}
        else:
            return {"warning": "Processing was not resumed."}

    def _mgmt_configure(self, payload, *args, **kwargs):
        if self.configure(payload):
            if self.restart():
                return {"message": "Processing (re)configured and (re)started."}
            else:
                return {"warning": "Processing was not (re)started and (re)configuring."}
        else:
            return {"warning": "Processing was not (re)configured."}
