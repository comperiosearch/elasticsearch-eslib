__author__ = 'Hans Terje Bakke'

# TODO: Logging

from .Controller import Controller
from ..procs.HttpMonitor import HttpMonitor
import json


class HttpController(Controller):

    def __init__(self, **kwargs):
        super(HttpController, self).__init__(**kwargs)

        self.config.set_default(
            mgmt_host = "localhost",
            mgmt_port = "4444"
        )

        self._receiver = None
        self._routes = {}

        # Add default management routes to functions
        self.add_route("POST"    , "shutdown" , self._mgmt_shutdown)
        self.add_route("GET"     , "status"   , self._mgmt_status)
        self.add_route("POST"    , "configure", self._mgmt_configure)
        self.add_route("GET|POST", "start"    , self._mgmt_start)
        self.add_route("GET|POST", "restart"  , self._mgmt_restart)
        self.add_route("GET|POST", "stop"     , self._mgmt_stop)
        self.add_route("GET|POST", "abort"    , self._mgmt_abort)
        self.add_route("GET|POST", "suspend"  , self._mgmt_suspend)
        self.add_route("GET|POST", "resume"   , self._mgmt_resume)

        self._receiver = HttpMonitor(name="receiver", hook=self._hook, host=self.config.mgmt_host, port=self.config.mgmt_port)
        self.register(self._receiver)

    #region Debugging

    def DUMP_ROUTES(self):
        for route in self._routes.keys():
            print route

    #endregion Debugging

    #region Controller management overrides

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

    def shutdown(self, wait=True):
        "Shut down the controller, including document processors."
        if not self._running:
            return False  # Not shut down; was not running
        if not self._stop():
            return False  # Not stopped properly

        # Stop the receiver
        self._receiver.stop()
        # Note: We cannot wait for this with HttpMonitor, because it will enter a deadlock.
        # (It will not finish unless the request has finished, and the request will not finish unless
        # we return from here.)
        if wait:
            self._receiver.wait()
        self._running = False
        return True

    def wait(self):
        "Wait until controller is shut down."
        if not self._running:
            return
        self._receiver.wait()

    #endregion Controller management overrides

    #region Routing and Route management

    def add_route(self, verbs, path, func):
        "Add a route from an incoming REST request to a function. Multiple verbs can be specified with pipe character."
        if path and path.startswith("/"):
            path = path [1:]
        for verb in verbs.split("|"):
            key = "%s_%s" % (verb.strip().upper(), path.lower())
            self._routes[key] = func

    def _hook(self, verb, path, data, format="application/json"):
        print "VERB=[%s], PATH=[%s], DATA=[%s]" % (verb, path, data)

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

    #endregion Routing and Route management

    #region Command handlers

    def _mgmt_shutdown(self, payload, *args, **kwargs):
        if self.shutdown(wait=False):
            return {"message": "Shutting down."}
        else:
            return {"warning": "Not shut down."}

    def _mgmt_status(self, payload, *args, **kwargs):
        return {"status": self.status(*args)}

    def _mgmt_start(self, payload, *args, **kwargs):
        if self.start():
            return {"message": "Processing started."}
        else:
            return {"warning": "Processing was not started."}

    def _mgmt_restart(self, payload, *args, **kwargs):
        if self.restart():
            return {"message": "Processing restarted."}
        else:
            return {"warning": "Processing not started."}

    def _mgmt_stop(self, payload, *args, **kwargs):
        if self.stop():
            return {"message": "Processing stopped."}
        else:
            return {"warning": "Processing was not stopped."}

    def _mgmt_abort(self, payload, *args, **kwargs):
        if self.abort():
            return {"message": "Processing aborted."}
        else:
            return {"warning": "Processing was not aborted."}

    def _mgmt_suspend(self, payload, *args, **kwargs):
        if self.suspend():
            return {"message": "Processing suspended."}
        else:
            return {"warning": "Processing was not suspended."}

    def _mgmt_resume(self, payload, *args, **kwargs):
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

    #endregion Command handlers