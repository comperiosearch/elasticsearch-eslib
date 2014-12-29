__author__ = 'Hans Terje Bakke'

# TODO: Logging

from .Service import Service
from ..procs.HttpMonitor import HttpMonitor
import json, requests


class HttpService(Service):
    """
    Common static config:
        name
        mgmt_endpoint
        mgmt_host

    Communication with manager:

        POST register
            name
            callback
        =>
            session_token
            assigned_port

        POST unregister

    Callback interface expects the following messages:

        GET  info
        POST register     # External debug command to register with the manager
        POST unregister
        POST shutdown
        GET  status
        POST update       # causes (re)start
        POST start
        POST stop
        POST abort
        POST suspend
        POST resume
    """

    config_keys = []

    def __init__(self, **kwargs):
        super(HttpService, self).__init__(**kwargs)

        self.config.set_default(
            # A management server we can register with, that will manage this process through the 'mgmt_endpoint'
            mgmt_host     = None,
            # The host:port endpoint where this service will listen for management commands
            mgmt_endpoint = "localhost:4444"
        )

        self._receiver = None
        self._mgmt_endpoint = None
        self._routes = {}

        # Add default management routes to functions
        self.add_route("GET"     , "info"      , self._mgmt_info)
        self.add_route("GET"     , "help"      , self._mgmt_help)
        self.add_route("GET"     , "status"    , self._mgmt_status)
        self.add_route("GET|POST", "register"  , self._mgmt_register)
        self.add_route("GET|POST", "unregister", self._mgmt_unregister)
        self.add_route("POST"    , "shutdown"  , self._mgmt_shutdown)
        self.add_route("POST"    , "update"    , self._mgmt_update)
        self.add_route("GET|POST", "start"     , self._mgmt_start)
        self.add_route("GET|POST", "restart"   , self._mgmt_restart)
        self.add_route("GET|POST", "stop"      , self._mgmt_stop)
        self.add_route("GET|POST", "abort"     , self._mgmt_abort)
        self.add_route("GET|POST", "suspend"   , self._mgmt_suspend)
        self.add_route("GET|POST", "resume"    , self._mgmt_resume)

        self._receiver = HttpMonitor(name="receiver", hook=self._hook)
        self.register_procs(self._receiver)

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

        self.on_setup()

        # Set the host address
        a = self.config.mgmt_endpoint.split(":")
        if len(a) > 0:
            self._receiver.config.host = a[0]
        if len(a) == 2:
            try:
                self._receiver.config.port = int(a[1])
            except Exception as e:
                return False  # Port was not an int
        else:
            self._receiver.config.port = self._assigned_port

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

    #region Registration with remote management service

    def register(self):
        if not self.config.mgmt_host:
            return False

        # TODO: try/except/log
        a = self.config.mgmt_endpoint.split(":")
        host = a[0]
        port = 0 if len(a) != 2 else int(a[1])
        # If port number is missing in the callback, a number will be assigned by the manager
        req = json.dumps({'id': self.name, 'host': host, 'port': port, "config_keys": self.config_keys})
        res = requests.post("http://%s/register" % self.config.mgmt_host, data=req, headers={'content-type': 'application/json'}) #, auth=('user', '*****'))
        jres = json.loads(res.content)

        err = jres.get("error")
        if err:
            self.log.error("Service registration failed: %s" % err)
            self._mgmt_endpoint = None
        else:
            # Use host and port returned from manager
            self._mgmt_endpoint = "%s:%d" % (jres["host"], jres["port"])

        #host, port = self.config.mgmt_endpoint.split(":")
        #req = json.dumps({'id': self.name, 'host': host, "port": port, "config_keys": self.config_keys})
        #res = requests.post("http://%s/register_service" % self.config.mgmt_host, data=req, headers={'content-type': 'application/json'}) #, auth=('user', '*****'))
        #jres = json.loads(res.content)

        return True

    def unregister(self):
        if not self.config.mgmt_host:
            return False
        # TODO: try/except/log
        req = json.dumps({'id': self.name})
        res = requests.delete("http://%s/unregister" % self.config.mgmt_host, data=req, headers={'content-type': 'application/json'}) #, auth=('user', '*****'))
        jres = json.loads(res.content)

        err = jres.get("error")
        if err:
            self.log.error("Service unregister failed: %s" % err)
        else:
            # Clear knowledge of management endpoint we got from manager
            self._mgmt_endpoint = None

        #host, port = self.config.mgmt_endpoint.split(":")
        #req = json.dumps({'id': self.name, 'host': host, "port": port})
        #res = requests.delete("http://%s/unregister_service" % self.config.mgmt_host, data=req, headers={'content-type': 'application/json'}) #, auth=('user', '*****'))
        ##jres = json.loads(res.content)

        return True

    #endregion

    #region Routing and Route management

    def add_route(self, verbs, path, func):
        "Add a route from an incoming REST request to a function. Multiple verbs can be specified with pipe character."
        if path and path.startswith("/"):
            path = path [1:]
        for verb in verbs.split("|"):
            key = "%s_%s" % (verb.strip().upper(), path.lower())
            self._routes[key] = func

    def _hook(self, verb, path, data, format="application/json"):
        #print "VERB=[%s], PATH=[%s], DATA=[%s]" % (verb, path, data)

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
            res = func(payload, *args, **kwargs)
            return res
        except Exception as e:
            self.log.exception("Unhandled exception.")
            return {"error": "Unhandled exception: %s: %s" % (e.__class__.__name__, e)}

    #endregion Routing and Route management

    #region Command handlers

    def _mgmt_info(self, payload, *args, **kwargs):
        return {
            "name": self.name,
            "host": self._mgmt_endpoint or self.config.mgmt_endpoint,
        }

    def _mgmt_help(self, payload, *args, **kwargs):
        return {"routes": self._routes.keys()}

    def _mgmt_register(self, payload, *args, **kwargs):
        if self.register():
            return {"message": "Registered with management service."}
        else:
            return {"warning": "Not registered with management service."}

    def _mgmt_unregister(self, payload, *args, **kwargs):
        if self.unregister():
            return {"message": "Unregistered from management service."}
        else:
            return {"warning": "Not unregistered from management service."}

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

    def _mgmt_update(self, payload, *args, **kwargs):
        if self.update(payload):
            return {"message": "Processing updated."}
        else:
            return {"warning": "Processing was not updated."}

    #endregion Command handlers
