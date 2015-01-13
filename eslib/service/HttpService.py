__author__ = 'Hans Terje Bakke'

from .Service import Service, ServiceOperationError
from .UrlParamParser import UrlParamParser
from ..procs.HttpMonitor import HttpMonitor
import json, requests


class Route(object):
    def __init__(self, func, verb, path_specification=None, param_specifications=None):
        self.func = func
        self.verb = verb
        self.path = path_specification
        self.params = param_specifications
        self.parser = UrlParamParser(path_specification=path_specification, param_specifications=param_specifications)

    def parse(self, url):
        return self.parser.parse(url)


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
        self._routes = []

        self.log.debug("Setting up routes.")

        # Add default management routes to functions
        self.add_route(self._mgmt_info      , "GET"     , "/info"      , None)
        self.add_route(self._mgmt_help      , "GET"     , "/help"      , None)
        self.add_route(self._mgmt_status    , "GET"     , "/status"    , None)
        self.add_route(self._mgmt_register  , "GET|POST", "/register"  , None)
        self.add_route(self._mgmt_unregister, "GET|POST", "/unregister", None)
        self.add_route(self._mgmt_shutdown  , "POST"    , "/shutdown"  , None)
        self.add_route(self._mgmt_update    , "POST"    , "/update"    , None)
        self.add_route(self._mgmt_start     , "GET|POST", "/start"     , None)
        self.add_route(self._mgmt_restart   , "GET|POST", "/restart"   , None)
        self.add_route(self._mgmt_stop      , "GET|POST", "/stop"      , None)
        self.add_route(self._mgmt_abort     , "GET|POST", "/abort"     , None)
        self.add_route(self._mgmt_suspend   , "GET|POST", "/suspend"   , None)
        self.add_route(self._mgmt_resume    , "GET|POST", "/resume"    , None)

        self._receiver = HttpMonitor(service=self, name="receiver", hook=self._hook)
        self.register_procs(self._receiver)

    #region Debugging

    def DUMP_ROUTES(self):
        print "%-6s %-20s %s" % ("VERB", "ROUTE", "QUERY PARAMETERS")
        for route in self._routes:
            print "%-6s %-20s %s" % (route.verb, route.path, route.params)

    #endregion Debugging

    #region Service management overrides

    def on_run(self):
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

        self.log.info("Starting service management listener.")

        try:
            self._receiver.start()
        except Exception as e:
            self.log.critical("Service managemnet listener failed to start.")
            return False  # Not started; failed

        return True

    def on_shutdown(self, wait):
        # Stop the receiver
        self.log.info("Stopping service management listener.")
        self._receiver.stop()
        return True

    def on_wait(self):
        # Note: We cannot use shutdown(wait=True) with HttpMonitor, because it will enter a deadlock.
        # (It will not finish unless the request has finished, and the request will not finish unless
        # we return from here.)
        self.log.debug("Waiting for service management listener to stop.")
        self._receiver.wait()
        self.log.info("Service management listener stopped.")
        return True  # Shut down successfully

    #endregion Service management overrides

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

    def add_route(self, func, verbs, path, params):
        "Add a route from an incoming REST request to a function. Multiple verbs can be specified with pipe character."
        for verb in verbs.split("|"):
            self._routes.append(Route(func, verb, path, params))

    def get_route(self, verb, path):
        for route in self._routes:
            if route.verb == verb:
                params = route.parse(path)
                if params is not None:
                    return route, params
        return None, None

    def _hook(self, request_handler, verb, path, data): #, format="application/json"):

        route, params = self.get_route(verb, path)

        if not route:
            return {"error": "No route for '%s'." % path}

        payload = data
        # if data and format == "application/json":
        #     payload = json.loads(data)

        try:
            # res = func(payload, *args, **kwargs)
            res = route.func(request_handler, payload, **params)
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
