__author__ = 'Hans Terje Bakke'

from .Service import Service
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
        manager_endpoint          Address 'host:port' to manager service; for receiving metadata updates, etc.
        management_endpoint       Management endpoint for this service; where it can be managed.
        connection_timeout        Timeout when connecting to other services.

    Communication with manager:

        POST hello
            id
            pid
            status
            metakeys
        => returns:
            assigned_port
            metadata
        DELETE goodbye
            id

    The management interface for this service expects the following messages:

        GET  hello
        => returns:
            id
            pid
            status
            metakeys

        # TODO:
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
            manager_endpoint    = None,
            # The host:port endpoint where this service will listen for management commands
            management_endpoint = "localhost:4444",

            connection_timeout = 1.0  # Whe connecting to other services
        )

        self.metadata = {}
        self._receiver = None
        self._routes = []

        self.log.debug("Setting up routes.")

        # Add default management routes to functions
        self.add_route(self._mgmt_hello     , "GET"         , "/hello"     , None)
        self.add_route(self._mgmt_help      , "GET"         , "/help"      , None)
        self.add_route(self._mgmt_status    , "GET"         , "/status"    , None)

        self.add_route(self._mgmt_shutdown  , "DELETE"      , "/shutdown"  , None)
        self.add_route(self._mgmt_start     , "GET|PUT|POST", "/start"     , None)
        # TODO (restart):
        self.add_route(self._mgmt_restart   , "GET|PUT|POST", "/restart"   , None)
        self.add_route(self._mgmt_stop      , "GET|PUT|POST", "/stop"      , None)
        self.add_route(self._mgmt_abort     , "GET|PUT|POST", "/abort"     , None)
        self.add_route(self._mgmt_suspend   , "GET|PUT|POST", "/suspend"   , None)
        self.add_route(self._mgmt_resume    , "GET|PUT|POST", "/resume"    , None)
        # TODO (update):
        self.add_route(self._mgmt_update    , "PUT|POST"    , "/update"    , None)

        self._receiver = HttpMonitor(service=self, name="receiver", hook=self._hook)
        self.register_procs(self._receiver)

    def remote(self, host, verb, path, data=None):
        res = requests.request(
            verb.lower(),
            "http://%s/%s" % (host, path),
            data=json.dumps(data) if data else None,
            headers={"content-type": "application/json"},
            timeout=self.config.connection_timeout
            #, auth=('user', '*****')
        )
        if res.content:
            return json.loads(res.content)
        else:
            return None

    #region Debugging

    def DUMP_ROUTES(self):
        print "%-6s %-20s %s" % ("VERB", "ROUTE", "QUERY PARAMETERS")
        for route in self._routes:
            print "%-6s %-20s %s" % (route.verb, route.path, route.params)

    #endregion Debugging

    #region Service management overrides

    def on_run(self):
        data = self._build_hello_message(self.config.management_endpoint)
        if self.config.manager_endpoint:
            # Say hello to manager, asking for a port number if we're missing one
            content = self.remote(self.config.manager_endpoint, "post", "hello", data=data)
            error = content.get("error")
            if error:
                self.log.error("Error from server: %s" % error)
                return False
            data["port"] = content["port"]
            self._metadata = content["metadata"]
        else:
            self.log.info("No manager endpoint specified. Running stand-alone.")
        self._receiver.config.host = data["host"]
        self._receiver.config.port = data["port"]

        self.log.info("Starting service management listener on '%s:%d'." % (self._receiver.config.host, self._receiver.config.port))

        try:
            self._receiver.start()
        except Exception as e:
            self.log.critical("Service managemnet listener failed to start.")
            return False  # Not started; failed

        return True

    def on_shutdown(self):
        # Tell the manager we're leaving
        if self.config.manager_endpoint:
            self.log.info("Saying goodbye to the manager.")
            data = {"id": self.name}
            self.remote(self.config.manager_endpoint, "delete", "goodbye", data=data)

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
            return {"error": "No route for %s:'%s'." % (verb, path)}

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

    def _build_hello_message(self, addr_str=None):
        host = None
        port = None
        if addr_str:
            a = addr_str.split(":")
            if len(a) == 2:
                host = a[0]
                port = int(a[1])
            else:
                host = addr_str
        else:
            host = self._receiver.config.host
            port = self._receiver.config.port
        return {
            "id"      : self.name,
            "host"    : host,
            "port"    : port,
            "pid"     : self.pid,
            "status"  : self.status,
            "metakeys": self.config_keys
        }

    def _mgmt_hello(self, request_handler, payload, **kwargs):
        self.log.debug("called: hello")
        return self._build_hello_message()

    def _mgmt_help(self, request_handler, payload, **kwargs):
        self.log.debug("called: help")
        return {"routes": self._routes.keys()}

    def _mgmt_status(self, request_handler, payload, **kwargs):
        self.log.debug("called: status")
        return {"status": self.status}

    def _mgmt_shutdown(self, request_handler, payload, **kwargs):
        self.log.debug("called: shutdown")
        if self.shutdown():
            return {"message": "Shutting down."}
        else:
            return {"error": "Not shut down."}

    # TODO
    def _mgmt_start(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_start")
        if self.processing_start():
            return {"message": "Processing started."}
        else:
            return {"error": "Processing was not started."}

    # TODO
    def _mgmt_restart(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_restart")
        if self.processing_restart():
            return {"message": "Processing restarted."}
        else:
            return {"error": "Processing not started."}

    # TODO
    def _mgmt_stop(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_stop")
        if self.processing_stop():
            return {"message": "Processing stopped."}
        else:
            return {"error": "Processing was not stopped."}

    # TODO
    def _mgmt_abort(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_abort")
        if self.processing_abort():
            return {"message": "Processing aborted."}
        else:
            return {"error": "Processing was not aborted."}

    # TODO
    def _mgmt_suspend(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_suspend")
        if self.processing_suspend():
            return {"message": "Processing suspended."}
        else:
            return {"error": "Processing was not suspended."}

    # TODO
    def _mgmt_resume(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_resume")
        if self.processing_resume():
            return {"message": "Processing resumed."}
        else:
            return {"error": "Processing was not resumed."}

    # TODO
    def _mgmt_update(self, request_handler, payload, **kwargs):
        self.log.debug("called: update")
        if self.update(payload):
            return {"message": "Processing updated."}
        else:
            return {"error": "Processing was not updated."}

    #endregion Command handlers
