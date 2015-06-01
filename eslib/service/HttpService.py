__author__ = 'Hans Terje Bakke'

from .Service import Service
from .UrlParamParser import UrlParamParser
from ..procs.HttpMonitor import HttpMonitor
from ..esdoc import tojson
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
        connection_timeout        Timeout when connecting to other services. Float or tuple of (connect, read) timeouts.

    Communication with manager:

        POST hello
            id
            type
            config_key
            host
            port
            pid
            status
            meta_keys              List of addresses to metadata sections, with dot notation.
        => returns:
            port                   The assigned IP port.
            metadata               Metadata according to requested meta_keys.
        DELETE goodbye
            id

    The management interface for this service expects the following messages:

        GET  hello
        => returns:
            id
            type
            config_key
            host
            all_interfaces = False   : Listen on all interfaces (0.0.0.0) if set.
            port
            pid
            status
            meta_keys

        # TODO:
        GET  help
        GET  status
        GET  stats
        POST shutdown
        POST start
        POST stop
        POST abort
        POST suspend
        POST resume

        GET  metadata              Return changeset and metadata used by this service.
        POST metadata              Receive changeset and *altered* metadata sections according to subscription.
    """

    metadata_keys = []

    def __init__(self, **kwargs):
        super(HttpService, self).__init__(**kwargs)

        self.config.set_default(
            # A management server we can register with, that will manage this process through the 'mgmt_endpoint'
            manager_endpoint    = None,
            # The host:port endpoint where this service will listen for management commands
            management_endpoint = "localhost",  # Note: In a multi-node scenario, localhost might mean something else to the manager... so beware.
            # Listen on all interfaces (0.0.0.0) if set.
            all_interfaces = False,

            connection_timeout = (3.5, 60)  # Whe connecting to other services
        )

        self.metadata = {}
        self._receiver = None
        self._routes = []

        self.log.debug("Setting up routes.")

        # Add default management routes to functions
        self.add_route(self._mgmt_hello     , "GET"         , "/hello"     , None)
        self.add_route(self._mgmt_help      , "GET"         , "/help"      , None)
        self.add_route(self._mgmt_status    , "GET"         , "/status"    , None)
        self.add_route(self._mgmt_stats     , "GET"         , "/stats"     , None)

        self.add_route(self._mgmt_shutdown  , "DELETE"      , "/shutdown"  , None)
        self.add_route(self._mgmt_start     , "GET|PUT|POST", "/start"     , None)
        self.add_route(self._mgmt_restart   , "GET|PUT|POST", "/restart"   , None)
        self.add_route(self._mgmt_stop      , "GET|PUT|POST", "/stop"      , None)
        self.add_route(self._mgmt_abort     , "GET|PUT|POST", "/abort"     , None)
        self.add_route(self._mgmt_suspend   , "GET|PUT|POST", "/suspend"   , None)
        self.add_route(self._mgmt_resume    , "GET|PUT|POST", "/resume"    , None)

        self.add_route(self._mgmt_metadata_get   , "GET"     , "/metadata"  , None)
        self.add_route(self._mgmt_metadata_update, "PUT|POST", "/metadata"  , None)

        self.add_route(self._mgmt_debug_free, "DELETE", "/debug/free"     , None)

        self._receiver = HttpMonitor(service=self, name="receiver", hook=self._hook)
        self.register_procs(self._receiver)

    def remote(self, host, verb, path, data=None):
        res = requests.request(
            verb.lower(),
            "http://%s/%s" % (host, path),
            data=tojson(data) if data else None,
            headers={"content-type": "application/json"},
            timeout=self.config.connection_timeout  # either float or tuple of (connect, read) timeouts
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
        if self.config.manager_endpoint == "standalone":
            self.config.manager_endpoint = None

        data = self._build_hello_message(self.config.management_endpoint)
        if self.config.manager_endpoint and not self.config.manager_endpoint == "standalone":
            # Say hello to manager, asking for a port number if we're missing one
            try:
                content = self.remote(self.config.manager_endpoint, "post", "hello", data=data)
                error = content.get("error")
                if error:
                    self.log.error("Error from manager: %s" % error)
                    return False
            except Exception as e:
                self.log.error("Communication with manager failed for 'hello' message: %s" % e)
                return False
            data["port"] = content["port"]
            # Apply metadata from response
            metablock = content.get("metadata")
            if metablock:
                try:
                    self.update_metadata(metablock.get("version"), metablock.get("data"), wait=True)
                except Exception as e:
                    self.log.exception("Error parsing metadata. But proceeding...")
                    # Not returning false here, but letting it start with metadata still pending.
        else:
            self.log.info("No manager endpoint specified. Running stand-alone.")
            if not data["port"]:
                self.log.critical("Port must be specified when running stand-alone.")
                exit(1)
        listener_host = "0.0.0.0" if self.config.all_interfaces else data["host"]
        self._receiver.config.host = listener_host
        self._receiver.config.port = data["port"]

        self.log.info("Starting service management listener on '%s:%d'." % (listener_host, self._receiver.config.port))

        try:
            self._receiver.start()
        except Exception as e:
            self.log.critical("Service management listener failed to start.")
            return False  # Not started; failed

        return True

    def on_shutdown(self):
        # Tell the manager we're leaving
        if self.config.manager_endpoint:
            self.log.info("Saying goodbye to the manager.")
            data = {"id": self.name}
            try:
                content = self.remote(self.config.manager_endpoint, "delete", "goodbye", data=data)
                error = content.get("error")
                if error:
                    self.log.error("Error from manager: %s" % error)
                    return False
            except Exception as e:
                self.log.warning("Communication with manager failed for 'goodbye' message: %s" % e)
                # But we go on...
                #return False

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
            "id"        : self.name,
            "type"      : self.__class__.__name__,
            "config_key": self.config_key,
            "host"      : host,
            "port"      : port,
            "pid"       : self.pid,
            "status"    : self.status,
            "meta_keys" : self.metadata_keys
        }

    def _mgmt_hello(self, request_handler, payload, **kwargs):
        self.log.trace("called: hello")
        return self._build_hello_message()

    def _mgmt_help(self, request_handler, payload, **kwargs):
        self.log.debug("called: help")
        routes = ["%s %s" % (r.verb, r.path) for r in self._routes]
        return {"routes": routes}

    def _mgmt_status(self, request_handler, payload, **kwargs):
        self.log.trace("called: status")
        return {"status": self.status}

    def _mgmt_stats(self, request_handler, payload, **kwargs):
        self.log.trace("called: stats")
        return {
            "status"     : self.status,
            "stats"      : self.get_stats()
        }

    # args: (bool)wait
    def _mgmt_shutdown(self, request_handler, payload, **kwargs):
        self.log.debug("called: shutdown")
        wait = False
        if payload:
            wait = payload.get("wait") or False
        if self.shutdown():
            if wait:
                return {"message": "Service shut down."}
            else:
                return {"message": "Service shutting down."}
        else:
            return {"error": "Not shut down."}

    def _mgmt_start(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_start")
        if self.processing_start():
            return {"message": "Processing started."}
        else:
            return {"error": "Processing was not started."}

    # args: (bool)wait
    def _mgmt_restart(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_restart")
        wait = False
        if payload:
            wait = payload.get("wait") or False
        if self.processing_restart():
            if wait:
                return {"message": "Processing (re)started."}
            else:
                return {"message": "Processing (re)starting."}
        else:
            return {"error": "Processing not started."}

    # args: (bool)wait
    def _mgmt_stop(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_stop")
        wait = False
        if payload:
            wait = payload.get("wait") or False
        if self.processing_stop(wait=wait):
            if wait:
                return {"message": "Processing stopped."}
            else:
                return {"message": "Processing stopping."}
        else:
            return {"error": "Processing was not stopped."}

    def _mgmt_abort(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_abort")
        if self.processing_abort():
            return {"message": "Processing aborted."}
        else:
            return {"error": "Processing was not aborted."}

    def _mgmt_suspend(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_suspend")
        if self.processing_suspend():
            return {"message": "Processing suspended."}
        else:
            return {"error": "Processing was not suspended."}

    def _mgmt_resume(self, request_handler, payload, **kwargs):
        self.log.debug("called: processing_resume")
        if self.processing_resume():
            return {"message": "Processing resumed."}
        else:
            return {"error": "Processing was not resumed."}

    def _mgmt_metadata_get(self, request_handler, payload, **kwargs):
        self.log.debug("called: get metadata")
        ret = {
            "version": self.metadata_version,
            "keys": self.metadata_keys,
            "data": self.metadata
        }
        return ret

    def _mgmt_metadata_update(self, request_handler, payload, **kwargs):
        self.log.debug("called: update metadata")
        old_changeset = self.metadata_version
        new_changeset = payload.get("version")
        metadata = payload.get("data")

        if new_changeset == old_changeset:
            return {"warning": "Version '%s' is already in use; update ignored." % new_changeset}
        self.update_metadata(new_changeset, metadata, wait=False)
        return {"message": "Metadata updated to version '%s'. Reconfiguring now." % new_changeset}


    def _mgmt_debug_free(self, request_handler, payload, **kwargs):
        self.log.trace("called: debug/free")

        import gc, psutil
        proc = psutil.Process()
        mem_before = proc.get_memory_info().rss / 1024 / 1024
        gc.collect()
        mem_after = proc.get_memory_info().rss / 1024 / 1024
        msg = ("Memory released by forced garbage collection: %d MB (%d => %d)" % (mem_before - mem_after, mem_before, mem_after))
        self.log.info(msg)

        return {"message": msg}

    #endregion Command handlers
