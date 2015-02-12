#!/usr/bin/env python
# -*- coding: utf-8 -*-

from eslib.service import HttpService, status
from eslib.procs import Timer
from eslib.time import utcdate
import json, datetime, Queue, os, signal, subprocess, sys

from elasticsearch import Elasticsearch
from elasticsearch.client.indices import IndicesClient
from elasticsearch.helpers import scan

class ServiceInfo(object):
    def __init__(self):
        # Persisted permanent data
        self.id         = None
        self.guest      = False  # Guests register/unregister themselves
        self.host       = None   # Host name (not port) where service should run (or guest IS running)
        self.type       = None
        self.config_key = None   # Section tag for service config in global config
        self.metadata_keys = []  # Sections/fields in the metadata that the service subscribes to
        self.fixed_port = False  # Whether port is given and insisted on, as opposed to dynamic
        # Persisted temporary data
        self.port       = None   # Port number assigned by manager or insisted by guest
        self.pid        = None   # OS Process ID
        self.last_seen  = None
        # Not persisted
        self.fail_count = 0
        self.status     = status.DEAD
        self.error      = None

    @property
    def addr(self):
        return "%s:%d" % (self.host, self.port) if self.port is not None else self.host

    def set_ok(self):
        self.error = None
        self.fail_count = 0


class ServiceManager(HttpService,):

    def __init__(self, **kwargs):
        super(ServiceManager, self).__init__(**kwargs)

        self.config.set_default(
            management_endpoint     = "localhost:5000", # for this service...
            service_runner          = None,
            service_dir             = None,

            elasticsearch_hosts     = ["localhost:9200"],
            elasticsearch_index     = "management",

            dynamic_port_ranges     = [("localhost", 5001, 5099)],
            connection_timeout      = (3.5, 10.0)  # max seconds to wait for connection to a service, max wait for read
        )

        self._es = None
        self._es_index = None

        self._services = {}
        self._config = {}

        # For dynamic port allocation:
        self._available_ports = {}
        for dpr in self.config.dynamic_port_ranges:
            ports = self._available_ports[dpr[0]] = []
            for port in range(dpr[1], dpr[2]+1):
                ports.append(port)

        self._timer = None
        self._upload_queue = None

        # Clear routes and re-enable a minimal set of calls that make sense here
        self._routes = []
        # Use those in HttpService:
        self.add_route(self._mgmt_hello             , "GET"     , "/hello"      , None)
        self.add_route(self._mgmt_help              , "GET"     , "/help"       , None)

        # self.add_route("GET"     , "status"    , self._mgmt_status)

        self.add_route(self._mgmt_service_register  , "POST|PUT" , "/hello"     , None)
        self.add_route(self._mgmt_service_unregister, "DELETE"   , "/goodbye"   , None)


        self.add_route(self._mgmt_service_list      , "GET"     , "/list"       , None)
        self.add_route(self._mgmt_service_stats     , "GET"     , "/stats"      , None)

        self.add_route(self._mgmt_service_add       , "POST|PUT", "/add"        , None)
        self.add_route(self._mgmt_service_remove    , "DELETE"  , "/remove"     , None)

        self.add_route(self._mgmt_service_run       , "POST|PUT", "/run"        , None)
        self.add_route(self._mgmt_service_shutdown  , "DELETE"  , "/shutdown"   , None)
        self.add_route(self._mgmt_service_kill      , "DELETE"  , "/kill"       , None)

        self.add_route(self._mgmt_processing_start  , "POST|PUT", "/processing_start"  , None)
        self.add_route(self._mgmt_processing_stop   , "POST|PUT", "/processing_stop"   , None)
        self.add_route(self._mgmt_processing_abort  , "POST|PUT", "/processing_abort"  , None)
        self.add_route(self._mgmt_processing_suspend, "POST|PUT", "/processing_suspend", None)
        self.add_route(self._mgmt_processing_resume , "POST|PUT", "/processing_resume" , None)

    @property
    def _now(self):
        return datetime.datetime.utcnow()

    def on_configure(self, credentials, config, global_config):
        self.config.set(
            management_endpoint     = config.get("management_endpoint") or self.config.management_endpoint,
            service_runner          = config.get("service_runner"),
            service_dir             = config.get("service_dir"),

            elasticsearch_hosts     = config["elasticsearch_hosts"],
            elasticsearch_index     = config["elasticsearch_index"],

            dynamic_port_range      = config.get("dynamic_port_range")
        )

    def on_setup(self):
        self._es = Elasticsearch(hosts=self.config.elasticsearch_hosts)
        self._es_index = self.config.elasticsearch_index

        self._load_services()
        self._probe_loaded_services()  # find PID and IP port for services

        self._upload_queue = Queue.Queue()

        self._timer = Timer(actions=[(1, 1, "ping")])
        self._timer.add_callback(self._ping)

        return True

    def _ping(self, doc):
        return  # DEBUG

        # TODO: Use a thread pool for pushing configs, and only pop off the queue if the thread pool is ready
        if self._upload_queue and self._upload_queue.qsize():
            while self._upload_queue.qsize():
                service = self._upload_queue.get_nowait()
                if service:
                    if service["fail_count"] >= 3:  # Three strikes, and you're out!
                        continue  # Ignore it for now... it must be re-registered
                        # TODO: Later we should just let if wait a bit longer before we try again
                    self._push_to_service(service)

    # def _push_to_service(self, service):
    #     self.log.debug("Pushing config to service '%s'." % service["id"])
    #
    #     # Push config to remote host
    #     try:
    #         req = json.dumps(service)
    #         res = requests.post("http://%s:%d/update" % (service["host"], service["port"]), data=req, headers={'content-type': 'application/json'}) #, auth=('user', '*****'))
    #     except Exception as e:
    #         self.log.error("Failed to push config to service '%s'.")
    #         service["fail_count"] += 1
    #         return
    #
    #     self.log.info("Config pushed to service '%s'." % service["id"])
    #     # Now finally register that we have seen this service:
    #     service["fail_count"] = 0
    #     self._save_registered_service(service["id"])


    #region Storage

    _matchall_query = {'query': {'match_all': {}}}

    def _storage_load_services(self):
        self.log.info("Loading registered services.")
        # ensure all documents are indexed before executing query
        docs = []
        try:
            ic = IndicesClient(self._es)
            # If index does not exist, create it:
            if not ic.exists(self._es_index):
                self.log.info("Management index '%s' did not exist. Creating it." % self._es_index)
                ic.create(self._es_index)
            ic.refresh(self._es_index)
            scan_resp = scan(self._es, index=self._es_index, doc_type='service', query=self._matchall_query)
            docs = [doc for doc in scan_resp]
        except Exception as e:
            self.log.critical("Failed to load registered services from index. Aborting. %s: %s" % (e.__class__.__name__, e))
            exit()

        services = {}
        for doc in docs:
            s = doc["_source"]
            service = services[doc["_id"]] = ServiceInfo()
            service.id            = doc["_id"]
            service.guest         = s["guest"]
            service.host          = s["host"]
            service.config_key    = s["config_key"]
            service.metadata_keys = s["metadata_keys"]
            service.fixed_port    = s["fixed_port"]
            service.port          = s["port"]
            service.pid           = s["pid"]
            service.last_seen     = utcdate(s.get("last_seen") or self._now)
        return services

    def _storage_save_service(self, service):
        "Expects services to have been added already. 'last_seen' is set to now."
        payload = {
            "guest"        : service.guest,
            "host"         : service.host,
            "config_key"   : service.config_key,
            "metadata_keys": service.metadata_keys,
            "fixed_port"   : service.fixed_port,
            "port"         : service.port,
            "pid"          : service.pid,
            "last_seen"    : service.last_seen  # elasticsearch api will serialize this properly
            # not persisting "fail_count", "error", "status"
        }

        try:
            self._es.index(self._es_index, doc_type="service", id=service.id, body=payload)
        except Exception as e:
            self.log.error("Failed to save registered service '%s' to index. %s: %s" % (id, e.__class__.__name__, e))
            # Report it as operation failed, but it is only persisting that has failed... no big deal.
            return False

    def _storage_delete_service(self, id):
        try:
            ic = IndicesClient(self._es)
            ic.refresh(index=self._es_index)
            res = self._es.delete(index=self._es_index, doc_type="service", id=id)
        except Exception as e:
            self.log.error("Failed to delete registered service '%s' from index. %s: %s" % (id, e.__class__.__name__, e))
            return False

    #endregion Storage

    #region Dynamic port allocation helpers

    def _get_port(self, host):
        if host in self._available_ports and self._available_ports[host]:
            return self._available_ports[host].pop(0)  # Get first available
        return None  # Not a managed host, or dynamic port pool exhausted.

    def _grab_port(self, host, port):
        if host in self._available_ports and port in self._available_ports[host]:
            self._available_ports[host].remove(port)
            return True
        return False

    def _release_port(self, host, port):
        if host in self._available_ports and not port in self._available_ports:
            # Now check if it's within our configured pool ranges
            for dpr in self.config.dynamic_port_ranges:
                if dpr[0] == host:
                    if dpr[1] <= port <= dpr[2]:
                        self._available_ports[host].append(port)
                        return True
                    else:
                        return False
        return False

    #endregion Dynamic port allocation helpers

    #region Helper methods

    def _load_services(self):
        self._services = self._storage_load_services()
        for service in self._services.values():
            if self._grab_port(service.host, service.port):
                self.log.trace("Service '%s' grabbed address '%s' from dynamic pool." % (service.id, service.addr))
        self.log.info("Loaded %d services." % len(self._services))

    def _probe_loaded_services(self):
        "Find PID for and IP port for matching services."

        self.log.info("Checking which registered services are already running.")
        for service in self._services.values():
            # If the process answers 'hello', we can try to match it
            if not service.port:
                if service.guest:
                    self._remove_dead_service(service)
                continue
            error = None
            addr = id = pid = status = metakeys = None
            try:
                content = self.remote(service.addr, "get", "hello")
                id       = content["id"]
                pid      = content["pid"]
                status   = content["status"]
                metakeys = content.get("metakeys")

                if not id == service.id:
                    error = "ID mismatch in 'hello' response at '%s'. Expected '%s', got '%s'." % (addr, service.id, id)
                    self.log.warning(error)
                    service.error = error
                    service.fail_count += 1
            except Exception as e:
                error = "Registered service '%s' did not respond properly to 'hello' at '%s'." % (service.id, addr)
                self.log.debug(error)
                service.fail_count += 1  # Because we failed to contact it... but maybe it's not supposed to be running
                self.error = None        # ... and that is why we do not register an error message here; wait for explicit launch request to do that.

            if error:
                self._remove_dead_service(service)
                continue

            self.log.info("Found service '%s' with pid=%s running on '%s'." % (id, pid, addr))
            service.pid           = pid
            service.status        = status
            service.metadata_keys = metakeys or []
            service.last_seen     = self._now
            service.error         = None
            service.fail_count    = 0
            self._storage_save_service(service)

    def _get_status(self, service, save=True):
        return self._get_stats(service, save)[0]

    def _get_stats(self, service, save=True):
        "Returns a tuple of (status, statistics)."

        ss = status.DEAD
        stats = None

        if service.status == status.DEAD:
            return (status.DEAD, stats)  # Don't even bother
        if not service.port:
            return (status.DEAD, stats)  # We cannot reach it without a port, so it's dead to us

        error = None
        try:
            content = self.remote(service.addr, "get", "stats")  # Note: Using 'stats' instead of 'status', now..
            ss = content.get("status")
            stats = content.get("stats")
            error = content.get("error")  # Should not really be possible!
            if not "status" in content:
                error = "Missing status field."
            if error:
                msg = "Failed to retrieve stats from service '%s' at '%s': %s" % (service.id, service.addr, error)
                self.log.error(msg)
        except Exception as e:
            error = str(e)
            msg = "Failed to communicate with service '%s' at '%s': %s" % (service.id, service.addr, e)
            self.log.warning(msg)

        if error:
            service.error = error
            service.fail_count += 1
            if service.fail_count >= 0:  # TODO: Always remove it for now
                self._remove_dead_service(service)
            return (status.DEAD, stats)

        service.status = ss
        service.last_seen = self._now
        service.error = None
        service.fail_count = 0
        if save:
            self._storage_save_service(service)
        return (ss, stats)

    def _remove_dead_service(self, service):
        service.pid = None
        service.status = status.DEAD
        #service.last_seen = self._now

        # Remove port from pool if it's in the dynamic pool
        if self._release_port(service.host, service.port):
            self.log.trace("Service '%s' released address '%s' back to dynamic pool." % (service.id, service.addr))
        if not service.fixed_port:
            service.port = None
        if service.guest:
            self._storage_delete_service(service.id)
            del self._services[service.id]
        else:
            self._storage_save_service(service)

    #endregion Helper methods

    #region Service interface commands

    def _mgmt_service_add(self, request_handler, payload, **kwargs):
        self.log.debug("called: add service '%s'" % payload.get("id"))
        return self._add_service(
            payload.get("guest") or False,  # guest
            payload.get("id"),              # service id
            payload.get("host"),            # hostname for where to run this service (or where guest IS running)
            payload.get("port"),            # port service insists to run admin port on
            payload.get("config_key"),      # config key
            payload.get("start") or False   # auto_start
        )

    def _mgmt_service_remove(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: remove service(s) [%s]" % ", ".join(ids))
        return self._remove_services(
            ids,
            payload.get("stop") or False
        )

    def _mgmt_service_register(self, request_handler, payload, **kwargs):
        self.log.debug("called: hello/register service '%s'" % payload.get("id"))
        return self._register_service(
            payload.get("id"),
            payload.get("type"),
            payload.get("config_key"),
            payload.get("host"),
            payload.get("port") or None,
            payload.get("pid"),
            payload.get("status"),
            payload.get("meta_keys"),
        )

    def _mgmt_service_unregister(self, request_handler, payload, **kwargs):
        self.log.debug("called: goodbye/unregister service '%s'" % payload.get("id"))
        return self._unregister_service(
            payload.get("id"),
        )

    def _mgmt_service_list(self, request_handler, payload, **kwargs):
        # TODO: name patterns from payload or kwargs
        #ids = payload.get("names") or []

        services = list(self._services.values())
        self.log.debug("called: list services")
        return self._get_service_info(services)

    def _get_own_service_info(self):
        host, port = self.config.management_endpoint.split(":")

        return {
            "id"         : self.name,
            "guest"      : True,
            "host"       : host,
            "type"       : self.__class__.__name__,
            "config_key" : self.config_key,
            "meta_keys"  : [],
            "fixed_port" : True,
            "port"       : port,
            "pid"        : self.pid,
            "last_seen"  : self._now,
            "fail_count" : 0,
            "fail_reason": None,
            "status"     : self.status,
            # Runtime statistics from service:
            "stats"      : self.get_stats()
        }

    def _get_service_info(self, services, return_missing=False):
        ret = {}
        for service in services:
            ss, stats = self._get_stats(service)
            # The service may have been removed in the call to _get_status:
            if not service.id in self._services:
                if return_missing:
                    ret[service.id] = {"error": "Service '%s' no longer exists." % service.id}
            else:
                ret[service.id] = {
                    "id"         : service.id,
                    "guest"      : service.guest,
                    "host"       : service.host,
                    "type"       : service.type,
                    "config_key" : service.config_key,
                    "meta_keys"  : service.metadata_keys,
                    "fixed_port" : service.fixed_port,
                    "port"       : service.port,
                    "pid"        : service.pid,
                    "last_seen"  : service.last_seen,
                    "fail_count" : service.fail_count,
                    "fail_reason": service.error,
                    "status"     : ss,
                    # Runtime statistics from service:
                    "stats"      : stats
                }
        return ret

    def _mgmt_service_stats(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []

        if not ids:
            return {self.name: self._get_own_service_info()}

        self.log.debug("called: get stats for service(s) [%s]" % ", ".join(ids))
        missing = [id for id in ids if id not in self._services]
        fetch = [service for service in self._services.values() if service.id in ids]
        ret = self._get_service_info(fetch, return_missing=True)
        for id in missing:
            if not id in ret:
                ret[id] = {"error": "Service '%s' not found." % id}
        return ret

    def _mgmt_service_run(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: run service(s) [%s]" % ", ".join(ids))
        return self._run_services(ids)

    def _mgmt_service_shutdown(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: shutdown service(s) [%s]" % ", ".join(ids))
        return self._shutdown_services(
            ids,
            payload.get("wait") or False
        )

    def _mgmt_service_kill(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: kill service(s) [%s]" % ", ".join(ids))
        return self._kill_services(
            ids,
            payload.get("force") or False
        )

    def _mgmt_processing_start(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: start service(s) [%s]" % ", ".join(ids))
        return self._start_processing(ids)

    def _mgmt_processing_stop(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: stop service(s) [%s]" % ", ".join(ids))
        return self._stop_processing(
            ids,
            payload.get("wait") or False
        )

    def _mgmt_processing_abort(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: abort service(s) [%s]" % ", ".join(ids))
        return self._abort_processing(ids)

    def _mgmt_processing_suspend(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: suspend service(s) [%s]" % ", ".join(ids))
        return self._suspend_processing(ids)

    def _mgmt_processing_resume(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: resume service(s) [%s]" % ", ".join(ids))
        return self._resume_processing(ids)

    #endregion Service interface commands

    #region Service interface helpers

    def _add_service(self, guest, id, host, port, config_key, auto_start, save=True):
        if port is not None and type(port) is not int:
            port = int(port)

        # Check if service is already registered
        if id in self._services:
            self.log.error("Tried to register an already registered service '%s'." % id)
            return {"error": "Service '%s' is already registered." % id}

        if port:
            # Check if another service is registered with this port, if port is insisted on
            for s in list(self._services.values()):
                if host == s.host and port == s.port:
                    self.log.error("Tried to register service '%s' on endpoint '%s', held by service '%s'." % (id, s.addr, s.id))
                    return {"error": "Another service '%s' is already registered on '%s'." % (s.id, s.addr)}
            # Remove port from pool if it's in the dynamic pool
            if self._grab_port(host, port):
                self.log.trace("Service '%s' grabbed address '%s:%d' from dynamic pool." % (id, host, port))

        service = ServiceInfo()
        service.id         = id
        service.guest      = guest
        service.host       = host
        service.config_key = config_key
        if port:
            service.fixed_port = True
            service.port       = port
            # dynamic port will be assigned upon service exec/run

        self._services[id] = service
        if save:
            self._storage_save_service(service)
        as_guest_str = " as guest" if service.guest else ""
        msg = "Service '%s' registered%s on endpoint '%s'." % (service.id, as_guest_str, service.addr)
        self.log.info(msg)

        error = None
        if auto_start:
            if self._launch_service(service): # Will log success and failures itself
                msg += " And launched!"
            else:
                msg += " But failed to launch!"
                error = "Service '%s' failed to launch." % id

        ret = {"message": msg}
        if error:
            ret["error"] = error
        return ret

    def _remove_services(self, ids, auto_stop):
        succeeded = []
        failed = []
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to remove a non-existing service '%s'." % id)
                #return {"error": "No such registered service '%s'." % id}
                failed.append(id)
                continue

            service = self._services[id]
            ss = self._get_status(service, save=False)  # We're saving later here anyway
            if not id in self._services:
                #return {"message": "Service '%s' removed." % id}
                succeeded.append(id)
                continue

            # We can only remove services that are dead; otherwise we require a shutdown first
            if ss == status.DEAD:
                service.guest = True  # Demoting to guest makes sure the _remove_dead_service will remove it
                self._remove_dead_service(service)
                msg = "Service '%s' removed." % id
                self.log.info(msg)
                #return {"message": msg}
                succeeded.append(id)
            elif auto_stop:
                was_guest = service.guest
                service.guest = True  # Demoting it to guest will make sure it is removed after it has shut down and sends a goodbye/unregister message
                self._storage_save_service(service)

                # Tell the service to shut down
                error = None
                try:
                    content = self.remote(service.addr, "delete", "shutdown", {"id": id})
                    error = content.get("error")
                except Exception as e:
                    error = str(e)
                if error:
                    self.log.warning("Remove active service ('%s') with shutdown request failed: %s" % (id, error))
                    #return {"error": "Service '%s' marked for removal, but shutdown request failed: %s" % (id, error)}
                    failed.append(id)
                else:
                    extra_info = " demoted to guest and" if was_guest else ""
                    self.log.info("Request to remove a running service '%s'; %s shut down message sent to service." % (id, extra_info))
                    #return {"message": "Service '%s' was running; %s and shut down message sent to service." % (id, extra_info)}
                    succeeded.append(id)
            else:
                msg = "Tried to remove a service ('%s') that was not dead without the auto_stop flag set." % id
                self.log.debug(msg)
                #return {"error": msg}
                failed.append(id)

        ret = {}
        if failed:
            ret["error"] = "Services not removed: [%s]" % ", ".join(failed)
        if succeeded:
            ret["message"] = "Services removed: [%s]" % ", ".join(succeeded)
        return ret

    def _register_service(self, id, service_type, config_key, host, port, pid, status, metakeys):
        # If this service is not registered, treat it as a guest.
        # Give it a port number if missing.

        guest = False
        dynport = None
        if not port:
            dynport = self._get_port(host)
            if not dynport:
                msg = "Failed to allocate dynamic port for host '%s'. Not managed host or dynamic port range exhausted." % host
                self.log.warning(msg)
                return {"error": msg}
            else:
                self.log.info("Allocated port '%s:%d' for service '%s'." % (host, dynport, id))
        else:
            for s in self._services.values()[:]:
                if s.port == port:
                    if s.id == id:
                        break  # Same service reserved port; this is ok
                    else:
                        self.log.warning("Service '%s' saying 'hello' and requesting fixed address '%s:%d', already reserved by service '%s'." % (id, host, port, s.id))
                        return {"error": "Address '%s:%d' is already reserved by service '%s'." % (host, port, s.id)}

        if not id in self._services:
            self.log.debug("Hello from unknown service '%s'; treating it as a guest." % id)
            guest = True
            self._add_service(guest, id, host, port, None, False, save=False)
        service = self._services[id]
        service.config_key = config_key
        service.type = service_type
        service.pid = pid
        service.status = status
        service.metadata_keys = metakeys or []
        service.port = port or dynport
        service.last_seen = self._now
        service.error = None
        service.fail_count = 0
        self._storage_save_service(service)

        # TODO: We might want to return some more stuff here...(?)
        ret = {
            "port": service.port,
            "metadata": {}  # TODO: METADATA
        }
        if guest:
            ret["message"] = "Service '%s' registered as guest." % id
        else:
            ret["message"] = "Manager says hello back to registered service '%s'." % id
        return ret

    def _unregister_service(self, id):
        if not id in self._services:
            self.log.debug("Tried to unregister a non-existing service '%s'." % id)
            return {"error": "No such registered service '%s'." % id}

        service = self._services[id]
        extra_info = ""
        if self._release_port(service.host, service.port):
            extra_info = " Dynamic port '%s' released back to pool." % (service.addr)
            if not service.fixed_port:
                service.port = None
        if service.guest:
            del self._services[id]
            self._storage_delete_service(id)
            msg = "Guest service '%s' removed after saying goodbye.%s" % (id, extra_info)
            self.log.info(msg)
            return {"message": msg}
        else:
            service.pid = None
            service.status = status.DEAD
            service.last_seen = self._now
            service.error = None
            service.fail_count = 0
            self._storage_save_service(service)
            self.log.info("Registered service '%s' said goodbye.%s" % (id, extra_info))
            return {"message": "Manager waves goodbye back to service '%s'.%s" % (id, extra_info)}

    def _run_services(self, ids):
        # TODO: Launch service on remote host
        succeeded = []
        failed = []
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to launch a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                if self._launch_service(service):  # Does all necessary logging itself
                    succeeded.append(id)
                else:
                    failed.append(id)

        ret = {}
        if failed:
            ret["error"] = "Services that failed to launch: [%s]" % ", ".join(failed)
        if succeeded:
            ret["message"] = "Services launched: [%s]" % ", ".join(succeeded)
        return ret

    def _launch_service(self, service):
        #runner = "/Users/htb/git/elasticsearch-eslib/bin/es-run"
        #run_dir = "/Users/htb/git/customer-nets/services"

        # This is the normal case, that it was started from es-run:
        runner = sys.argv[0]
        run_dir = os.path.normpath(os.path.join(os.getcwd(), "../.."))
        # But there may be reasons to override this, especially if NOT run by es-run:
        if self.config.service_runner:
            runner = self.config.service_runner
        if self.config.service_dir:
            run_dir = self.config.service_dir

        # print "***RUNNER=", runner
        # print "***RUN_DIR=", run_dir
        # print "***CONFIG_FILE=", self.config_file
        # print "***CONFIG_KEY=", service.config_key

        if service.guest:
            self.log.warning("Tried to launch guest service '%s'. Guests cannot be managed." % service.id)
            return False
        ss = self._get_status(service)
        # It might have been removed by _get_status..
        if not service.id in self._services:
            self.log.warning("Service '%s' is no longer registered; cannot launch." % service.id)
            return False
        if not ss == status.DEAD:
            self.log.debug("Cannot launch service ('%s') with status '%s'." % (service.id, ss))
            return False

        self.log.debug("Launching service '%s' at '%s'." % (service.id, service.addr))

        args = [
            sys.executable,  # Same python that is running this
            runner,
            "-d", run_dir,
            service.id,
            "-m", self.config.management_endpoint,  # This manager address
            "-e", service.addr,  # Own address
            "--daemon"  # Needed for logging to directories anyway..
        ]
        if self.config_file:
            args.extend(["-f", self.config_file])
        if service.config_key:
            args.extend(["-c", service.config_key])

        p = None
        try:
            p = subprocess.Popen(
                args,
                stdout = subprocess.PIPE,
                stderr = subprocess.PIPE
            )
            # TODO: Grab stdout/stderr
        except Exception as e:
            self.log.exception("Failed to launch service '%s' at '%s'." % (service.id, service.addr))
            service.error = str(e)
            service.fail_count += 1
            return False

        service.pid = p.pid
        service.error = None
        service.fail_count = 0
        service.status = status.DEAD
        #service.last_seen = self._now
        #self._storage_save_service()
        # TODO: Perhaps just wait for service to say 'hello'?

        self.log.status("Service '%s' launched at '%s' with pid=%d." % (service.id, service.addr, service.pid))
        return True

    def _shutdown_services(self, ids, wait):
        # Since it has the same inner code as a processing operation:
        return self._processing_operation(ids, "delete", "shutdown", "shut down", "shut down", [status.IDLE, status.ABORTED, status.PROCESSING, status.SUSPENDED], wait)

    def _kill_services(self, ids, force):
        # This is only for extreme cases.
        # Normally, a shutdown should do the trick.

        # TODO: Kill service on remote host

        succeeded = []
        failed = []
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to kill a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                if not service.pid:
                    self.log.debug("Missing pid for servivce '%s'; cannot kill." % id)
                    failed.append(id)
                else:
                    dead = False
                    try:
                        if force:
                            self.log.debug("Forcefully kill service '%s', pid=%d." % (id, service.pid))
                        else:
                            self.log.debug("Attempting to kill service '%s', pid=%d." % (id, service.pid))
                        os.kill(service.pid, signal.SIGKILL if force else signal.SIGTERM)
                    except OSError as e:
                        self.log.debug("Killing service '%s', pid=%d, failed. errno=" % (id, service.pid, e.errno))
                        failed.append(id)
                        if e.errno == 3:  # does not exist
                            dead = True  # Although kill failed, consider it dead (because it does no longer exist)
                    else:
                        # SIGTERM kills should cause a shutdown which will later cause a 'goodbye' notification back to this manager.
                        if force:
                            dead = True  # Consider it gone
                        succeeded.append(id)

                    if dead:
                        service.last_seen = self._now
                        self.error = None
                        self.fail_count = 0  # Because we killed it..
                        self._remove_dead_service(service)
                        succeeded.append(id)

        ret = {}
        if failed:
            ret["error"] = "Services that failed to be killed: [%s]" % ", ".join(failed)
        if succeeded:
            ret["message"] = "Services killed: [%s]" % ", ".join(succeeded)
        return ret

    def _processing_operation(self, ids, remote_verb, remote_command, infinitive_str, past_tense_str, required_status_list, wait=None):
        succeeded = []
        failed = []
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to %s a non-existing service '%s'." % (infinitive_str, id))
                failed.append(id)
                continue
            else:
                service = self._services[id]
                ss = self._get_status(service)
                if not ss in required_status_list:
                    self.log.debug("Tried to %s a service with status '%s'." % (infinitive_str, ss))
                    failed.append(id)
                    continue

                error = None
                try:
                    data = None
                    if wait is not None:
                        data = {"wait": wait}
                    content = self.remote(service.addr, remote_verb, remote_command, data)
                    error = content.get("error")
                except Exception as e:
                    error = str(e)
                if error:
                    self.log.warning("Service '%s' failed to %s: %s" % (id, infinitive_str, error))
                    failed.append(id)
                else:
                    self.log.info("%s message sent to service '%s'." % (infinitive_str.capitalize(), id)) #SPECIAL
                    succeeded.append(id)

        ret = {}
        if failed:
            ret["error"] = "Service not %s: [%s]" % (past_tense_str, ", ".join(failed))
        if succeeded:
            ret["message"] = "Services %s: [%s]" % (past_tense_str, ", ".join(succeeded))
        return ret

    def _start_processing(self, ids):
        return self._processing_operation(ids, "post", "start", "start", "started", [status.IDLE, status.ABORTED])

    def _stop_processing(self, ids, wait):
        return self._processing_operation(ids, "post", "stop", "stop", "stopped", [status.PROCESSING, status.PENDING, status.SUSPENDED], wait)

    def _abort_processing(self, ids):
        return self._processing_operation(ids, "post", "abort", "abort", "aborted", [status.PROCESSING, status.PENDING, status.SUSPENDED, status.STOPPING])

    def _suspend_processing(self, ids):
        return self._processing_operation(ids, "post", "suspend", "suspend", "suspended", [status.PROCESSING])

    def _resume_processing(self, ids):
        return self._processing_operation(ids, "post", "resume", "resume", "resumed", [status.SUSPENDED])

    #endregion Service interface helpers

    #region Service overrides

    def get_stats(self):
        stats = super(ServiceManager, self).get_stats()
        available_ports = {}
        for host, ports in self._available_ports.iteritems():
            available_ports[host] = len(ports)
        stats["available_ports"] = available_ports
        return stats

    #TODO: ALL OF THE BELOW++, fix them when I work with metadata

    def on_status(self):
        return {"timer": self._timer.status}

    def on_start(self):
        self._timer.start()
        return True

    def on_restart(self):
        self._timer.restart()
        return True

    def on_stop(self):
        self._timer.stop()
        self._timer.wait()
        return True

    def on_abort(self):
        self._timer.abort()
        self._timer.wait()
        return True

    def on_suspend(self):
        self._timer.suspend()
        return True

    def on_resume(self):
        self._timer.resume()
        return True

    #endregion Service overrides

    #
    # # TODO: OLD STUFF
    # def _mgmt_push(self, payload, *args, **kwargs):
    #     print "Pushing configs"
    #     for remote_id, value in self._registered_services.iteritems():
    #         remote_endpoint, remote_config_keys = value
    #         print "Pushing config to '%s' at '%s'" % (remote_id, remote_endpoint)
    #         # Push config to remote host
    #         req = json.dumps(self._config) #payload)
    #         res = requests.post("http://%s/update" % remote_endpoint, req) #, auth=('user', '*****'))
    #
    #     return {"message": "Config pushed."}

