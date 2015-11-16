#!/usr/bin/env python
# -*- coding: utf-8 -*-

from eslib.service import status
from eslib.service.ServiceLauncher import ServiceLauncherBase
from eslib.service import dicthelp
from eslib.procs import Timer
from eslib.time import utcdate
from eslib import esdoc
import datetime, Queue, os, signal
from threading import Lock
from copy import deepcopy

from elasticsearch import Elasticsearch
from elasticsearch.client.indices import IndicesClient
from elasticsearch.helpers import scan

from .ServiceLauncher import ServiceLauncher  # Just to get the type name


class ServiceInfo(object):
    def __init__(self):
        # Persisted permanent data
        self.id         = None
        self.guest      = False        # Guests register/unregister themselves
        self.host       = None         # Host name (not port) where service should run (or guest IS running)
        self.type       = None
        self.config_key = None         # Section tag for service config in global config
        self.metadata_keys = []        # Sections/fields in the metadata that the service subscribes to
        self.fixed_port = False        # Whether port is given and insisted on, as opposed to dynamic
        self.boot_state = status.DEAD  # Whether it should be dead, running idle or processing after boot
        # Persisted temporary data
        self.port       = None         # Port number assigned by manager or insisted by guest
        self.pid        = None         # OS Process ID
        self.last_seen  = None
        # Not persisted
        self.fail_count = 0
        self.status     = status.DEAD
        self.error      = None
        self.metadata_pending = None

    @property
    def addr(self):
        return "%s:%d" % (self.host, self.port) if self.port is not None else self.host

    def set_ok(self):
        self.error = None
        self.fail_count = 0

class Metadata(object):
    # status/state constants
    HISTORIC = "historic"
    EDIT     = "edit"
    ACTIVE   = "active"

    def __init__(self):
        self.version     = 0
        self.updated     = None
        self.status      = None
        self.description = ""
        self.data        = {}

    def clone(self):
        "Create and return a shallow clone."
        item = Metadata()
        item.version = self.version
        item.updated = self.updated
        item.status  = self.status
        item.data    = self.data
        return item


class ServiceManager(ServiceLauncherBase):

    def __init__(self, **kwargs):
        super(ServiceManager, self).__init__(**kwargs)

        self.config.set_default(
            elasticsearch_hosts     = ["localhost:9200"],
            elasticsearch_index     = "management",

            dynamic_port_ranges     = [("localhost", 5001, 5099)],
            connection_timeout      = (3.5, 10.0)  # max seconds to wait for connection to a service, max wait for read
        )

        self._es = None
        self._es_index = None

        self._services = {}
        self._metadata_versions = {}
        self._metadata_use      = None
        self._metadata_edit     = None
        self._metadata_next_version = 0
        self._metadata_lock     = Lock()
        self._metadata_pending  = None

        self._available_ports = {}

        self._timer = None
        self._upload_queue = None

        self._reboot_wait  = []  # List of services we are currently rebooting; just waiting for 'goodbye' first.
        self._reboot_ready = []  # Services that we can now safely try to boot again. (TODO: May require a grace time to endure lingering locks to limited resources, such as ip-ports, limited number of DB connections, etc.)

        # Clear routes and re-enable a minimal set of calls that make sense here
        self._routes = []
        # Use those in HttpService:
        self.add_route(self._mgmt_hello             , "GET"     , "/hello"      , None)
        self.add_route(self._mgmt_help              , "GET"     , "/help"       , None)
        self.add_route(self._mgmt_debug_free        , "DELETE"  , "/debug/free" , None)

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

        self.add_route(self._mgmt_service_set_boot  , "POST|PUT", "/set_boot_state"    , None)
        self.add_route(self._mgmt_service_boot      , "POST|PUT", "/boot"              , None)
        self.add_route(self._mgmt_service_reboot    , "POST|PUT", "/reboot"            , None)

        self.add_route(self._mgmt_metadata_list     , "GET"     , "/meta/list"         , None)
        self.add_route(self._mgmt_metadata_commit   , "PUT|POST", "/meta/commit"       , None)
        self.add_route(self._mgmt_metadata_rollback , "PUT|POST", "/meta/rollback/{version:int}", None)
        self.add_route(self._mgmt_metadata_drop     , "DELETE"  , "/meta/drop/{version:int}", None)
        self.add_route(self._mgmt_metadata_import   , "PUT|POST", "/meta/import"       , ["?commit:bool", "?message:str"])
        self.add_route(self._mgmt_metadata_get      , "GET"     , "/meta/{?version}"   , ["?path:str"])

        self.add_route(self._mgmt_metadata_put      , "PUT|POST", "/meta/put"          , ["?path:str", "?merge:bool"])
        self.add_route(self._mgmt_metadata_remove   , "DELETE"  , "/meta/remove"       , None)
        self.add_route(self._mgmt_metadata_delete   , "DELETE"  , "/meta/delete"       , None)

    @property
    def _now(self):
        return datetime.datetime.utcnow()

    def on_configure(self, credentials, config, global_config):
        super(ServiceManager, self).on_configure(credentials, config, global_config)
        self.config.set(
            elasticsearch_hosts     = config["elasticsearch_hosts"],
            elasticsearch_index     = config["elasticsearch_index"],

            dynamic_port_ranges     = config.get("dynamic_port_ranges"),

            all_interfaces          = config.get("all_interfaces")  # Listen on all interfaces (0.0.0.0) if set
        )

    def on_setup(self):
        self._es = Elasticsearch(hosts=self.config.elasticsearch_hosts)
        self._es_index = self.config.elasticsearch_index

        self._load_services()
        self._load_metadata()
        self._probe_loaded_services()  # find PID and IP port for services

        self._upload_queue = Queue.Queue()

        self._timer = Timer(actions=[(1, 1, "ping")])
        self._timer.add_callback(self._ping)

        procs = [self._timer]

        # Link procs (just one in this case..)
        self.link(*procs)

        #  Register them for debug dumping
        self.register_procs(*procs)

        # For dynamic port allocation:
        self._available_ports = {}
        for dpr in self.config.dynamic_port_ranges:
            ports = self._available_ports[dpr[0]] = []
            for port in range(dpr[1], dpr[2]+1):
                ports.append(port)

        return True

    def _get_metadata_for_service(self, metaitem, service):
        # Note: expects metaitem to be constant during this operation

        meta = Metadata()
        meta.version = metaitem.version
        meta.updated = metaitem.updated
        # irrelevant: meta.status, meta.description
        for complex_path in service.metadata_keys:
            aa = complex_path.split("=>")
            path = aa[0].strip()
            alias = aa[1].strip() if len(aa) > 1 else path
            value = dicthelp.get(metaitem.data, path)
            esdoc.putfield(meta.data, alias, value)
        return meta

    def _ping(self, proc, doc):
        # Reboot handling
        if self._reboot_ready:
            # Only do one each tick...
            service = self._reboot_ready.pop(0)
            self.log.info("Booting service '%s' as part of reboot request." % service.id)
            self._boot([service.id])

        # Metadata change handling
        if self._metadata_pending:
            old_item, new_item = self._metadata_pending
            self._metadata_pending = None

            # TODO: Create a diff to see which sections are modified...

            for service in list(self._services.values()):
                # TODO: Ask each service for latest status instead?
                if service.status != status.DEAD and service.metadata_keys:
                    # TODO: In this naiive implementation we do not know if there is an actual need to send a metadata update, so we send every time:
                    meta = self._get_metadata_for_service(new_item, service)
                    service.metadata_pending = meta

        # TODO: This could rather spawn a thread for each service and operate in parallell, but for now we loop through them:
        for service in list(self._services.values()):
            if service.metadata_pending:
                meta = service.metadata_pending
                service.metadata_pending = None
                error = None
                try:
                    payload = {
                        "version": meta.version,
                        "updated": meta.updated,
                        "data": meta.data
                    }
                    content = self.remote(service.addr, "post", "metadata", payload)
                except Exception as e:
                    error = str(e)
                    msg = "Failed to communicate with service '%s' at '%s': %s" % (service.id, service.addr, e)
                    self.log.warning(msg)

                if error:
                    service.error = error
                    service.fail_count += 1
                    if service.fail_count >= 0:  # TODO: Always remove it for now
                        self._remove_dead_service(service)

    #region Storage :: services

    _matchall_query = {'query': {'match_all': {}}}

    def _storage_load_services(self):
        self.log.info("Loading registered services.")
        docs = []
        try:
            ic = IndicesClient(self._es)
            # If index does not exist, create it:
            if not ic.exists(self._es_index):
                self.log.info("Management index '%s' did not exist. Creating it." % self._es_index)
                ic.create(self._es_index)
            # ensure all documents are indexed before executing query
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
            service.boot_state    = s.get("boot_state")
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
            "boot_state"   : service.boot_state,
            "port"         : service.port,
            "pid"          : service.pid,
            "last_seen"    : service.last_seen  # elasticsearch api will serialize this properly
            # not persisting "fail_count", "error", "status"
        }

        try:
            self._es.index(self._es_index, doc_type="service", id=service.id, body=payload)
        except Exception as e:
            self.log.error("Failed to save registered service '%s' to index. %s: %s" % (service.id, e.__class__.__name__, e))
            # Report it as operation failed, but it is only persisting that has failed... no big deal.
            return False

    def _storage_delete_service(self, id):
        try:
            ic = IndicesClient(self._es)
            ic.refresh(index=self._es_index)
            res = self._es.delete(index=self._es_index, doc_type="service", id=id)
            return True
        except Exception as e:
            self.log.error("Failed to delete registered service '%s' from index. %s: %s" % (id, e.__class__.__name__, e))
            return False

    #endregion Storage :: services

    #region Storage :: metadata

    _meta_versions_query = {"_source": ["status", "updated", "description"]}

    def _storage_load_meta_list(self):
        self.log.info("Loading metadata.")
        docs = []
        try:
            ic = IndicesClient(self._es)
            # If index does not exist, create it:
            if not ic.exists(self._es_index):
                self.log.info("Management index '%s' did not exist. Creating it." % self._es_index)
                ic.create(self._es_index)
            # ensure all documents are indexed before executing query
            ic.refresh(self._es_index)

            # Load version list
            scan_resp = scan(self._es, index=self._es_index, doc_type='metadata', query=self._meta_versions_query)
            docs = [doc for doc in scan_resp]
        except Exception as e:
            self.log.critical("Failed to load metadata version list from index. Aborting. %s: %s" % (e.__class__.__name__, e))
            exit()

        use_id = None
        edit_id = None

        meta_versions = {}
        for doc in docs:
            s = doc["_source"]
            meta = Metadata()
            meta.version       = int(doc["_id"])
            meta.status        = s["status"]
            meta.updated       = utcdate(s["updated"])
            meta.description   = s["description"]
            if meta.status == Metadata.EDIT:
                edit_id = meta.version
            elif meta.status == Metadata.ACTIVE:
                use_id = meta.version
            # no meta.data here
            meta_versions[meta.version] = meta
        return (meta_versions, use_id, edit_id)

    def _storage_load_meta_item(self, version):
        self.log.debug("Loading metadata item with version %d." % version)
        # ensure all documents are indexed before executing query
        try:
            ic = IndicesClient(self._es)
            ic.refresh(self._es_index)

            doc = self._es.get(self._es_index, doc_type="metadata", id=version)
            s = doc["_source"]
            meta = Metadata()
            meta.version       = int(doc["_id"])
            meta.status        = s["status"]
            meta.updated       = utcdate(s["updated"])
            meta.description   = s["description"]
            meta.data          = s["data"]
            return meta
        except Exception as e:
            self.log.error("Failed to load specific metadata item %d from index." % version)
            return None

    def _storage_save_meta_item(self, metaitem):
        payload = {
            "status"       : metaitem.status,
            "updated"      : metaitem.updated,
            "description"  : metaitem.description,
            "data"         : metaitem.data
        }

        try:
            self._es.index(self._es_index, doc_type="metadata", id=metaitem.version, body=payload)
        except Exception as e:
            self.log.error("Failed to save metadata item %d to index. %s: %s" % (metaitem.version, e.__class__.__name__, e))
            # Report it as operation failed, but it is only persisting that has failed... no big deal.
            return False

    def _storage_delete_meta_item(self, version):
        try:
            ic = IndicesClient(self._es)
            ic.refresh(index=self._es_index)
            res = self._es.delete(index=self._es_index, doc_type="metadata", id=version)
            return True
        except Exception as e:
            self.log.error("Failed to delete metadata item %d from index. %s: %s" % (version, e.__class__.__name__, e))
            return False

    #endregion Storage :: metadata

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

    #region Remote launcher helpers

    def _on_remote_host(self, service):
        host, port = self.config.management_endpoint.split(":")
        return service.host != host

    def _get_remote_launcher(self, service):
        launcher_type_name = ServiceLauncher.__name__
        for candidate in self._services.values():
            if (candidate.type == launcher_type_name) and (service.host == candidate.host):
                return candidate
        return None

    #endregion Remote launcher helpers

    #region Helper methods :: services

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
                metakeys = content["meta_keys"]

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
        service.metadata_pending = None
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

    #endregion Helper methods :: services

    #region Helper methods :: metadata

    def _load_or_create_meta_item(self, version, status, data=None):
        meta = None
        if version is None:
            meta = Metadata()
            meta.version = self._metadata_next_version
            self._metadata_next_version += 1
            meta.updated = self._now
            meta.status = status
            if data:
                meta.data = data
            self.log.debug("Creating new metadata item; version=%d, status='%s'." % (meta.version, meta.status))
            self._metadata_versions[meta.version] = meta
            self._storage_save_meta_item(meta)
        else:
            meta = self._storage_load_meta_item(version)
            self._metadata_versions[meta.version] = meta
        return meta

    def _load_metadata(self):
        self._metadata_versions, use_id, edit_id = self._storage_load_meta_list()

        self._metadata_next_version = (edit_id +1 if edit_id else 0)

        # Load or create active item and edit item
        self._metadata_use  = self._load_or_create_meta_item(use_id, Metadata.ACTIVE)
        self._metadata_edit = self._load_or_create_meta_item(edit_id, Metadata.EDIT)

        self.log.info("Metadata loaded.")

    def _commit_metadata_edit_set(self, description=None):

        self.log.debug("Committing metadata edit set.")

        with self._metadata_lock:

            old_edit   = self._metadata_edit
            old_active = self._metadata_use

            # Create a new edit set
            self._metadata_edit = self._load_or_create_meta_item(None, Metadata.EDIT, deepcopy(old_edit.data))  # This will create and save
            # Make old edit set active
            old_edit.description = description or ""
            old_edit.status = Metadata.ACTIVE
            old_edit.updated = self._now
            self._metadata_use = old_edit
            self._storage_save_meta_item(self._metadata_use)
            # Make old active set historic
            old_active.status = Metadata.HISTORIC
            old_active.updated = self._now
            old_active_clone = old_active.clone()
            self._storage_save_meta_item(old_active)
            # The old data is not needed in the versions cache one we have saved it:
            old_active.data = {}

            self._metadata_pending = (old_active_clone, self._metadata_use.clone())

            self.log.status("Metadata edit set committed to active; version=%d." % self._metadata_use.version)

    # Currently obsolete and replaced with _rollback_edit_metadata, but kept here for now; just in case...
    def _rollback_active_metadata(self, version):
        # version must be int

        self.log.debug("Attempting to roll back metadata active set to version %d." % version)

        with self._metadata_lock:

            # Load old version
            loaded = None
            for item in self._metadata_versions.values():
                if item.version == version:
                    if item.status != Metadata.HISTORIC:
                        self.log.error("Tried to rollback metadata active set to non-historic version %d; status='%s'." % (item.version, item.status))
                        return False
                    loaded =  self._storage_load_meta_item(item.version)
                    self._metadata_versions[item.version] = loaded
                    break

            if not loaded:
                self.log.error("Failed to load version %d for rollback; version not found." % version)
                return False

            loaded.status = Metadata.ACTIVE
            loaded.updated = self._now
            self._storage_save_meta_item(loaded)

            old_active = self._metadata_use
            self._metadata_use = loaded

            # Make old active set historic
            old_active.status = Metadata.HISTORIC
            old_active.updated = self._now
            old_active_clone = old_active.clone()
            self._storage_save_meta_item(old_active)
            # The old data is not needed in the versions cache one we have saved it:
            old_active.data = {}

            self._metadata_pending = (old_active_clone, self._metadata_use.clone())

            self.log.status("Active metadata set rolled back to version %d." % self._metadata_use.version)

        return True

    def _rollback_edit_metadata(self, version):
        # version must be int

        self.log.debug("Attempting to roll back metadata edit set to version %d." % version)

        with self._metadata_lock:

            # Load old version
            loaded = None
            for item in self._metadata_versions.values():
                if item.version == version:
                    if item.status == Metadata.EDIT:
                        self.log.error("Tried to rollback metadata edit set to self; version %d, status='%s'." % (item.version, item.status))
                        return False
                    loaded =  self._storage_load_meta_item(item.version)
                    self._metadata_versions[item.version] = loaded
                    break

            if not loaded:
                self.log.error("Failed to load version %d for rollback; version not found." % version)
                return False

            # Replace data in current edit set
            self._metadata_edit.data = deepcopy(loaded.data)
            self._metadata_edit.updated = self._now
            self._storage_save_meta_item(self._metadata_edit)

            self.log.status("Metadata edit set copied from version %d." % self._metadata_use.version)

        return True

    def _delete_metadata_version(self, version):
        # version must be int

        self.log.debug("Attempting to delete metadata version %d." % version)

        with self._metadata_lock:

            # Find old version
            found = None
            for item in self._metadata_versions.values():
                if item.version == version:
                    if item.status != Metadata.HISTORIC:
                        self.log.error("Tried to delete a non-historic version of metadata; version=%d, status='%s'." % (item.version, item.status))
                        return False
                    found = item
                    break

            if not found:
                self.log.error("Failed to delete metadata version %d; version not found." % version)
                return False

            deleted = self._storage_delete_meta_item(found.version)
            if deleted:
                del self._metadata_versions[found.version]
                self.log.info("Historic metadata version %d deleted." % found.version)
            return deleted

        return True

    def _import_metadata(self, data, commit, message):
        # expects data to be valid dict

        with self._metadata_lock:
            self._metadata_edit.data = data
            self._metadata_edit.updated = self._now
            if not commit:  # Will otherwise be saved during the commit
                self._storage_save_meta_item(self._metadata_edit)
        self.log.info("New metadata imported to edit set.")
        if commit:
            self._commit_metadata_edit_set(message)

    #endregion Helper methods :: metadata

    #region Service interface commands

    def _mgmt_service_add(self, request_handler, payload, **kwargs):
        self.log.debug("called: add service '%s'" % payload.get("id"))
        return self._add_service(
            payload.get("guest") or False,  # guest
            payload.get("id"),              # service id
            payload.get("host"),            # hostname for where to run this service (or where guest IS running)
            payload.get("port"),            # port service insists to run admin port on
            payload.get("boot_state") or status.DEAD,  # boot state
            payload.get("config_key"),      # config key
            payload.get("start") or False,  # auto_start
        )

    def _mgmt_service_remove(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: remove service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._remove_services(
            ids,
            payload.get("all") or False,
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

        self.log.trace("called: list services")
        services = list(self._services.values())
        services.append(self)  # Add this object (NB: it is a different class than the other services)
        return self._get_service_info(services)

    def _mgmt_service_stats(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []

        if not ids:
            return {self.name: self._get_own_service_info()}

        self.log.debug("called: get stats for service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        missing = [id for id in ids if id not in self._services]
        fetch = [service for service in self._services.values() if service.id in ids]
        if self.name in ids:
            fetch.append(self)  # Add this object to be fetched (NB: it is a different class from the other service info objects)
        ret = self._get_service_info(fetch, return_missing=True)
        for id in missing:
            if not id in ret:
                ret[id] = {"error": "Service '%s' not found." % id}
        return ret

    def _mgmt_service_run(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: run service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._run_services(
            ids,
            payload.get("all") or False,
            payload.get("start") or False
        )

    def _mgmt_service_shutdown(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: shutdown service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._shutdown_services(
            ids,
            payload.get("all") or False,
            payload.get("wait") or False
        )

    def _mgmt_service_kill(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: kill service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._kill_services(
            ids,
            payload.get("all") or False,
            payload.get("force") or False
        )

    def _mgmt_processing_start(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: start service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._start_processing(
            ids,
            payload.get("all") or False
        )

    def _mgmt_processing_stop(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: stop service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._stop_processing(
            ids,
            payload.get("all") or False,
            payload.get("wait") or False
        )

    def _mgmt_processing_abort(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: abort service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._abort_processing(
            ids,
            payload.get("all") or False
        )

    def _mgmt_processing_suspend(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: suspend service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._suspend_processing(
            ids,
            payload.get("all") or False
        )

    def _mgmt_processing_resume(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: resume service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._resume_processing(
            ids,
            payload.get("all") or False
        )

    def _mgmt_service_reboot(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: reboot service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._reboot_services(
            ids,
            payload.get("all") or False,
        )

    def _mgmt_service_set_boot(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        self.log.debug("called: set boot state for service(s) [%s]" % ("(all)" if payload.get("all") else ", ".join(ids)))
        return self._set_boot(
            ids,
            payload.get("boot_state"),
            payload.get("all") or False
        )

    def _mgmt_service_boot(self, request_handler, payload, **kwargs):
        ids = payload.get("ids") or []
        all = payload.get("all")
        if not ids:
            all = True
        self.log.debug("called: boot service(s) [%s]" % ("(all)" if all else ", ".join(ids)))
        return self._boot(
            ids,
            all
        )

    #endregion Service interface commands

    #region Service interface helpers

    def _get_own_service_info(self):
        host, port = self.config.management_endpoint.split(":")

        return {
            "id"         : self.name,
            "guest"      : True,
            "host"       : host,
            "type"       : self.__class__.__name__,
            # no "boot_state" for this one..
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
            if service == self:
                ret[self.name] = self._get_own_service_info()
                continue
            ss, stats = self._get_stats(service, save=False)
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
                    "boot_state" : service.boot_state,
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

    def _parse_boot_state(self, boot_state_str):
        if boot_state_str is None:
            return status.DEAD
        boot_state_str = boot_state_str.lower()
        for s in [status.DEAD, status.IDLE, status.PROCESSING]:
            if boot_state_str == s.lower():
                return s
        return None  # Parsing failed; illegal string

    def _add_service(self, guest, id, host, port, boot_state, config_key, auto_start, save=True):
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

        boot_state_parsed = self._parse_boot_state(boot_state)
        service.boot_state = boot_state_parsed or status.DEAD

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
        if boot_state is None:
            ret["warning"] = "Unknown or illegal boot state '%s' ignored."
        return ret

    def _remove_services(self, ids, all, auto_stop):
        succeeded = []
        failed = []
        if all:
            ids = self._services.keys()[:]
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
            elif not all:
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
            self._add_service(guest, id, host, port, None, None, False, save=False)

        service = self._services[id]
        service.config_key    = config_key
        service.type          = service_type
        service.pid           = pid
        service.status        = status
        service.metadata_keys = metakeys or []
        service.port          = port or dynport
        service.last_seen     = self._now
        service.error         = None
        service.fail_count    = 0
        service.metadata_pending = None
        self._storage_save_service(service)

        meta = self._get_metadata_for_service(self._metadata_use.clone(), service)
        meta_payload = {
            "version": meta.version,
            "updated": meta.updated,
            "data": meta.data
        }

        ret = {
            "port": service.port,
            "metadata": meta_payload
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

        # Reboot handling
        if service in self._reboot_wait:
            self._reboot_wait.remove(service)
            self._reboot_ready.append(service)
            # The runner thread should now launch the service when it sees fit.

        extra_info = ""
        if self._release_port(service.host, service.port):
            extra_info = " Dynamic port '%s' released back to pool." % service.addr
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

    def _run_services(self, ids, all=False, start=None):
        succeeded = []
        failed = []
        if all:
            ids = self._services.keys()[:]
        for id in ids:
            if not all and not id in self._services:
                self.log.debug("Tried to launch a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                if self._launch_service(service, start):  # Does all necessary logging itself
                    succeeded.append(id)
                else:
                    failed.append(id)

        ret = {}
        if failed:
            ret["error"] = "Services that failed to launch: [%s]" % ", ".join(failed)
        if succeeded:
            ret["message"] = "Services launched: [%s]" % ", ".join(succeeded)
        return ret

    def _launch_service(self, service, start):
        if self._on_remote_host(service):
            return self._launch_remote_service(service, start)
        else:
            return self._launch_local_service(service, start)

    def _launch_remote_service(self, service, start):
        self.log.debug("Launching service '%s' on remote host at '%s'." % (service.id, service.addr))

        launcher = self._get_remote_launcher(service)
        if not launcher:
            self.log.error("Launching service '%s' requires a launcher on host '%s'. None found." % (service.id, service.host))
            return False

        config_dict = self.load_config()

        data = {
            "id"              : service.id,
            "config"          : config_dict,
            "config_key"      : service.config_key,
            "endpoint"        : service.addr,
            "manager_endpoint": self.config.management_endpoint,  # This manager address
            "start"           : start
        }
        error = None
        try:
            content = self.remote(launcher.addr, "post", "launch", data)
            error = content.get("error")
        except Exception as e:
            error = str(e)
        if error:
            self.log.error("Error communicating with launcher at '%s': %s" % (launcher.addr, error))
            return False
        pid = content.get("pid")
        if not pid:
            return False

        service.pid = pid
        # No need to save this yet; wait for registration call to do that.

        self.log.status("Service '%s' launched on remote host at '%s' with pid=%d." % (service.id, service.addr, service.pid))

        return True

    def _launch_local_service(self, service, start):

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

        config_dict = self.load_config()

        p = None
        try:
            p = self.spawn(
                service.id,
                config_dict, service.config_key,
                service.addr,  # Own address
                self.config.management_endpoint,  # This manager address
                start)
        except Exception as e:
            self.log.exception("Failed to launch service '%s' at '%s'." % (service.id, service.addr))
            service.error = str(e)
            service.fail_count += 1
            return False

        service.pid = p.pid
        service.error = None
        service.fail_count = 0
        service.status = status.DEAD
        # For the rest, wait for service to say 'hello' back.
        #service.last_seen = self._now
        #self._storage_save_service()

        self.log.status("Service '%s' launched at '%s' with pid=%d." % (service.id, service.addr, service.pid))
        return True

    def _shutdown_services(self, ids, all, wait):
        # Since it has the same inner code as a processing operation:
        return self._processing_operation(ids, "delete", "shutdown", "shut down", "shut down", [status.IDLE, status.ABORTED, status.PROCESSING, status.SUSPENDED], all, wait)

    def _kill_services(self, ids, all, force):
        # This is only for extreme cases.
        # Normally, a shutdown should do the trick.

        succeeded = []
        failed = []
        if all:
            ids = self._services.keys()[:]
        for id in ids:
            if not all and not id in self._services:
                self.log.debug("Tried to kill a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                if not service.pid:
                    self.log.debug("Missing pid for service '%s'; cannot kill." % id)
                    failed.append(id)
                else:
                    dead = False
                    if force:
                        self.log.debug("Forcefully kill service '%s', pid=%d." % (id, service.pid))
                    else:
                        self.log.debug("Attempting to kill service '%s', pid=%d." % (id, service.pid))

                    if self._on_remote_host(service):

                        # Kill on remote

                        launcher = self._get_remote_launcher(service)
                        if not launcher:
                            self.log.error("Killing service '%s' requires a launcher on host '%s'. None found." % service.name, service.host)
                            failed.append(id)
                        else:
                            data = {
                                "id"   : service.id,
                                "pid"  : service.pid,
                                "force": force
                            }
                            error = None
                            try:
                                content = self.remote(launcher.addr, "delete", "kill", data)
                                error = content.get("error")
                            except Exception as e:
                                error = str(e)
                            if error:
                                self.log.error("Error communicating with launcher at '%s': %s" % (launcher.addr, error))
                                failed.append(id)
                            dead = content.get("killed") or False
                            # It may not be dead quite yet, but we have successfully submitted a kill signal,
                            # so it is hopefully going down.
                            succeeded.append(id)

                    else:

                        # Kill locally

                        try:
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
                            else:
                                # It may not be dead quite yet, but we have successfully submitted a kill signal,
                                # so it is hopefully going down.
                                succeeded.append(id)

                    if dead:
                        service.last_seen = self._now
                        self.error = None
                        self.fail_count = 0  # Because we killed it..
                        self._remove_dead_service(service)
                        #succeeded.append(id)

        ret = {}
        if failed:
            ret["error"] = "Services that failed to be killed: [%s]" % ", ".join(failed)
        if succeeded:
            ret["message"] = "Services killed: [%s]" % ", ".join(succeeded)
        return ret

    def _processing_operation(self, ids, remote_verb, remote_command, infinitive_str, past_tense_str, required_status_list, all=False, wait=False):
        succeeded = []
        failed = []
        if all:
            # ServiceLauncher type should not be affected
            launcher_type_name = ServiceLauncher.__name__
            ids = [key for key,service in self._services.iteritems() if service.type != launcher_type_name]
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to %s a non-existing service '%s'." % (infinitive_str, id))
                failed.append(id)
                continue
            else:
                service = self._services[id]
                ss = self._get_status(service)
                if not ss in required_status_list:
                    if not all:
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

    def _start_processing(self, ids, all):
        return self._processing_operation(ids, "post", "start", "start", "started", [status.IDLE, status.ABORTED], all)

    def _stop_processing(self, ids, all, wait):
        return self._processing_operation(ids, "post", "stop", "stop", "stopped", [status.PROCESSING, status.PENDING, status.SUSPENDED], all, wait)

    def _abort_processing(self, ids, all):
        return self._processing_operation(ids, "post", "abort", "abort", "aborted", [status.PROCESSING, status.PENDING, status.SUSPENDED, status.STOPPING], all)

    def _suspend_processing(self, ids, all):
        return self._processing_operation(ids, "post", "suspend", "suspend", "suspended", [status.PROCESSING], all)

    def _resume_processing(self, ids, all):
        return self._processing_operation(ids, "post", "resume", "resume", "resumed", [status.SUSPENDED], all)

    def _set_boot(self, ids, boot_state, all=False, wait=False):
        boot_state_parsed = self._parse_boot_state(boot_state)
        if boot_state_parsed is None:
            return {"error": "Unknown or illegal boot state '%s'." % boot_state}

        succeeded = []
        failed = []
        if all:
            ids = self._services.keys()[:]
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to set boot state for a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                service.boot_state = boot_state_parsed
                succeeded.append(id)
                # Persist
                self._storage_save_service(service)

        ret = {}
        if failed:
            ret["error"] = "Failed to set boot state for services: [%s]" % (", ".join(failed))
        if succeeded:
            ret["message"] = "Boot state set to '%s' for services: [%s]" % (boot_state_parsed, ", ".join(succeeded))
        return ret

    def _boot(self, ids, all=False):
        succeeded = []
        failed = []
        if all:
            ids = self._services.keys()[:]
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to boot a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                boot_state = service.boot_state
                if boot_state is None or boot_state == status.DEAD:
                    continue  # Booting not required
                # Check the current status...
                ss = self._get_status(service)
                if ss == status.DEAD:
                    # Launch service
                    start = False
                    if boot_state == status.PROCESSING:
                        start = True
                    ok = self._launch_service(service, start)
                    if ok:
                        succeeded.append(id)
                    else:
                        failed.append(id)
                elif ss in [status.PROCESSING, status.SUSPENDED, status.PENDING, status.STOPPING, status.CLOSING]:
                    continue  # Already running, so booting not required
                elif boot_state == status.PROCESSING:
                    # Start processing
                    res = self._start_processing([id], False)
                    error = res.get("error")
                    if error:
                        failed.append(id)
                    else:
                        succeeded.append(id)

        ret = {}
        if failed:
            ret["error"] = "Services that failed to boot: [%s]" % (", ".join(failed))
        if succeeded:
            ret["message"] = "Services booted: [%s]" % (", ".join(succeeded))
        return ret

    def _reboot_services(self, ids, all):
        # When we reboot a service, we first register that we are rebooting, then we shut it down,
        # and when we get a 'goodbye' message, we mark it to be booted again. The service tick should
        # do the actual rebooting.

        succeeded = []
        failed = []
        if all:
            ids = self._services.keys()[:]
        for id in ids:
            if not id in self._services:
                self.log.debug("Tried to reboot a non-existing service '%s'." % id)
                failed.append(id)
                continue
            else:
                service = self._services[id]
                ss = self._get_status(service)
                if ss == status.DEAD:
                    self._reboot_ready.append(service)
                else:
                    error = None
                    try:
                        data = None
                        content = self.remote(service.addr, "delete", "shutdown", data)
                        error = content.get("error")
                    except Exception as e:
                        error = str(e)
                    if error:
                        self.log.warning("Service '%s' failed to shut down: %s" % (id, error))
                        failed.append(id)
                    else:
                        self.log.info("Shutdown message sent to service '%s'." % id)
                        succeeded.append(id)
                        self._reboot_wait.append(service)

        ret = {}
        if failed:
            ret["error"] = "Service not shut down for reboot: [%s]" % (", ".join(failed))
        if succeeded:
            ret["message"] = "Services now pending reboot: [%s]" % (", ".join(succeeded))
        return ret

    #endregion Service interface helpers

    #region Metadata interface commands

    def _mgmt_metadata_list(self, request_handler, payload, **kwargs):
        self.log.debug("called: list metadata versions")

        retlist = []
        for v in list(self._metadata_versions.values()):
            retlist.append({
                "version"    : v.version,
                "status"     : v.status,
                "updated"    : v.updated,
                "description": v.description
            })
        return {"versions": retlist}

    def _mgmt_metadata_commit(self, request_handler, payload, **kwargs):
        self.log.debug("called: commit metadata")

        self._commit_metadata_edit_set(payload.get("description") if payload else None)
        return {"message": "Metadata edit set version %d committed to active." % self._metadata_use.version}

    def _mgmt_metadata_rollback(self, request_handler, payload, **kwargs):
        self.log.debug("called: rollback metadata")

        version = kwargs["version"]  # Should be an int when it gets here

        if self._rollback_edit_metadata(version):
            return {"message": "Metadata edit set rolled back to version %d." % self._metadata_use.version}
        else:
            return {"error": "Failed to roll back metadata edit set to version %d." % version}

    def _mgmt_metadata_drop(self, request_handler, payload, **kwargs):
        self.log.debug("called: delete (drop) metadata set")

        version = kwargs["version"]  # Should be an int when it gets here

        if self._delete_metadata_version(version):
            return {"message": "Metadata version %d deleted." % version}
        else:
            return {"error": "Failed to delete metadata version %d." % version}

    def _mgmt_metadata_import(self, request_handler, payload, **kwargs):
        self.log.debug("called: import metadata")

        commit = kwargs.get("commit") or False
        message = kwargs.get("message")

        if not type(payload) is dict:
            return {"error": "Payload is not a valid dict."}

        self._import_metadata(payload, commit, message)
        extra_str = " and committed" if commit else ""
        return {"message": "New metadata imported to current edit set%s." % extra_str}

    def _mgmt_metadata_get(self, request_handler, payload, **kwargs):
        self.log.debug("called: get metadata")

        version = kwargs.get("version")
        path = kwargs.get("path") or ""
        if version is None or version == "":
            version = Metadata.EDIT
        elif not version in [Metadata.ACTIVE, Metadata.EDIT]:
            # Make sure it is numeric
            try:
                version = int(version)
            except ValueError:
                return {"error": "Version must be one of '%s', '%s', or numeric (int)." % (Metadata.ACTIVE, Metadata.EDIT)}

        with self._metadata_lock:
            metaitem = None

            if version == Metadata.EDIT:
                metaitem = self._metadata_edit
            elif version == Metadata.ACTIVE:
                metaitem = self._metadata_use
            else:
                for item in self._metadata_versions.values():
                    if item.version == version:
                        metaitem = self._storage_load_meta_item(version)
                        break
            if not metaitem:
                return {"error": "Failed to get metadata version %s." % version}

            # The data could possibly be modified between this method returns and the lock
            # is released, and the data is json serialized in the http service layer.
            # So we'd better deep clone here already.

            data = dicthelp.get(metaitem.data, path)

            return {
                "version": metaitem.version,
                "status" : metaitem.status,
                "updated": metaitem.updated,
                "data"   : deepcopy(data)
            }

    def _mgmt_metadata_put(self, request_handler, payload, **kwargs):
        # Can take list in data, and optionally merge instead of replace.
        self.log.debug("called: put/append metadata")

        path  = kwargs.get("path") or ""
        merge_list = kwargs.get("merge") or False

        if not type(payload) is dict:
            return {"error": "Payload is not a valid dict."}

        # path = payload.get("path")
        # data = payload.get("data")
        # merge_list = payload.get("merge") or False

        data = payload

        # if not path:
        #     return {"error": "Missing path."}
        if not isinstance(data, dict):
            return {"error": "Missing data or data not dict."}

        removed = False
        error = False
        with self._metadata_lock:
            try:
                changed = False
                node = dicthelp.get(self._metadata_edit.data, path)
                for k,v in data.iteritems():
                    changed |= dicthelp.put(node, k, v, merge_list)
                if changed:
                    self._storage_save_meta_item(self._metadata_edit)
            except Exception as e:
                error = e

        if error:
            return {"error": "Failed to put data at path '%s': %s" (path, e)}
        else:
            return {"message": "Nothing changed." if not changed else "Something was changed."}

    def _mgmt_metadata_remove(self, request_handler, payload, **kwargs):
        self.log.debug("called: remove metadata list items")

        path = payload.get("path")
        data_list = payload.get("list")

        if not path:
            return {"error": "Missing path."}
        if not data_list:
            return {"error": "Missing list items."}

        removed = False
        error = False
        with self._metadata_lock:
            try:
                removed = dicthelp.remove_list_items(self._metadata_edit.data, path, data_list)
                if removed:
                    self._storage_save_meta_item(self._metadata_edit)
            except Exception as e:
                error = e

        if error:
            return {"error": "Failed to remove data at path '%s': %s" (path, e)}
        else:
            return {"message": "Nothing removed." if not removed else "Something was remove."}

    def _mgmt_metadata_delete(self, request_handler, payload, **kwargs):
        self.log.debug("called: delete metadata sections")

        paths = payload.get("paths")
        collapse = payload.get("collapse") or False

        if not paths:
            return {"error": "Missing paths."}

        deleted = False
        error = False
        with self._metadata_lock:
            try:
                deleted = dicthelp.delete(self._metadata_edit.data, paths, collapse)
                if deleted:
                    self._storage_save_meta_item(self._metadata_edit)
            except Exception as e:
                error = e

        if error:
            return {"error": "Failed to delete sections [%s]: %s" (", ".join(paths), e)}
        else:
            return {"message": "Nothing deleted." if not deleted else "Something was deleted."}

    #endregion Metadata interface commands

    #region Service overrides

    def on_stats(self, stats):
        available_ports = {}
        for host, ports in self._available_ports.iteritems():
            available_ports[host] = len(ports)
        stats["available_ports"] = available_ports

    #endregion Service overrides
