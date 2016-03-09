from .. import Configurable
from ..esdoc import tojson
import json, requests, logging
#TODO: SET UP LOGGING


def remote(host, verb, path, data=None, params=None):
#    print("***VERB=%s, PATH=%s, DATA=" % (verb, path), data)
    if data is None:
        data = {}
    res = requests.request(
        verb.lower(),
        "http://%s/%s" % (host, path),
        data=tojson(data),
        params=params,
        headers={"content-type": "application/json"},
        timeout=(3.5, 60)
    )
    if res.content:
        return json.loads(res.content)
    else:
        return None


class Client(Configurable):

    # We must know either the service name or the direct service address.
    # The service status and stats will be fetched from the service directly; not the manager.
    #
    # config:
    #    manager_address
    #    service_name
    #    service_address
    #
    # Append callback for service events here, signature = func(service_name, event_name, message)
    #
    #    events = []
    #

    def __init__(self, id_, manager=None, service=None, address=None, **kwargs):
        """

        :param id_: An ID identifying this client on the server. (can be anything..)
        :param manager: Address host:port to service manager. Used with 'service' name.
        :param service: Name of service known to the service manager. Used with service 'manager'.
        :param address: Direct address to service, host:port. Will override specification with 'manager' and 'service'.
        :param kwargs:
        :return:
        """
        super(Client, self).__init__(**kwargs)
        self._setup_logging()

        self.config.set_default(
            manager_address = manager or "localhost:4000",
            service_name    = service or None,
            service_address = address or None
        )

        self._service_address = None
        self._service_name    = None

        self.events = []

    def _setup_logging(self):
        self.log  = logging.getLogger("esclient")

    def _remote(self, verb, path, data=None, params=None):
        self._ensure_service_address()
        return remote(self._service_address, verb, path, data, params)

    def _ensure_service_address(self):
        if not self._service_address:
            if self.config.service_address:
                self._service_address = self.config.service_address
                # Talk to the service and get its id/name
                res = remote(self._service_address, "GET", "hello")
                error = res.get("error")
                if error:
                    msg = "(from service) %s" % error
                    self.log.error(msg)
                    raise Exception(msg)
                self._service_name = res["id"]
                self.log.info("Retrieved name for service at address '%s': %s" % (self._service_address, self._service_name))
            else:
                if not self.config.manager_address or not self.config.service_name:
                    msg = "Cannot get service address for manager='%s', service='%s'."
                    self.log.error()
                    raise Exception(msg)
                res = remote(self.config.manager_address, "GET", "stats", {"ids": [self.config.service_name]})
                error = res.get("error")
                if error:
                    msg = "(from service manager) %s" % error
                    self.log.error(msg)
                    raise Exception(msg)
                stats = res.get(self.config.service_name)
                # TODO: REPORT ON ERROR {u'error': u"Service 'remote' not found."}
                name = stats["id"]
                host = stats["host"]
                port = stats["port"]
                self._service_name = name
                self._service_address = "%s:%d" % (host, port)
                self.log.info("Retrieved address for service '%s': %s" % (self._service_name, self._service_address))

    def _check_event(self, doc):
        if doc and isinstance(doc, dict) and "_event" in doc:
            service_name = doc.get("_id")
            event_name = doc.get("_event")
            event_type = doc.get("_type")
            self.log.debug("Event '%s' received from service '%s'; type='%s'" % (event_name, service_name, event_type))
            if self.events:
                for event_handler in self.events:
                    try:
                        event_handler(service_name, event_name, event_type)
                    except Exception as e:
                        self.log.exception("Exception in event handler.")
            return True
        return False

    # Region service status and management

    def start(self):
        res = self._remote("POST", "start")
        error = res.get("error")
        if error:
            self.log.error("Remote service failed to start: %s" % error)
            return False
        return True

    def stop(self):
        res = self._remote("POST", "stop")
        error = res.get("error")
        if error:
            self.log.error("Remote service failed to stop: %s" % error)
            return False
        return True

    def status(self):
        return self._remote("GET", "status").get("status")

    def stats(self):
        return self._remote("GET", "stats")

    def meta(self):
        return self._remote("GET", "metadata")

    def help(self):
        return self._remote("GET", "help")

    #endregion Service status and management

    #region Document operations

    def fetch(self, socket_name=None, limit=1):
        "Returns a generator to iterate returned (non-event) documents."

        params = {}
        if socket_name:
            params["socket"] = socket_name
        if limit is not None:
            params["limit"] = limit
        res = self._remote("GET", "fetch", None, params)
        print "===FETCH RETURNED:", res
        error = res.get("error")
        if error:
            doc = {
                "_id"    : self._service_name,
                "_event" : "error",
                "_type"  : "fetch"
            }
            self._check_event(doc)
        else:
            documents = res.get("documents")
            if documents:
                self.log.debug("Received %d/%d documents." % (len(documents), limit or 0))
                for doc in documents:
                    if not self._check_event(doc):
                        yield doc

    def put(self, doc, connector_name=None):
        params = {"connector": connector_name} if connector_name else None
        self._remote("PUT", "put", doc, params)

    def listen(self, func, address=None):
        # TODO: WebSocket thingie?
        pass

    #endregion Document operations

# TYPICAL USAGE:

# client.start()  # Optional
# res = client.put("my doc")
# print res
# client.stop()  # Optional

# # Scenario #1 (simple http put with result)
# client = Client("localhost:4000", "hookie", "id myself")
# # Scenario #2 (listen and yield documents, receive signals) (can use with simple put)
# def my_listener(service_name, proc_name, doc):
#     print doc
#     yield "I received doc#%s" % doc.get("_id")
# def my_listener(service_name, signal):
#     print "I received signal '%s' from service '%s'." % (service_name, signal)
# client = Client("localhost:4000", "hookie", "id myself")
# client.listen(my_listener, "optional hard coded address")
