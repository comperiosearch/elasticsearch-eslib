from . import HttpService
from .. import Processor
import Queue


# NOTE: THIS IS YET EXPERIMENTAL (htb, 2016-03-21)


class RemotingService(HttpService):

    def __init__(self, **kwargs):
        super(RemotingService, self).__init__(**kwargs)

        # Add routes to functions
        self.add_route(self._mgmt_fetch, "GET"     , "/fetch", ["?socket", "?limit"])
        self.add_route(self._mgmt_put  , "PUT|POST", "/put"  , ["?connector"])

        self._queues = {}
        self._put_proc = None

        # NOTE: In on_setup, where you create the fetch proc, set config var congestion_limit

    def setup_put(self, proc):
        self.log.info("Registering put Processor '%s'." % proc.name)
        self._put_proc = proc

    def setup_fetch(self, proc, socket_names=None):
        self.log.info("Creating fetch buffers for Processor '%s'." % proc.name)
        if isinstance(socket_names, basestring):
            socket_names = [socket_names]
        for socket_name in proc.sockets:
            if not socket_names or socket_name in socket_names:
                self._register_callback(proc, socket_name)

    def _register_callback(self, proc, socket_name):
        def callback(proc, doc):
            queue = self._queues[socket_name]
            queue.put(doc)
            pass
        self._queues[socket_name] = Queue.Queue()
        proc.add_callback(callback, socket_name)

    def _put(self, doc, connector_name):
        if self._put_proc:
            self._put_proc.put(doc, connector_name)

    def _fetch(self, socket_name=None, limit=0):
        docs = []
        if socket_name and socket_name in self._queues:
            queue = self._queues[socket_name]
        elif len(self._queues) > 0:
            # TODO: Get default socket instead, or error
            queue = self._queues.keys()[0]
        else:
            return ([], -1)  #  TODO: Or rather an error

        ##print "LIMIT=", limit
        while not queue.empty() and (limit == 0 or len(docs) < limit):
            ##print "LEN(DOCS)=%d" % len(docs)
            doc = queue.get_nowait()
            queue.task_done()
            if doc:
                docs.append(doc)
        return (docs, queue.qsize())

    #region Extra service interface methods

    def _mgmt_fetch(self, request_handler, payload, **kwargs):
        socket_name = kwargs.get("socket")
        limit       = kwargs.get("limit") or 0  # 0 = unlimited
        limit = int(limit)
        ##print "=== KWARGS:", kwargs
        ##print "=== LIMIT:", limit
        (docs, qsize) = self._fetch(socket_name, limit)
        return {"documents": docs, "status": self.status, "queued": qsize}

    def _mgmt_put(self, request_handler, payload, **kwargs):
        connector_name = kwargs.get("connector")
        doc = payload
        self._put(doc, connector_name)

    #endregion Extra service interface methods

    def on_stats(self, stats):
        super(RemotingService, self).on_stats(stats)
        stats["queued"] = {k:q.qsize() for k,q in self._queues.iteritems()}
