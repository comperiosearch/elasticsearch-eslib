__author__ = 'mats'

from ..Generator import Generator
from .neo4j import Neo4j

from itertools import izip
import time, logging

class Neo4jReader(Generator):
    """
    The purpose of this processor is to ask Neo4j if a node with a given
    user id has it's full set of properties.

    It takes an id and determines whether or not it has its properties set.
    If it lacks properties, it will be outputted by the 'ids' socket.

    Connectors:
        id         (str)      : Incoming IDs to check.
    Sockets:
        ids        (str)      : Outputs IDs that lack properties.

    Config:
        batchsize  = 20       : How many IDs to gather up before making a call to Neo4j.
        batchtime  = 5.0      : How many seconds to wait before we send a batch if it is not full.
        host       = localhost: The host we should connect to
        port       = 7474     : The default neo4j port

    """

    def __init__(self, **kwargs):
        super(Neo4jReader, self).__init__(**kwargs)
        self.create_connector(self._incoming_id, "id", "str", "Incoming IDs to check.")
        self._missing = self.create_socket("missing", "str", "Outputs IDs that lack properties.")
        #self._missing = self.create_socket("output", "???", "Outputs data retrived, one document per ID.")

        self.config.set_default(
            batchsize = 20,
            batchtime = 5.0,
            host      = "localhost",
            port      = 7474
        )

        self._neo4j = None

        self._queue = []
        self._last_get = time.time()
        self._has_properties = set([])

    #TODO: Could place this in Neo4jBase
    def on_open(self):
        """
        Instantiates both a neo4j-instance and a twitter-instance.

        Raises:
            - ConnectionError if neo4j can't contact its server
            - Exception if twitter can't authenticate properly
        """

        # TODO: Need logging, request timeout and exception handling down there:
        self.log.debug("Connecting to Neo4j.")
        self._neo4j = Neo4j(host=self.config.host, port=self.config.port)
        self.log.status("Connected to Neo4j on %s:%d." % (self.config.host, self.config.port))

    def _incoming_id(self, id_):
        """
        Takes an incoming id, gets the correct query string from self.neo4j,
        before appending the query to self._queue
        """
        if id_ not in self._has_properties:
            query = self._neo4j.get_node_query_if_properties(id_)
            self._queue.append((id_, query))

    def on_tick(self):
        """
        Commit items in queue if queue exceeds batchsize or it's been long
        since last commit.
        """
        if ((len(self._queue) >= self.config.batchsize) or
            (time.time() - self._last_get > self.config.batchtime and self._queue)):
            self._get()

    def on_shutdown(self):
        """ Get rid of rest of queue before shutting down. """
        while self._queue:
            self._get()

    def _get(self):
        num_elem = len(self._queue)
        if num_elem > self.config.batchsize:
            num_elem = self.config.batchsize

        ids, queries = [list(t)
                        for t in
                        izip(*self._queue[:num_elem])]
        rq = self._neo4j._build_rq(queries)
        resp = self._neo4j.commit(rq)
        self.log.debug("Asking neo4j for %i users." % num_elem)
        self._queue = self._queue[num_elem:]
        self._last_get = time.time()
        self._write_uids(ids, resp)

    def _write_uids(self, ids, resp):
        """
        Outputs the ids of the nodes in the resp-object to a socket.

        Args:
            ids: The ids that corresponds to a query
            resp: a requests-module response object with neo4j-nodes in 'graph'-
                  format.
        """
        for uid, result in izip(ids, resp.json()["results"]):
            if not result["data"]:
                self._missing.send(uid)
                if self.doclog.isEnabledFor(logging.TRACE):
                    self.doclog.trace("uid %s does not have properties" % uid)
            else:
                self._has_properties.add(uid)
