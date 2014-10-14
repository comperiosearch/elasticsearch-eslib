__author__ = 'mats'

from ..Generator import Generator
from ..neo4j import Neo4j

from itertools import izip
import time

class Neo4jReader(Generator):
    """
    The purpose of this processor is to ask Neo4j if a node with a given
    user id has it's full set of properties.

    It takes an id and determines whether or not it has its properties set.
    If it lacks properties, it will be outputted by the 'ids' socket.

    """

    def __init__(self, **kwargs):
        super(Neo4jReader, self).__init__(**kwargs)
        self.create_connector(self._incoming_id, "id", "str")
        self.create_socket("ids", "str", "Outputs ids that lack properties")
        self.config.set_default(
            batchsize=20,
            batchtime=5
        )
        self._queue = []
        self.last_get = time.time()
        self.has_properties = set([])

    #TODO: Could place this in Neo4jBase
    def on_open(self):
        """
        Instantiates both a neo4j-instance and a twitter-instance.

        Raises:
            - ConnectionError if neo4j can't contact its server
            - Exception if twitter can't authenticate properly

        """
        self.neo4j = Neo4j(host=self.config.host, port=self.config.port)

    def _incoming_id(self, id_):
        """
        Takes an incoming id, gets the correct query string from self.neo4j,
        before appending the query to self._queue

        """
        if id_ not in self.has_properties:
            query = self.neo4j.get_node_query_if_properties(id_)
            self._queue.append((id_, query))

    def on_tick(self):
        """
        Commit items in queue if queue exceeds batchsize or it's been long
        since last commit.

        """
        now = time.time()

        if ((len(self._queue) >= self.config.batchsize) or
            (now - self.last_get > self.config.batchtime and self._queue)):
            self.get()

    def on_shutdown(self):
        """ Get rid of rest of queue before shutting down. """
        while self._queue:
            self.get()

    def get(self):
        ids, queries = [list(t)
                        for t in
                        izip(*self._queue[:self.config.batchsize])]

        rq = self.neo4j._build_rq(queries)
        resp = self.neo4j.commit(rq)
        self._queue = self._queue[self.config.batchsize:]
        self.last_get = time.time()
        self.write_uids(ids, resp)

    def write_uids(self, ids, resp):
        """
        Outputs the ids of the nodes in the resp-object to a socket.

        Args:
            ids: The ids that corresponds to a query
            resp: a requests-module response object with neo4j-nodes in 'graph'-
                  format.

        """

        for uid, result in izip(ids, resp.json()["results"]):
            if not result["data"]:
                self.sockets["ids"].send(uid)
            else:
                self.has_properties.add(uid)