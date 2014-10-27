__author__ = 'mats'

from ..Generator import Generator
from ..neo4j import Neo4j

from itertools import izip
import time

class Neo4jWriter(Generator):
    """
    This is a pipeline step which primary function is to push an edge
    between the author of a tweet to all the people mentioned in the tweet.
    
    Connectors:
        edge       (graph-edge)   : Edge object to write.
        user       (graph-user)   : User object to write.

    Config:
        batchsize  = 20           : How many IDs to gather up before making a call to Neo4j.
        batchtime  = 5.0          : How many seconds to wait before we send a batch if it is not full.
        host       = localhost: The host we should connect to
        port       = 7474     : The default neo4j port

    """

    def __init__(self, **kwargs):
        super(Neo4jWriter, self).__init__(**kwargs)
        self.create_connector(self._incoming_edge, "edge", "graph-edge")
        self.create_connector(self._incoming_user, "user", "graph-user")
        # This could be better
        self.edge_queue = []
        self.last_edge_commit = time.time()
        self.user_queue = []
        self.last_user_commit = time.time()
        self.config.set_default(
            batchsize=20,
            batchtime=5,
            host="localhost",
            port="7474"
        )

    def on_open(self):
        """
        Instantiates both a neo4j-instance and a twitter-instance.

        Raises:
            - ConnectionError if neo4j can't contact its server
            - Exception if twitter can't authenticate properly

        """

        self.neo4j = Neo4j(host=self.config.host, port=self.config.port)

    def _incoming_edge(self, document):
        """
        Takes an edge and puts it's correct query in the queue.

        Args: 
            document: A dict with "from", "to" and "type" as fields.

        The ambition is that this Processor should never go down no matter
        what happens to a document in this method.

        """
        try:
            from_id = document["from"]
            to_id = document["to"]
            edge_type = document["type"]
        except KeyError as ke:
            print("Document was not parsed")
            return

        query = self.neo4j.get_edge_query(from_id, edge_type, to_id)
        self.edge_queue.append(query)

    def _incoming_user(self, document):
        query, params = self.neo4j.get_node_merge_query(document)
        self.user_queue.append((query, params))

    def on_tick(self):
        """
        Commit items in queue if queue exceeds batchsize or it's been long
        since last commit.

        """
        now = time.time()
        if((len(self.edge_queue) >= self.config.batchsize) or
            (now - self.last_edge_commit >= self.config.batchtime and
                 self.edge_queue)):
            self.edge_send()

        if((len(self.user_queue) >= self.config.batchsize) or
            (now - self.last_user_commit >= self.config.batchtime and
                 self.user_queue)):
            self.user_send()

    def on_shutdown(self):
        """ Clear out the rest of the items in the queue """
        while self.edge_queue:
            self.edge_send()
        while self.user_queue:
            self.user_send()

    def edge_send(self):
        num_edges = len(self.edge_queue)
        rq = self.neo4j._build_rq(self.edge_queue[:num_edges])
        self.neo4j.commit(rq)
        self.edge_queue = self.edge_queue[num_edges:]
        self.last_edge_commit = time.time()

    def user_send(self):
        num_users = len(self.user_queue)
        users, params = [list(t)
                         for t in
                         izip(*self.user_queue[:num_users])]

        rq = self.neo4j._build_rq(users, params)
        self.neo4j.commit(rq)
        self.user_queue = self.user_queue[num_users:]
        self.last_user_commit = time.time()

    # def parse(self, document):
    #     """
    #     Assumes that document is a valid esdoc, with the full tweet
    #     source code found in _source.
    #
    #     Raises:
    #         - KeyError if entities are not included in the tweet
    #
    #     Ideas:
    #         - We could extract hashtags and
    #         - "in_response_to" extract, and perhaps use
    #         - retweeted_status for some cool retweet stuff.
    #
    #     """
    #     edges = []
    #     from_id = document["_source"]["user"]["id_str"]
    #     uniques = set([from_id])
    #     for obj in document["_source"]["entities"]["user_mentions"]:
    #         to_id = obj["id_str"]
    #         uniques.add(to_id)
    #         edges.append((from_id, "mentioned", to_id))
    #
    #     return edges, uniques


