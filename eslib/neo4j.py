from __future__ import division
from .Configurable import Configurable
import json
import requests

class Neo4j(Configurable):
    """
    This class communicates with a neo4j database.

    """

    def __init__(self, **kwargs):
        super(Neo4j, self).__init__(**kwargs)

        self.config.set_default(
            host = "localhost",
            port = "7474"
            )
        self._set_config()
        self.validate()

    def _set_config(self):
        """ Set some config variables. """

        self.config.datapath = "http://{0}:{1}/db/data".format(
                self.config.host,
                self.config.port
        )

        self.config.nodepath = "/".join([self.config.datapath, "node"])
        self.config.headers = dict([])
        self.config.headers["get"] = {"Accept": "application/json"}
        self.config.headers["put"] = {"Content-Type": "application/json"}

    def validate(self):
        """ 
        Get the neo4j version number.

        Raises:
            - ConnectionError if requests fails to connect to the 
              host + port combination.

        """

        r = requests.get(self.config.datapath, 
                         headers=self.config.headers["get"])
        version = r.json()["neo4j_version"]
        print "Connected to Neo4j-server OK, version= {0}".format(version)

    def _get_edge_query(self, from_id, rel_type, to_id):
        """
        Returns a merge cypher-query.

        Args:
            to_id: the property 'id' matching the twitter user id.
            from_id: the property 'id' matching the twitter user id.
            rel_type: 'follows' or 'mentions'

        Returns:
            str: A cypher query that creates the edge and possibly a node.

        """

        return (("MERGE (a {id: %s}) "
                "MERGE (b {id: %s}) "
                "MERGE a-[:%s]->b "
                "RETURN *") % (from_id, to_id, rel_type))
    
    def write_edge(self, from_id, rel_type, to_id):
        """
        Puts an edge into neo4j.

        Args:
            from_id: The source node of the edge
            rel_type: The relationship type. A mentioned is denoted "mentioned"
            to_id: The mentioned node.

        Raises:
            Will raise error from requests if something went wrong

        """

        create_str = self._get_edge_query(from_id, rel_type, to_id)
        create_rq = self._build_rq([create_str], "graph")
        print create_rq

        # PERHAPS NOT COMMIT EVERY EDGE?
        path = "{0}/transaction/commit".format(self.config.datapath)
        resp = requests.post(path,
                             data=json.dumps(create_rq),
                             headers=self.config.headers["put"])

        resp.raise_for_status()

    def _build_rq(self, queries, result_data_contents="REST"):
        """
        Return a dictionary of statements to be supplied to neo4j.

        Args:
            queries: either int or list of actual queries. 
            result_data_contents: 

        """

        if type(queries) == int:
            queries = [None] * queries
        rq_dict = {"statements" : []}
        for i, q in enumerate(queries):
            rq_dict["statements"].append({"statement": q})
            rq_dict["statements"][i]["resultDataContents"] = [result_data_contents]
        return rq_dict