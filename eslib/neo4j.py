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

        self.config.set_default(host="localhost", port="7474")
        self._set_config()
        self.validate()

    def _set_config(self):
        """ Set some config variables. """

        self.config.data_path = "http://{0}:{1}/db/data".format(
            self.config.host,
            self.config.port)

        self.config.node_path = "/".join([self.config.data_path, "node"])
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

        r = requests.get(self.config.data_path,
                         headers=self.config.headers["get"])
        version = r.json()["neo4j_version"]
        print "Connected to Neo4j-server OK, version= {0}".format(version)

    @staticmethod
    def _get_edge_query(from_id, rel_type, to_id):
        """
        Returns a merge cypher-query.

        Args:
            to_id: the property 'id' matching the twitter user id.
            from_id: the property 'id' matching the twitter user id.
            rel_type: 'follows' or 'mentions'

        Returns:
            str: A cypher query that creates the edge and possibly a node.

        """
        # TODO: what to do with labels here.

        return (("MERGE (a:user {id: %s}) "
                 "MERGE (b:user {id: %s}) "
                 "MERGE a-[:%s]->b "
                 "RETURN *") % (from_id, to_id, rel_type))
    @staticmethod
    def _get_node_merge_query(user):
        labels = "user"
        if user["level"] == 0:
            labels += ":seed"

        statement = (("MERGE (n {id: {node_id}}) "
                     "ON MATCH SET n={props}, n :%s "
                     "ON CREATE SET n={props}, n :%s "
                     "RETURN (n)") % (labels, labels))

        props = {"node_id": user["id"], "props": user}

        return statement, props


    def _get_node_query_if_properties(self, uid):
        """
        Return the query that fetches a node with the given id.
        Note: the id is the property id, not the neo4j id.

        Args:
            uid: the user id of a node, either a int-parsable string or an int
        Returns:
            str: A Cypher query string that will return a node given it is
                 present.

        """

        return (("MATCH (n:user) "
                 "WHERE (n.id = %s AND HAS(n.name)) "
                 "RETURN n") % str(uid))

    def commit(self, create_rq):
        """
        Write a rq to neo4j.

        Raises:
            Error from requests if something went wrong

        """

        path = "{0}/transaction/commit".format(self.config.data_path)
        resp = requests.post(path,
                             data=json.dumps(create_rq),
                             headers=self.config.headers["put"])

        resp.raise_for_status()
        return resp



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

        # PERHAPS NOT COMMIT EVERY EDGE?
        path = "{0}/transaction/commit".format(self.config.data_path)
        resp = requests.post(path,
                             data=json.dumps(create_rq),
                             headers=self.config.headers["put"])

        resp.raise_for_status()
        return resp

    @staticmethod
    def _build_rq(queries, properties=None, result_data_contents="graph"):
        """
        Return a dictionary of statements to be supplied to neo4j.

        Args:
            queries: either int indicating the number of queries to be put
                     in the dict at a later time, or iterable of actual queries.
            properties:
            result_data_contents: the preferred return format from neo4j

        Raises:
            Will cause hell if queries is not int or iterable.

        Returns:
            a dict containing the json-compatible dict of transactions.

        """

        # Idially one should not use type() in if.
        # TODO: Preallocate memory
        if type(queries) == int:
            queries = [None] * queries
        rq_dict = {"statements" : []}
        if len(queries) == 0:
            return rq_dict

        for i, q in enumerate(queries):
            rq_dict["statements"].append({"statement": q})
            rq_dict["statements"][i]["resultDataContents"] = [result_data_contents]
            if (properties is not None) and (i < len(properties)):
                rq_dict["statements"][i]["parameters"] = properties[i]

        return rq_dict

    @staticmethod
    def add_statement(rq_dict, statement, result_data_contents="graph"):
        """
        Modifies rq_dict in place and adds another statement

        Args:
            rq_dict: a dict containing valid neo4j transactions
            statement: a new neo4j transaction to add to the commit
            result_data_contents: the preferred return format from neo4j

        Raises:
            KeyError: if "statements" not in request_dict

        """
        rq_dict["statements"].append({"statement": statement})
        rq_dict["statements"][-1]["resultDataContents"] = [result_data_contents]


    def get_users(self, user_ids):
        """
        Get users from neo4j that have the ids specified in user_ids

        Args:
            user_ids: iterable of ints or int-parseable strings

        Raises:
            Error: from requests if something is wrong with the request
            ValueError: if the resp can't be json-decoded

        Returns:
            A dict of containing the results of the response

        """

        path = "{0}/transaction/commit".format(self.config.data_path)
        queries = (self._get_node_query_no_properties(uid) for uid in user_ids)
        rq = self._build_rq(queries, "graph")
        resp = requests.get(path, data=json.dumps(rq),
                            headers=self.config.headers["get"])
        resp.raise_for_status()
        return resp.json()


    # def parse_user(self, item):
    #     """
    #     This method tries to put a user into neo4j.
    #
    #     Args:
    #         item: A dict representing a user.
    #
    #     """
    #     node_id = item["id"]
    #     labels = "user"
    #     if item["level"] == 0:
    #         labels += ":seed"
    #     path = "/".join([self.path, "transaction", "commit"])
    #     rq = self.get_cypher_dict()
    #
    #     statement = "MERGE (n {id: {node_id}}) ON MATCH SET n={props}, n :%s ON CREATE SET n={props}, n :%s RETURN (n)" %(labels, labels)
    #     rq["statements"][0]["parameters"] = {"node_id": node_id,
    #                                          "labels": labels,
    #                                          "props": item
    #                                          }
    #     rq["statements"][0]["statement"] = statement
    #     resp = requests.post(path,
    #                          data=json.dumps(rq),
    #                          headers=self.headers["put"])