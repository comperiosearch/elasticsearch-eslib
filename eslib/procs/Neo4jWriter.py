from ..Processor import Processor
from ..neo4j import Neo4j
from ..twitter import Twitter

class Neo4jWriter(Processor):
    """
    This is a pipeline step whos primary function is to push an edge
    between the author of a tweet to all the people mentioned in the tweet.
    
    A secondary thing this Processor does, is make sure that the nodes involved
    in the relationship (edge), has fully populated properties within neo4j.
    The way this happens is that if the node in neo4j does not have properties,
    the Process will ask Twitter for the properties for thate node.

    """
    def __init__(self, **kwargs):
        super(Neo4jWriter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", "esdoc.twitter")
        

    def on_open(self):
        """
        Instantiates both a neo4j-instance and a twitter-instance.

        Raises:
            - ConnectionError if neo4j can't contact its server
            - Exception if twitter can't authenticate properly

        """

        self.neo4j = Neo4j(host=self.config.host, port=self.config.port)
        # self.twitter = Twitter(
        #     consumer_key=self.config.consumer_key,
        #     consumer_secret=self.config.consumer_secret,
        #     access_token=self.config.access_token,
        #     access_token_secret=self.config.access_token_secret
        # )

    def _incoming(self, document):
        """
        Process an incomming document, create edges in neo4j. Get properties
        of nodes if missing.

        Args: 
            document: a doc of protocol as described in self.create_connector.


        The ambition is that this Processor should never go down no matter
        what happens to a document in this method.

        """

        try:
            (edges, uniques) = self.parse(document)
        except KeyError as ke:
            print("Document was not parsed")
            return
        
        for edge in edges:
            self.neo4j.write_edge(*edge)

        for unique in uniques:
            # Make sure the all of the ids properties are in neo4j
            pass

    def parse(self, document):
        """
        Assumes that document is a valid esdoc, with the full tweet
        source code found in _source. 

        Raises:
            - KeyError if entities are not included in the tweet

        Ideas:
            - We could extract hashtags and
            - "in_response_to" extract, and perhaps use
            - retweeted_status for some cool retweet stuff.

        """
        edges = []
        from_id = document["_source"]["user"]["id_str"]
        uniques = set([from_id])
        for obj in document["_source"]["entities"]["user_mentions"]:
            to_id = obj["id_str"]
            uniques.add(to_id)
            edges.append((from_id, "mentioned", to_id))

        return edges, uniques

