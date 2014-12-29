__author__ = 'mats'
from ..Generator import Generator
from .twitter import Twitter

class TwitterFollowerGetter(Generator):
    """
    This generator takes as input the ids of twitter users, and then goes
    ahead and retrieves the followers or friends of this user,
    and outputs the ids.

    # TODO: Document argument 'twitter' and how to configure this. 'outgoing'

    Connectors:
        ids        (str)         : Incoming IDs to get data for.
    Sockets:
        ids        (str)         : IDs of related nodes.

    Config:
        outgoing   = True        : # TODO: Document this
    """
    def __init__(self, twitter=None, **kwargs):
        super(TwitterFollowerGetter, self).__init__(**kwargs)
        self.twitter = twitter
        self.create_connector(self._incoming, "ids", "str")
        self._output_id = self.create_socket("ids", "str", "IDs of related nodes.")
        self._output_edge = self.create_socket("edge", "graph-edge")
        self.config.set_default(outgoing=True, reltype="follows")


    def on_open(self):
        if self.twitter is None:
            self.twitter = Twitter(
                consumer_key=self.config.consumer_key,
                consumer_secret=self.config.consumer_secret,
                access_token=self.config.access_token,
                access_token_secret=self.config.access_token_secret
            )

    def _incoming(self, document):
        try:
            id_ = int(document)
        except ValueError:
            self.doclog.exception("Could not parse id: %s to int" % str(document))
        else:
            related = self.twitter.get_follows(uid=str(id_), outgoing=self.config.outgoing)
            self._send(id_, related)

    def _send(self, origin, related):
        for id_ in related:
            edge = {"from": None, "type": self.config.reltype, "to": None}
            self._output_id.send(id_)
            if self.config.outgoing:
                edge["from"] = origin
                edge["to"] = id_
            else:
                edge["from"] = id_
                edge["to"] = origin

            if all(edge.itervalues()):
                self.doclog.trace("Sending edge %s to Neo4j" % str(edge))
                self._output_edge.send(edge)
            else:
                self.doclog.error("Edge had None-fields: %s" % str(edge))