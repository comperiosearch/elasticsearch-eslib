__author__ = 'mats'
from .. import Processor


class ParseEdgeToIds(Processor):

    def __init__(self, **kwargs):
        super(ParseEdgeToIds, self).__init__(**kwargs)
        self.create_connector(self._incoming, "ids", "graph-edge")
        self.create_socket("ids", "str", "All the unique ids")
        self.create_socket("edges", "graph-edge", "All the edges")

    def _incoming(self, document):
        if document["type"] == "author":
            self.sockets["ids"].send(document["from"])
        else:
            self.sockets["edges"].send(document)
            self.sockets["ids"].send(document["from"])
            self.sockets["ids"].send(document["to"])
