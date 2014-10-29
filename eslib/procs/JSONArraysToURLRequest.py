__author__ = 'Eivind Eidheim Elseth'

from ..Processor import Processor
import json
class JSONArraysToURLRequest(Processor):
    def __init__(self, **kwargs):
        super(JSONArraysToURLRequest, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", "str")
        self._output = self.create_socket("output", "urlrequest", "A single urlrequest", is_default=True)
        self.config.set_default(
            prepend = "",
            what = "JSONArraysToURLRequest",
            who = "user"
        )


    def _incoming(self, document):
        """

        """
        doc = json.loads(document)
        for (key, value) in doc.iteritems():
            if type(value) is list:
                for val in value:
                    self._handle(val)

    def _handle(self, value):
        if self.config.prepend:
            value = self.config.prepend + value
        output = {}
        output["url"] = value
        output["what"] = self.config.what
        output["who"] = self.config.who

        self._output.send(output)
