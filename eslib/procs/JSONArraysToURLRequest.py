__author__ = 'Eivind Eidheim Elseth'

from ..Processor import Processor
import json

class JSONArraysToURLRequest(Processor):
    """
    Translates the content of a JSON object with arrays into url requests.
    An object is expected in the form:
    {
        "key": ["list", "of", "values"],
        "some other key": ["even", "more", "values"]
    }
    Keys that don't have an array as its value will be ignored.

    You can also prepend each value with a string if you do not get the entire url from the source socket

    Sockets:
        input     (urlrequset)   (default)   : Output from the command line utility's stdout
    Config:
        prepend         = None   : The string to prepend each value with.
    """


    def __init__(self, **kwargs):
        super(JSONArraysToURLRequest, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", "str")
        self._output = self.create_socket("output", "urlrequest", "A single urlrequest", is_default=True)
        self.config.set_default(
            prepend = None,
            what = "JSONArraysToURLRequest",
            who = "user"
        )


    def _incoming(self, document):
        """
        Parses the JSON string and sends the url request object to the default socket
        :param document:
        """
        doc = json.loads(document)

        for output in self._handle_all(doc):
            self._output.send(output)

    def _handle_all(self, jsonobject):
        """
        Yields request objects for each of the arrays in the JSON object
        :param jsonobject: The JSON object with arrays
        :return: a generator which generates request objects
        """
        for (key, value) in jsonobject.iteritems():
            if type(value) is list:
                for val in value:
                    yield self._handle(val)


    def _handle(self, value):
        """
        Creates a url request object with the given value, and with the what and who defined for the instance
        :param value: the value which is added to the url portion of the request object
        :return: the request object
        """
        if self.config.prepend:
            value = self.config.prepend + value
        output = {}
        output["url"] = value
        output["what"] = self.config.what
        output["who"] = self.config.who
        return output

