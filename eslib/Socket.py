# -*- coding: utf-8 -*-

from .Terminal import Terminal


class Socket(Terminal):
    "Output terminal in a Processor. Writes data to one or more subscribing connectors of matching protocol."

    def __init__(self, name, protocol=None, mimic=None):
        super(Socket, self).__init__(name, protocol)
        self.type = Socket
        self.callbacks = []  # List of methods for external callbacks
        self.mimic = mimic

    def send(self, document):
        "Send data to all subscribing connectors and callbacks."

        # Send data to all accepting connectors
        subscribers = self.connections[:]
        for subscriber in subscribers:
            if subscriber.accepting:
                subscriber.receive(document)
        # Finally, notify all subscribing callbacks
        for callback in self.callbacks:
            callback(self.owner, document)

    @property
    def has_output(self):
        if self.connections or self.callbacks:
            return True
        return False

    def _find_mimic_proto(self, visited=None):
        if not visited:
            visited = []
        if self.mimic and self.mimic.connections and not self in visited:
            visited.append(self)
            connected_socket = self.mimic.connections[0]
            return connected_socket._find_mimic_proto(visited)
        return self.protocol

    @property
    def mimiced_protocol(self):
        return self._find_mimic_proto()
