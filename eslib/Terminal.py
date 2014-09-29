# -*- coding: utf-8 -*-


class TerminalProtocolException(Exception):
    def __init__(self, socket, connector):
        msg = "Socket: %s.%s(%s), Connector: %s.%s.%s(%s)" % (socket.owner.name, socket.name, socket.protocol, connector.owner.name, connector.name, connector.name, connector.protocol)
        super(Exception, self).__init__(self, msg)


class Terminal(object):
    "Common abstract base class for connectors and sockets."

    ANY_PROTOCOL = "any"

    def __init__(self, name, protocol):
        self.type        = None   # type:      Either 'Socket' or 'Connector'
        self.owner       = None   # Processor:
        self.name        = ""     # str:       Name of terminal
        self.protocol    = ""     # str:       Name of object format expected as input/output on this terminal
        self.description = ""     # str:       Text describing purpose and property of this terminal

        self.connections = []

        self.name = name or "unnamed"
        self.protocol = protocol or Terminal.ANY_PROTOCOL

    def attach(self, terminal):
        self.connections.append(terminal)

    def detach(self, terminal):
        if terminal in self.connections:
            self.connections.remove(terminal)

    def get_connections(self, owner=None, terminal_name=None):
        "Return all connections if owner is missing. Ignore terminal_name is owner is missing."
        connections = []
        for c in self.connections[:]:
            if not owner or (c.owner == owner and (not terminal_name or c.name == terminal_name)):
                connections.append(c)
        return connections

    @staticmethod
    def protocol_compliance(socket, connector):
        if connector.protocol == Terminal.ANY_PROTOCOL or socket.protocol == Terminal.ANY_PROTOCOL:
            return True
        ss = socket.protocol.split()
        cc = connector.protocol.split()
        return ss[:len(cc)] == cc[:len(cc)]
