# -*- coding: utf-8 -*-

from .Connector import Connector
from .Socket import Socket


class TerminalInfo(object):

    def __init__(self, terminal=None, include_connections=True):
        if terminal:
            self.type = terminal.type # t.__class__.__name__
            owner_name = "orphan"
            if terminal.owner: owner_name = terminal.owner.name or "???"
            if terminal.owner: owner_name = terminal.owner.name or "???"
            self.owner = owner_name
            self.name = terminal.name
            self.protocol = terminal.protocol
            self.description = terminal.description
            connections = terminal.get_connections()
            self.count = len(connections)
            self.connections = []
            if include_connections:
                for c in terminal.get_connections():
                    self.connections.append(TerminalInfo(c, False))

    def DUMP(self, follow_connections=True, verbose=False, indent=0):
        spacing = "  "
        spc = spacing * indent
        type_indicator = "?"
        mimic_str = ""
        if self.type is Socket:
            type_indicator = "+"
            if self.mimic:
                mimic_str = " (mimic=%s)" % self.mimic.name
        elif self.type is Connector:
            type_indicator = "-"

        print "%s%c%s.%s(%s) (conns=%d)%s" % (spc, type_indicator, self.owner, self.name, self.protocol, self.count, mimic_str)
        if verbose and self.description:
            print "\"%s%s%s\"" % (spc, spc, self.description)

        if follow_connections and self.connections:
            subindent = 0
            if verbose:
                print "%sConnections:" % spc
                subindent += 1
            for c in self.connections:
                c.DUMP(False, verbose, subindent+1)

