class Controller(object):
    def __init__(self, *args):
        self._items = args[:]

    def DUMP(self):
        fmt = "%-15s %-15s %-7s %-8s %-9s %-7s %-9s %-4s %-4s %-5s %-5s"
        print fmt % ("Type", "Name", "Running", "Stopping", "Accepting", "Aborted", "Suspended", "Ins", "Outs", "Keep", "Count")
        for item in self._items:
            producers = 0
            subscribers = 0
            for p in item.connectors.itervalues():
                producers += len(p.connections)
            for p in item.sockets.itervalues():
                subscribers += len(p.connections)
            print fmt % (item.__class__.__name__, item.name, item.running, item.stopping, item.accepting, item.aborted, item.suspended, producers, subscribers, item.keepalive, item.count)

# TODO:
# * wait(*args) wait for all or mentioned Processors to finish
# * stop(*args) stop all or mentioned Processors, including keepalives
# * start(*args) start all or mentioned Processors
# * abort(*args) abort all Processors or mentioned Processors
# * suspend/resume(*args) suspend/resume all or mentioned Processors
