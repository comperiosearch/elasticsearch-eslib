#!/usr/bin/env python

#region Logging

import eslib
import logging
LOG_FORMAT = ('%(name) -8s %(levelname) -10s %(funcName) -30s %(lineno) 5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

console = logging.StreamHandler()
console.setLevel(logging.TRACE)
console.setFormatter(logging.Formatter("%(firstname) -8s / %(lastname) -8s %(funcName) -15s %(lineno) 5d %(levelname) -10s %(message)s"))

proclog = logging.getLogger("proclog")
proclog.setLevel(logging.TRACE)
proclog.addHandler(console)

doclog  = logging.getLogger("doclog")
doclog.setLevel(logging.TRACE)
doclog.addHandler(console)

log = logging.getLogger("servicedemo")
log.setLevel(logging.TRACE)
log.addHandler(console)

#endregion Logging

#region Test processors

from eslib.procs import FileWriter, HttpMonitor, Timer
from eslib import Processor, Monitor

class MyMonitor(Monitor):
    def __init__(self, **kwargs):
        super(MyMonitor, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input")
        self.create_socket("output")

    def _incoming(self, doc):
        self.sockets["output"].send([
            self.name,
            self.config.common,
            self.config.configurable
        ])

class MyMiddle(Processor):
    def __init__(self, **kwargs):
        super(MyMiddle, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input")
        self.create_socket("output")

    def _incoming(self, doc):
        self.sockets["output"].send([
            "/".join([doc[0], self.name]),
            "/".join([doc[1], self.config.common]),
            "/".join([doc[2], self.config.configurable])
        ])

class MyEnd(Processor):
    def __init__(self, **kwargs):
        super(MyEnd, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input")
        self.create_socket("output")

    def _incoming(self, doc):
        self.sockets["output"].send([
            "/".join([doc[0], self.name]),
            "/".join([doc[1], self.config.common]),
            "/".join([doc[2], self.config.configurable])
        ])

#region Test processors

#region Set up pipeline

suspended = False

def mgmt_hook(verb, path, data):
    global suspended
    global ping, mon1, mon2, mid, end

    log.info("Call to mgmt_hook: %s %s: %s" % (verb, path, data))

    procs = [ping, mon1, mon2, mid, end]

    if verb == "GET":

        if path == "status":
            res_procs = {}
            res = {"processors": res_procs}
            for proc in procs:
                res_procs[proc.name] = {
                    "running"  : proc.running,
                    "suspended": proc.suspended,
                    "processed": proc.__dict__.get("count"),
                    "pending"  : (proc.connectors["input"].pending if "input" in proc.connectors else 0)
                }
            return res

        if path == "suspend":
            ping.suspend()
            suspended = True
            return {"suspended": suspended}

        if path == "resume":
            ping.resume()
            suspended = False
            return {"suspended": suspended}

        if path.startswith("config/"):
            cmd, procname, value = path.split("/")
            found = None
            for p in procs:
                if p.name == procname:
                    found = p
                    break
            if found:
                p.config.configurable = value
                p.restart()
            return {"message": "%s.config.configurable changed to '%s' and reloaded." % (p.name, value)}

    return {"error": "Unrecognized command '%s %s'." % (verb, path)}


ping = Timer      (name="ping", actions=[(3, 3, "ping")])

mon1 = MyMonitor  (name="mon1", common="common-mon", configurable="initial-mon1")
mon2 = MyMonitor  (name="mon2", common="common-mon", configurable="initial-mon2")
mid  = MyMiddle   (name="mid" , common="common-mid", configurable="initial-mid" )
end  = MyEnd      (name="end" , common="common-end", configurable="initial-end" )

comm = HttpMonitor(name="comm", hook=mgmt_hook, host="localhost", port=4444)

ping.attach(mon1).attach(mon2)
#mon1.subscribe(ping)
#mon2.subscribe(ping)
mid.subscribe(mon1).subscribe(mon2)
end.subscribe(mid)

w = FileWriter()
w.subscribe(end)

#endregion Set up pipeline

#region Run

print "**comm starting"
comm.start()

print "**run starting"
ping.start()

try:
    ping.wait()
except KeyboardInterrupt:
    print "**run stopping"
    ping.stop()
    mon1.stop()
    mon2.stop()
    end.wait()

print "**stopping comm"
comm.stop()
comm.wait()
print "**comm stopped"

print "**run finished"

#endregion Run
