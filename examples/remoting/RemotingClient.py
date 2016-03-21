#!/usr/bin/env python

# NOTE:
# Example usage of the currently (as of writing) experimental RemotingService,
# talking to the DummyRemotingService.

# Import and set up some simple logging
from eslib.service.Client import Client
import logging, time
# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)
logging.getLogger("requests").setLevel(logging.WARNING)
format='%(name)10s %(levelname)8s %(message)s'
logging.basicConfig(format=format, level=logging.INFO)

# One way of creating the client, by asking the service manager for a service named "remoting".
# (We call ourself the "Hooker" client, hooking onto the dummy service. It is just a name..)
client = Client("Hooker", manager="localhost:5000", service="remoting")

# Another way is to address the service directly:
# client = Client("Hooker", address="localhost:5001")

# We can ask it for status... whether it is "DEAD", "idle", "processing", "stopping", etc.
print "STATUS =", client.status()

# We can ask to see detailed stats
print "STATS  =", client.stats()

# We can ask to see what knowledge it has of the metadata from the common service metadata repository
print "META   =", client.meta()

# We can list all available HTTP routes
print "HELP   ="
for item in client.help()["routes"]:
    print "   %-6s %s" % tuple(item.split(" "))

# We can start and stop the service (the processing part, not run and shut down the service process itself):
# print "START=", client.start()
# print "STATUS=", client.status()
# print "STOP=", client.stop()
# print "STATUS=", client.status()
# time.sleep(2)
# print "STATUS=", client.status()

# TODO: We might want to be able to send stop(wait=True, timeout=10)
#print "START=", client.start()  # NOTE: Will get error back if already started...

# This is how we send data to the service for further processing
print "PUT=", client.put("yo", "input")

# This is how we ask for a portion (here batch size = 2) of data queued for output in service.
resultGenerator = list(client.fetch("output", 2))
print "FETCH", list(resultGenerator)
