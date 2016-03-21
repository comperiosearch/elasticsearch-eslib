# NOTE:
#
# REMOTING SERVICE IS YET EXPERIMENTAL (as of when this was written)
#
# This is an example of how to create a service based on the RemotingService.
# See also RemotingClient.py for example of how to call it remotely.
#
# SETUP:
#
#  Copy the file to your service "source" directory, and add to the package __init__.py file:
#
#   from .DummyRemotingService       import DummyRemotingService
#   __all__ = (
#       "DummyRemotingService"
#   )
#
# In the service "config" directory, configure it like
#
#   remoting:
#     type                : "DummyRemotingService"
#     frequency           : 3
#     lifespan            : 120

from eslib.service import RemotingService, PipelineService
from eslib.procs import Timer
from eslib import Processor


# COMMENT to the below connectors and sockets:
# The "command" socket and connector are set to default, so that we can easily create a
# service based on the pipeline service. Then all pipleline processors are linked so that
# start/stop events etc are easily propagated the way we want. The downside to this
# approach is that the socket and connector we want to use from the client will have to
# be names, as they are not the default ones.
# (Here, by client example: client.fetch("output"), and client.put("input").)


class FetchProc(Processor):
    def __init__(self, **kwargs):
        super(FetchProc, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input")
        self.command = self.create_socket("command", is_default=True)  # To link easily as pipeline
        self.output = self.create_socket("output")
        self.num = 0

    def on_open(self):
        self.num = 0

    def _incoming(self, doc):
        # For each incoming tick, generate one output doc:
        self.num += 1
        print "SEDNING TO QUEUE:", self.num
        self.output.send(self.num)

class PutProc(Processor):
    def __init__(self, **kwargs):
        super(PutProc, self).__init__(**kwargs)
        self.create_connector(self._command, "command", is_default=True)  # To link easily as pipeline
        self.create_connector(self._incoming, "input")

    def _command(self, doc):
        pass  # Down the drain; this is simply for linking

    def _incoming(self, doc):
        print("INCOMING DOC:", doc)


class DummyRemotingService(RemotingService, PipelineService):

    def __init__(self, **kwargs):
        super(DummyRemotingService, self).__init__(**kwargs)

        self.config.set_default(
            timer_frequency       = 3,
            lifespan              = 0
        )

    def on_configure(self, credentials, config, global_config):
        self.config.set(
            manager_endpoint      = global_config.get("manager_host"),
            management_endpoint   = config.get("management_endpoint"),

            timer_frequency       = config["frequency"],
            lifespan              = config["lifespan"]
        )

    def on_setup(self):
        timer = Timer(
            service = self,
            name    = "timer",
            actions = [(self.config.timer_frequency, self.config.timer_frequency, "ping")]
        )
        fetchProc = FetchProc(
            service = self,
            name    = "fetchProc",
        )
        putProc = PutProc(
            service = self,
            name    = "putProc"
        )

        procs = [timer, fetchProc, putProc]
        self.link(*procs)

        self.setup_put(putProc)
        self.setup_fetch(fetchProc, "output")

        return True
