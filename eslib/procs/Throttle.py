__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
import time


class Throttle(Processor):
    """
    Only pass through documents that satisfy a whitelist of terms or where certain terms do not occur in a combination
    with blacklisted terms.

    Connectors:
        input      (esdoc)   : Incoming document in 'esdoc' dict format.
    Sockets:
        output     (esdoc)   : Documents that passed the blacklist filtering, arrived on 'input' connector.

    Config:
        delay     = 1.0      : Time to delay document throughput, in seconds (float).
        drop      = False    : Drop items we don't have time for instead of buffering up.
    """

    def __init__(self, **kwargs):
        super(Throttle, self).__init__(**kwargs)

        m = self.create_connector(self._incoming, "input", None, "Incoming document.")
        self.output = self.create_socket("output" , None, "Outgoing document.", mimic=m)

        self.config.set_default(
            delay  = 1.0,
            drop   = True
        )

        self._last_write_ts = 0

    def on_open(self):
        self._last_write_ts = 0

    def _incoming(self, doc):
        if self.output.has_output:
            if self.config.drop:
                now_ts = time.time()
                if now_ts - self._last_write_ts > self.config.delay:  # Otherwise just ignore the incoming doc
                    self._last_write_ts = now_ts
                    self.output.send(doc)
                    #print "QUEUE=", self.connectors["input"].queue.qsize()
            else:
                time.sleep(self.config.delay)
                self.output.send(doc)
                #print "QUEUE=", self.connectors["input"].queue.qsize()
