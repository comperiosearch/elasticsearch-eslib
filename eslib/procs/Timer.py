__author__ = 'Hans Terje Bakke'

from eslib import Monitor
import time

class Timer(Monitor):
    """
    Send a command on an output socket at configured interval.
    The configured 'actions' is a list of vectors of (initial_offset, interval, document).
    The time units are in seconds ('float'). The 'document' is *whatever* you want on to output,
    typically a string or a dict. type.

    Note that if you have very short intervals, you might want to adjust the run loop delay 'sleep' (not in 'config').
    (It defaults to 0.5 seconds for this processor.)

    Sockets:
        output     (*)       : Output occurring at configured intervals. From the 'document' part of the configured action.

    Config:
        actions   = []       : Time to delay document throughput, in seconds (float).
    """
    def __init__(self, **kwargs):
        super(Timer, self).__init__(**kwargs)
        self._output = self.create_socket("output", None, "Output occurring at configured intervals. From the 'document' part of the configured action.")

        # (Override) Let ticks last half a second here by default... there's generally no rush, unless intervals are very short:
        self.sleep = 0.5

        self.config.set_default(actions=[]) # A list of tuples of (initial_offset, interval, document)

        self._actions = []

    def on_open(self):
        now = time.time()
        self._actions = []
        if self._actions is not None:
            if not hasattr(self._actions, '__iter__'):
                msg = "'config.actions' is not iterable."
                self.log.critical(msg)
                raise ValueError(msg)
            for a in self.config.actions:
                # Validate tuple format
                if not type(a) in [list, tuple] or not len(a) == 3 or not type(a[0]) in [int, float] or not type(a[1] in [int, float]):
                    msg = "An element in 'config.actions' is not of expected format and/or type '(initial_offset, interval, document)'."
                    self.log.error(msg)
                    #raise ValueError(msg)  # Maye not critical enough to raise exception, just skip the wrong one.
                self._actions.append([now + a[0], a[1], a[2]])

    def on_tick(self):
        now = time.time()
        for a in self._actions:
            if now >= a[0]:
                # Next time for this one is...
                a[0] = now + a[1]
                # Then send the action/document
                self._output.send(a[2])
