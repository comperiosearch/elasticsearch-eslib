__author__ = 'Hans Terje Bakke'

from subprocess import Popen, PIPE
from Queue import Queue
from select import select
from ..esdoc import tojson

from ..Monitor import Monitor

class ProcessWrapper(Monitor):

    def __init__(self, **kwargs):
        super(ProcessWrapper, self).__init__(**kwargs)
        self.create_connector(self._incoming, None, "Documents that will be sent as input to the wrapped process' stdin, JSON serialized.")
        self._output = self.create_socket("output", None, "Output from the wrapped process. Attempted JSON deserialized unless 'raw_ouput' is set.")

        self.config.set_default(
            command   = None,
            raw_ouput = True
        )

        self._process = None
        self._outgoing = None

    def on_open(self):
        self.log.debug("Creating subprocess from command: %s" % (self.config.command))
        self._process = Popen(
            self.config.command,  # TODO: Wrapped in list or like this?
            stdin=PIPE, stdout=PIPE, stderr=PIPE)
        self.log.status("Subprocess started. PID = %d" % self._process.pid)

        self._outgoing = Queue()

    def on_close(self):
        if self._process:
            # The process should now have terminated, but in case it has not we will send it a hard kill:
            self.log.info("Killing subprocess. (SIGKILL)")
            self._process.kill()  # TODO: NOT SURE THIS IS A GOOD IDEA... I THINK IT WILL ALSO KILL THE PARENT PROCESS (THIS)
            self._process = None
        self._outgoing = None

    def on_abort(self):
        pass  # on_close() will take care of it

    def on_shutdown(self):
        if not self._process:
            return

        self.log.info("Sending remaining items to subprocess.")
        while self._process and not self._outgoing.empty():
            self._handle_io()

        # Notify process that it's time to hang up
        if self._process:
            self.log.info("Terminating subprocess. (SIGTERM)")
            self._process.terminate()

        if self._process:
            self.log.info("Waiting for subprocess to finish.")
            while self._process:
                self._handle_io()

    def on_tick(self):
        # Process as much as we see pending
        while self._process and not self.end_tick_reason and not self.suspended:
            self._handle_io()


    def _handle_io(self):
        if not self._process:
            return

        r, w, e = select([self._process.stdout.fileno(), self._process.stderr.fileno()], [self._process.stdin.fileno()], [], 0)
        if not r and not w and not e:
            return  # Nothing to do right now; let's get some air..

        if self._process.stderr.fileno() in r:
            msg = self._process.stderr.readline()  # Bugger... what if there's no newline? Will it block?
            self.log.warning("Subprocess stderr: %s" % msg)

        if self._process.stdout.fileno() in r:
            incoming = self._process.stdout.readline()  # Bugger... what if there's no newline? Will it block?
            if incoming:
                self._send(incoming)
            else:
                # This is the end... we must have lost contact with the process. Perhaps it finished...
                self.log.debug("EoF; stopping.")
                self._process = None
                self.stop()
                return

        if self._process.stdin.fileno() in w:
            # Send an item from the queue
            if not self._outgoing.empty():
                doc = self._outgoing.get()
                self._outgoing.task_done()
                try:
                    print >> self._process.stdout, doc
                except Exception as e:
                    self.log.exception("Error writing to subprocess. PROBABLY BROKEN PIPE -- I AM CURIOUS WHICH EXCEPTION! (htb)")
                    self._process = None
                    self.stop()
                    return


    def _incoming(self, document):
        # TODO: Convert document to JSON string if necessary
        s = tojson(document)
        self._outgoing.put(s)

    def _send(self, raw):
        # TODO: Convert from JSON if necessary
        doc = raw
        self._output.send(doc)
