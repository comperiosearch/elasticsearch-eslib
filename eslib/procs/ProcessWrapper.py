__author__ = 'Hans Terje Bakke'

from subprocess import Popen, PIPE
from Queue import Queue
from select import select
import codecs, json
from ..esdoc import tojson
import time

from ..Generator import Generator

class ProcessWrapper(Generator):
    """
    Wrap a subprocess.

    Connectors:
        input      (*)         : Incoming documents to be passed on to the subprocess.
                                 Directly if str/unicode, otherwise as a JSON string.
    Sockets:
        output     (*)         : Output from subprocess. Either raw or deserialized from JSON if 'deserialize' is True.

    Config:
        command       = None   : The command to run. Either a direct command in 'str' format or a list of
                                 command and arguments as passed on the command line.
        deserialize   = False  : Whether to deserialize the output from the subprocess as JSON.
    """

    def __init__(self, **kwargs):
        super(ProcessWrapper, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", None, "Incoming documents to be passed on to the subprocess. Directly if str/unicode, otherwise as a JSON string.")
        self._output = self.create_socket("output", None, "Output from subprocess. Either raw or deserialized from JSON if 'deserialize' is True.")

        self.config.set_default(
            command          = None,
            deserialize      = False
        )

        self._process = None
        self._outgoing = None

    def on_open(self):
        command = self.config.command

        if not command:
            raise ValueError("There is no command!")
        elif type(command) in [str, unicode]:
            command = [command]
        elif not type(command) in [list,tuple]:
            raise ValueError("Command must be a str, list or tuple.")

        self.log.debug("Creating subprocess from command: %s" % (self.config.command))
        self._process = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        self.log.status("Subprocess started. PID=%d" % self._process.pid)

        self._outgoing = Queue()

    def on_close(self):
        if self._process:
            # The process should now have terminated, but in case it has not we will send it a hard kill:
            self.log.info("Killing subprocess. (SIGKILL)")
            self._process.kill()
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

        # Close write socket to subprocess. This is the subprocess' cue that the game is over
        if self._process:
            self.log.debug("Closing write channel to subprocess.")
            self._process.stdin.close()

        if self._process:
            self.log.info("Waiting for subprocess to finish.")
            while self._process:
                self._handle_io()

    def on_tick(self):
        # Process as much as we see pending
        while self._process and not self.end_tick_reason and not self.suspended:
            self._handle_io()
            time.sleep(0.2)

    def _readline(self, stream):
        line = stream.readline()  # Bugger... what if there's no newline? Will it block?
        line = codecs.decode(line, stream.encoding or "UTF-8", "replace")
        return line.strip()

    def _writeline(self, stream, line):
        line = codecs.encode(line, stream.encoding or "UTF-8", "replace")
        print >> self._process.stdin, line

    def _handle_io(self):
        while self._process and (self.stopping or (not self.suspended and not self.end_tick_reason)):
            # Process as much as possible

            # if not self._process:
            #     return

            outputs = []
            if not self._process.stdin.closed:
                outputs = [self._process.stdin.fileno()]
            r, w, e = select([self._process.stdout.fileno(), self._process.stderr.fileno()], outputs, [], 0)
            if not r and not e and (not w or self._outgoing.empty()):
                # print "**LEAVING-1"
                return  # Nothing to do right now; let's get some air..

            if self._process.stderr.fileno() in r:
                msg = self._readline(self._process.stderr)
                if msg:
                    self.log.warning("Subprocess stderr: %s" % msg)

            if self._process.stdout.fileno() in r:
                incoming = self._readline(self._process.stdout)
                if incoming:
                    self._send(incoming)
                else:
                    # This is the end... we must have lost contact with the process. Perhaps it finished...
                    self.log.debug("EoF; stopping.")
                    self._process = None
                    self.stop()
                    return

            if outputs and self._process.stdin.fileno() in w:
                # Send an item from the queue
                if not self._outgoing.empty():
                    doc = self._outgoing.get()
                    self._outgoing.task_done()
                    try:
                        self._writeline(self._process.stdin, doc)
                    except Exception as e:
                        self.log.exception("Error writing to subprocess: %s: %s" % (e.__class__.__name__, e.message))
                        self._process = None
                        self.stop()
                        return

    def _incoming(self, document):
        if self._outgoing:
            s = None
            if type(document) in [str, unicode]:
                s = document
            else:
                s = tojson(document)
            # What we put on the _outgoing queue is a ready-to-send string in internal 'str' or 'unicode' format.
            self._outgoing.put(s)

    def _send(self, string):
        # This string received from the subprocess is in internal 'str' or 'unicode' format.
        if self.config.deserialize:
            doc = None
            try:
                doc = json.loads(string)
            except Exception as e:
                self.doclog.error("Failed to deserialize output from assumed JSON string.")
            if doc:
                self._output.send(doc)
        elif string:
            self._output.send(string)

    #region Utility methods

    def send_signal(self, signal):
        if not self._process:
            raise Exception("No process to communicate with.")
        else:
            self._process.send_signal(signal)

    #endregion Utility methods
