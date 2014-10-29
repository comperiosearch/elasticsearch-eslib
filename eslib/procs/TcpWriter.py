__author__ = 'Hans Terje Bakke'

from ..Generator import Generator
import socket, json
from select import select
from ..time import json_serializer_isodate


class TcpWriter(Generator):
    """
    Write incoming documents to a TCP port.
    Documents of type 'str' and 'unicode' are writtes as-is. Other types are attempted written as JSON.

    Connectors:
        input      (*)         : Incoming documents to write to a TCP socket.

    Config:
        hostname      = ""     : Default to any address the machine happens to have. Use "localhost" to enforce local onlu.
        port          = 4000   :
    """
    def __init__(self, **kwargs):
        super(TcpWriter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", None, "Incoming documents to write to a TCP socket.")

        self.config.set_default(
            hostname = "",
            port   = 4000
        )

        self._connections = []  # List of (socket, address) pairs
        self._socket = None

    def on_open(self):
        self._socket = None
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        address = (self.config.hostname #or socket.gethostname()
                   , self.config.port)
        try:
            sock.bind(address)
            #sock.setblocking(0)
            sock.listen(0)  # No backlog limit
            self.log.info("Listening for connections on %s:%d." % address)
        except socket.error as e:
            self.log.critical("Listener failed to bind to %s:%d. (errno=%d, message=%s)" % (self.config.hostname, self.config.port, e.errno, e.args[1]))
            raise e

        self._connections = []
        self._socket = sock

    def on_close(self):
        if self._connections:
            for c in self._connections:
                s, a = c
                s.close()
            self._connections = []
        if self._socket:
            self._socket.close()
            self._socket = None
            self.log.info("Listener closed.")

    def on_tick(self):
        if not self.running or self.stopping:
            return

        r, w, e = select([self._socket], [], [], 0)  # Non-blocking
        for ignore in r:
            c = self._socket.accept()  # Would rather have handled the 'ignore' socket from the read list directly
            s, a = c
            self.log.info("New connection from %s:%d." % a)
            self._connections.append(c)

    def _send(self, data):
        connections = self._connections[:]
        for c in connections:
            s, a = c
            try:
                s.sendall((data + "\n").encode("utf8"))
                #s.flush()
            except socket.error as e:
                if e.errno == socket.errno.EPIPE:  # Broken pipe
                    self.log.info("Connection closed by client %s:%d." % a)
                else:
                    self.log.error("Unhandled error writing to socket from %s:%d. Disconnecting. (errno=%d, message=%s)" %
                                   (a[0], a[1], e.errno, e.args[1]))
                self._connections.remove(c)

    def _incoming(self, document):
        if document:
            data = document
            if not type(document) in [str, unicode]:
                data = json.dumps(document, default=json_serializer_isodate)
            self._send(data)

            self.count += 1
            self.total += 1
