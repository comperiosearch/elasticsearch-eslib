__author__ = 'Hans Terje Bakke'

from ..Generator import Generator
from ..esdoc import tojson
import socket
from select import select


class TcpWriter(Generator):
    """
    Write incoming documents to a TCP port.
    Documents of type 'str' and 'unicode' are writtes as-is. Other types are attempted written as JSON.

    NOTE: This processor operates as a Generator, but is considered to be passive; hence keepalive defaults to False.

    Connectors:
        input      (*)         : Incoming documents to write to a TCP socket.

    Config:
        hostname      = ""     : Default to any address the machine happens to have. Use "localhost" to enforce local onlu.
        port          = 4000   :
        reuse_address = False  : Whether to allow reusing an existing TCP address/port.
    """
    def __init__(self, **kwargs):
        super(TcpWriter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", None, "Incoming documents to write to a TCP socket.")

        self.keepalive = False  # Passive of nature, hence this default

        self.config.set_default(
            hostname      = "",
            port          = 4000,
            reuse_address = False
        )

        self._connections = []  # List of (socket, address) pairs
        self._socket = None

    def on_open(self):
        self._socket = None
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.config.reuse_address:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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

    @staticmethod
    def _get_conn(connections, sock):
        for c in connections:
            if c[0] == sock:
                return c
        return None

    def on_tick(self):
        if not self.running or self.stopping:
            return

        r, w, e = select([self._socket], [], [self._socket], 0)  # Non-blocking
        if e:
            self.log.warning("Error on server socket -- now what?")
        if r:
            # We have one or more new connections pending. Get one and return to run loop.
            c = self._socket.accept()
            s, a = c
            self.log.info("New connection from %s:%d." % a)
            self._connections.append(c)

        # Check for dead connections
        connections = self._connections[:]
        sockets = [s for s,a in connections]
        r, w, e = select(sockets, [], sockets, 0)
        if e:
            self.log.warning("Error on connected socket -- now what?")
        for s in r:
            # This socket is intended for write only, but since there is now data,
            # we read a bit just to work down the input buffer. If it is empty, getting
            # here means the connection has been closed on the other end, and we can remove it.
            data = s.recv(1024)
            if not data:
                s.close()
                c = self._get_conn(connections, s)
                if c and c in self._connections:
                    self.log.info("Connection closed by client %s:%d." % c[1])
                    self._connections.remove(c)
                else:
                    self.log.info("Unknown connection closed by client.")

    def _send(self, data):
        connections = self._connections[:]
        for c in connections:
            s, a = c
            try:
                s.sendall((data + "\n").encode("utf8"))
                #s.flush()
            except socket.error as e:
                if e.errno == socket.errno.EPIPE:  # Broken pipe
                    self.log.info("Connection closed by client %s:%d. (Broken pipe)" % a)
                else:
                    self.log.error("Unhandled error writing to socket from %s:%d. Disconnecting. (errno=%d, message=%s)" %
                                   (a[0], a[1], e.errno, e.args[1]))
                self._connections.remove(c)

    def _incoming(self, document):
        if document:
            data = document
            if not type(document) in [str, unicode]:
                data = tojson()
            self._send(data)

            self.count += 1
            self.total += 1
