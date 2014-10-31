__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
from SocketServer import TCPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import json


class _ServerHandlerClass(SimpleHTTPRequestHandler):

    def _respond(self, res):
        # Send a response (hmm... should we bother?)
        if res:
            self.send_response(404, res)
        else:
            self.send_response(200, "Ok")
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_GET(self):
        "Treat URL path below root as a document of 'str' type."
        data = self.path[1:]

        self.server.owner.doclog.debug("GET : Incoming document from '%s', length %d." % (self.client_address, len(data)))

        res = self.server.owner._incoming_GET(data)
        self._respond(res)

    def do_POST(self):

        # Note: We ignore content type (requirement would be "application/json" or whatever..), and assume it is JSON.

        owner = self.server.owner
        max_length = owner.config.max_length

        length = int(self.headers['Content-Length'])
        owner.doclog.debug("POST: Incoming document from '%s', length %d." % (self.client_address, length))

        if max_length and length > max_length:
            owner.doclog.debug()
            owner.doclog.warning("POST: Incoming document of size %d exceeded max length of %d." % (length, max_length))
            self._respond("Too large data bulk dropped by server. Length = %d exceeding max length = %d." % (length, max_length))
        else:
            data = self.rfile.read(length)#.decode('utf-8')
            res = self.server.owner._incoming_POST(data)
            self._respond(res)

class HttpMonitor(Monitor):
    """
    Monitor incoming documents on a HTTP endpoint.

    For messages received via HTTP GET it uses the the path from URL path after host address as a 'str' type input.
    For messages received via HTTP POST it expects the content body to be JSON.

    Sockets:
        output     (*)       : Document received on the HTTP endpoint.

    Config:
        host              = "localhost"
        port              = 4000
        max_length        = 1024*1024     : Max length of incoming data via POST, in bytes.
    """

    def __init__(self, **kwargs):
        super(HttpMonitor, self).__init__(**kwargs)
        self.output = self.create_socket("output", None, "Document received on the HTTP endpoint.")
        self.config.set_default(host="localhost", port=4000)

        self.config.set_default(
            host       = "localhost",
            port       = 4000,
            max_length = 1024*1024  # 1 MB
        )

        TCPServer.allow_reuse_address = True  # OBS: Class level setting.

    def on_open(self):
        self.log.info("Starting HTTP listener on %s:%d" % (self.config.host, self.config.port))
        self._server = TCPServer((self.config.host, self.config.port), _ServerHandlerClass, bind_and_activate=True)
        self._server.owner = self
        self._server.timeout = 1.0  # Max 1 second blocking in _server.handle_request()

    def on_close(self):
        self.log.info("Closing HTTP listener.")
        self._server.server_close()
        self._server = None

    def _incoming_GET(self, data):
        if self.suspended:
            return "Server suspended; incoming data ignored."

        if not data:
            return "Missing data."

        self.total += 1
        self.count += 1
        self.output.send(data)
        return None  # Ok

    def _incoming_POST(self, data):

        if self.suspended:
            return "Server suspended; incoming data ignored."

        if not data:
            return "Missing data."

        try:
            doc = json.loads(data)
        except Exception as e:
            self.doclog.error("Failed to parse incoming data as JSON: " + e.message)
            return "Failed to parse data as JSON."

        self.total += 1
        self.count += 1
        self.output.send(doc)
        return None  # Ok

    def on_startup(self):
        self.total = 0
        self.count = 0

    def on_tick(self):
        self._server.handle_request()
