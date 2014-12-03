__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
from SocketServer import ThreadingTCPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from ..esdoc import tojson
import json

class _ServerHandlerClass(SimpleHTTPRequestHandler):

    def _respond_error(self, message, code=None):
        self.send_error(code or 500, message or "Unspecified server error")
        self.end_headers()

    def _respond(self, payload, code=None, message=None):
        """
        res[0]  # response code
        res[1]  # response message
        res[2]  # response body)
        """
        if payload is None:
            # TODO: Should we send a 200 response here or should this one have no response at all?
            self.send_response(code or 200, message or "OK")
        else:
            if type(payload) in [None, str, unicode]:
                self.send_response(code or 200, message or "OK")
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(payload)
            else:
                try:
                    body = tojson(payload) # TODO: my own serializer
                except Exception as e:
                    self.server.owner.log.error("Failed to serialize JSON response: %s: %s" % (e.__class__.__name__, e))
                    self.send_error(500, "Failed to serialize json response.")
                else:
                    self.send_response(code or 200, message or "Ok")
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(body)

    def do_GET(self):
        "Treat URL path below root as a document of 'str' type."
        # Use path as data
        path = self.path[1:]
        self.server.owner.doclog.debug("GET : Incoming document from '%s', length=%d." % (self.client_address, len(path)))
        res = self.server.owner._incoming_GET(path)
        self._respond(res)

    def do_POST(self):

        # Note: We ignore content type (requirement would be "application/json" or whatever..), and assume it is JSON.

        owner = self.server.owner
        max_length = owner.config.max_length

        length = int(self.headers.get('Content-Length') or 0)
        owner.doclog.debug("POST: Incoming document from '%s', length=%d." % (self.client_address, length))

        if max_length and length > max_length:
            owner.doclog.debug()
            owner.doclog.warning("POST: Incoming document of size %d exceeded max length of %d." % (length, max_length))
            self._respond_error("Too large data bulk dropped by server. Length = %d exceeding max length = %d." % (length, max_length))
        else:
            path = self.path[1:]
            data = self.rfile.read(length).decode('utf-8')
            res = self.server.owner._incoming_POST(path, data)
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

    ThreadingTCPServer.allow_reuse_address = True  # OBS: Class level setting.

    def __init__(self, hook=None, **kwargs):
        super(HttpMonitor, self).__init__(**kwargs)
        self.output = self.create_socket("output", None, "Document received on the HTTP endpoint.")
        self.config.set_default(host="localhost", port=4000)

        self.config.set_default(
            host       = "localhost",
            port       = 4000,
            max_length = 1024*1024  # 1 MB
        )

        self.hook = hook

    def on_open(self):
        self.log.info("Starting HTTP listener on %s:%d" % (self.config.host, self.config.port))
        self._server = ThreadingTCPServer((self.config.host, self.config.port), _ServerHandlerClass, bind_and_activate=True)
        self._server.owner = self
        self._server.timeout = 1.0  # Max 1 second blocking in _server.handle_request()

    def on_close(self):
        self.log.info("Closing HTTP listener.")
        self._server.server_close()
        self._server = None

    def _incoming_GET(self, path):
        if self.suspended:
            return "Server suspended; incoming data ignored."

        #if not path:
        #    return "Missing data."

        self.total += 1
        self.count += 1
        self.output.send(path)

        return self._call_hook("GET", path, None)

    def _incoming_POST(self, path, data):
        if self.suspended:
            return "Server suspended; incoming data ignored."

        #if not data:
        #    return "Missing data."

        if data:
            # TODO: Check for content type application/json before trying to deserialize json
            try:
                doc = json.loads(data)
            except Exception as e:
                self.doclog.error("Failed to parse incoming data as JSON: " + e.message)
                return "Failed to parse data as JSON."

            self.total += 1
            self.count += 1
            self.output.send(doc)

        return self._call_hook("POST", path, data)

    def _call_hook(self, http_verb, path, data):
        if self.hook:
            try:
                return self.hook(http_verb, path, data)
            except Exception as e:
                self.log.exception("Failed to call hook: %s: %s" % (e.__class__.__name__, e))


    def on_startup(self):
        self.total = 0
        self.count = 0

    def on_tick(self):
        self._server.handle_request()
