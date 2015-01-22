__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
from SocketServer import ThreadingTCPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from ..esdoc import tojson
import json, logging

class _ServerHandlerClass(SimpleHTTPRequestHandler):

    # Override end_headers() to allow cross-domain requests
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        SimpleHTTPRequestHandler.end_headers(self)

    # Overload to make this thing shut up!!
    def log_message(self, format, *args):
        pass

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

    # def do_GET(self):
    #     "Treat URL path below root as a document of 'str' type."
    #     # Use path as data
    #     path = self.path
    #     self.server.owner.doclog.debug("GET : Incoming document from '%s', length=%d." % (self.client_address, len(path)))
    #     res = self.server.owner._incoming_GET(self, "GET", path, path)
    #     self._respond(res)
    #
    def _do_VERB_that_takes_data(self, verb):

        # Note: We ignore content type (requirement would be "application/json" or whatever..), and assume it is JSON.

        owner = self.server.owner
        max_length = owner.config.max_length

        length = int(self.headers.get('Content-Length') or 0)
        if owner.doclog.isEnabledFor(logging.TRACE):
            owner.doclog.trace("%s: Incoming document from '%s', length=%d." % (verb, self.client_address, length))

        if max_length and length > max_length:
            owner.doclog.debug()
            owner.doclog.warning("%s: Incoming document of size %d exceeded max length of %d." % (verb, length, max_length))
            self._respond_error("Too large data bulk dropped by server. Length = %d exceeding max length = %d." % (length, max_length))
        else:
            path = self.path#[1:]
            data = self.rfile.read(length).decode('utf-8')
            res = self.server.owner._incoming_WITH_DATA(self, verb, path, data)
            self._respond(res)

    def do_GET(self):
        self._do_VERB_that_takes_data("GET")
    def do_PUT(self):
        self._do_VERB_that_takes_data("PUT")
    def do_POST(self):
        self._do_VERB_that_takes_data("POST")
    def do_DELETE(self):
        self._do_VERB_that_takes_data("DELETE")


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

    def _incoming_WITH_DATA(self, request_handler, verb, path, data):
        if self.suspended:
            return "Server suspended; incoming data ignored."

        if verb == "GET":
            self.total += 1
            self.count += 1
            # For GET, use path as document
            if self.output.has_output:
                self.output.send(path[1:])

        data_obj = None
        if data:
            if request_handler.headers.gettype() == "application/json":
                try:
                    data_obj = json.loads(data)
                except Exception as e:
                    self.doclog.error("Failed to parse incoming data as JSON: " + e.message)
                    return "Failed to parse data as JSON."
            else:
                data_obj = data

        if verb != "GET":
            self.total += 1
            self.count += 1
            if self.output.has_output:
                # For non-GET, use data as document
                self.output.send(data_obj)

        return self._call_hook(request_handler, verb, path, data_obj)

    def _call_hook(self, request_handler, http_verb, path, data):
        if self.hook:
            try:
                return self.hook(request_handler, http_verb, path, data)
            except Exception as e:
                self.log.exception("Failed to call hook: %s: %s" % (e.__class__.__name__, e))


    def on_startup(self):
        self.total = 0
        self.count = 0

    def on_tick(self):
        self._server.handle_request()
