__author__ = 'Hans Terje Bakke'

# TODO: Use queue/exchange so that multiple monitors can connect and get the same data.
# TODO:    Right now they consume (empty) the queue and contend for the same data.

from ..Processor import Processor
from .RabbitmqBase import RabbitmqBase
from ..esdoc import tojson


class RabbitmqWriter(Processor, RabbitmqBase):
    """
    Write data to RabbitMQ.
    Writes data with type 'str', 'unicode', 'int', or 'float'. Lists and dicts are written as 'json'.
    Other types are cast to 'str'.
    The 'type' registered with the metadata is then either 'str', 'unicode', 'int', 'float' or 'json'.

    Connectors:
        input      (*)       : Document to write to configured RabbitMQ.

    Config:
        host              = localhost  :
        port              = 5672       :
        admin_port        = 15672      :
        username          = guest      :
        password          = guest      :
        virtual_host      = None       :
        exchange          = None       : If specified, data is written to this 'exchange', and also
                                         persisted on a durable queue '<exchange>_shared'. Clients can
                                         ask to listen to the exchange on this queue ('consumable'
                                         behaviour, the default), or to listen to a live stream on an
                                         exclusive queue that is a copy of all data meant only for that
                                         listener. Clients connected to the shared queue will consume data
                                         from it, thus splitting workload (intended) or competing for the
                                         same data (unintended).
        queue             = "default"  : Not used if 'exchange' is specified.

        max_reconnects    = 3          :
        reconnect_timeout = 3          :
    """

    def __init__(self, **kwargs):
        super(RabbitmqWriter, self).__init__(**kwargs)

        self._connector = self.create_connector(self._incoming, "input", None, "Document to write to configured RabbitMQ.")

    def on_open(self):
        self.count = 0
        self._open_connection()
        self.log.info("Connected to RabbitMQ.")

    def on_close(self):
        if self._close_connection():
            self.log.info("Connection to RabbitMQ closed.")

    def _incoming(self, document):
        if document == None:
            return

        data = None
        msg_type = None
        if isinstance(document, basestring):
            data = document
            msg_type = type(document).__name__
        elif isinstance(document, (int, long, float)):
            data = str(document)
            msg_type = type(document).__name__
        elif isinstance(document, (list, dict)):
            try:
                data = tojson(document)
            except TypeError as e:
                self.doclog.error("JSON serialization failed: %s" % e.message)
                return
            msg_type = "json"
        else:
            data = str(document)
            msg_type = "str" #type(document).__name__
            self.doclog.warning("Writing document of unsupported type '%s' as type 'str'." % type(document).__name__)

        if self._publish(msg_type, data):
            self.count += 1

    def is_congested(self):
        return self._connector.queue.qsize() > 10000
