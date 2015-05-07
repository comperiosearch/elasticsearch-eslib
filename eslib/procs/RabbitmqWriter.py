__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
from .RabbitmqBase import RabbitmqBase
from ..esdoc import tojson
import time


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
        persisting        = True       : When this is on, the exchange will store data in a queue until it
                                         is consumed by a consuming monitor. Otherwise, data will only be
                                         queued if there is a listener.
        max_reconnects    = 3          :
        reconnect_timeout = 3          :
        max_queue_size    = 100000     : If the output queue exceeds this number, this processor is considered congested.
    """

    MAX_CONNECTOR_QUEUE_SIZE = 10000
    CHECK_QUEUE_INTERVAL = 5 # 5 seconds; how often to check whether the message queue is "congested"

    _is_reader = False  # This is a writer

    def __init__(self, **kwargs):
        super(RabbitmqWriter, self).__init__(**kwargs)

        self._connector = self.create_connector(self._incoming, "input", None, "Document to write to configured RabbitMQ.")

        self.config.set_default(
            persisting     = True,
            max_queue_size = 100000
        )

        self._last_check_queue_time = 0
        self._last_known_queue_size = 0


    def on_open(self):
        self._last_check_queue_time = 0
        self._last_known_queue_size = 0

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
        if super(RabbitmqWriter, self).is_congested():
            return True
        if self._connector.queue.qsize() > self.MAX_CONNECTOR_QUEUE_SIZE:
            return True
        elif not self.config.exchange or self.config.persisting:
            if self.config.max_queue_size:
                now = time.time()
                if now - self._last_check_queue_time > self.CHECK_QUEUE_INTERVAL:
                    try:
                        self._last_known_queue_size = self.get_queue_size()
                    except Exception as e:
                        self.log.warning("Failed to get queue size for queue '%s': %s" % (self._queue_name, e))
                    self._last_check_queue_time = now

                if self._last_known_queue_size > self.config.max_queue_size:
                    return True

        return False
