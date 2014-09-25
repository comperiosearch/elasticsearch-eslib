__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
from .RabbitmqBase import RabbitmqBase
import pika, json


class RabbitmqWriter(Processor, RabbitmqBase):
    """
    Write data to Rabbit MQ.
    Data in 'str' or 'unicode' types are written directly, everything else is written as a JSON string.
    The 'type' registered with the metadata is then either 'str', 'unicode' or 'json'.

    Connectors:
        input      (*)       : Document to write to configured RabbitMQ.

    Config:
        host              = localhost  :
        port              = 5672       :
        admin_port        = 15672      :
        username          = guest      :
        password          = guest      :
        virtual_host      = None       :
        queue             = "default"  :
    """

    def __init__(self, name=None):
        #super(RabbitmqWriter, self).__init__(name)
        Processor.__init__(self, name)
        RabbitmqBase.__init__(self)

        self.create_connector(self._incoming, "input", None, "Document to write to configured RabbitMQ.")

    def on_open(self):
        self._open_connection()

    def on_close(self):
        self._close_connection()

    def _incoming(self, document):
        if document == None:
            return

        data = None
        msg_type = None
        if type(document) in [str, unicode]:
            data = document
            msg_type = type(document).__name__
        elif type(document) in [int, float]:
            data = str(document)
            msg_type = type(document).__name__
        elif type(document) in [list, dict]:
            try:
                data = json.dumps(document)
            except TypeError as e:
                self.doclog.warning("JSON serialization failed: %s" % e.message)
                return
            msg_type = "json"
        else:
            data = str(document)
            msg_type = "str" #type(document).__name__
            self.doclog.debug("Writing document of unsupported type '%s' as type 'str'." % type(document).__name__)

        properties = pika.BasicProperties(
            delivery_mode = 2,  # make messages persistent
            type = msg_type
        )

        self._channel.basic_publish(exchange="", routing_key=self.config.queue, body=data, properties=properties)
