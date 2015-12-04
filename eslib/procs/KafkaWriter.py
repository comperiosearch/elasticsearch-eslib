__author__ = 'Hans Terje Bakke'

# NOTE: Using sync producer. Should change to async if performance sucks.

from ..Processor import Processor
from ..esdoc import tojson
from pykafka import KafkaClient


class KafkaWriter(Processor):
    """
    Write data to Kafka.
    Writes data with type 'str', 'unicode', 'int', or 'float'. Lists and dicts are written as 'json'.
    Other types are cast to 'str'.
    The 'type' registered with the metadata is then either 'str', 'unicode', 'int', 'float' or 'json'.

    Connectors:
        input      (*)       : Document to write to configured Kafka topic.

    Config:
        hosts             = ["localhost:9292"]    : List of Kafka hosts.
        topic             = "default_topic"       :
    """

    def __init__(self, **kwargs):
        super(KafkaWriter, self).__init__(**kwargs)

        self._connector = self.create_connector(self._incoming, "input", None, "Document to write to configured RabbitMQ.")

        self.config.set_default(
            hosts = ["localhost:9092"],
            topic = "default_topic"
        )

        self._client   = None
        self._producer = None

    def on_open(self):
        self.count = 0
        self._client = KafkaClient(",".join(self.config.hosts))
        topic = self._client.topics[self.config.topic]
        self._producer = topic.get_sync_producer()
        self.log.info("Connected to Kafka topic '%s'." % self.config.topic)

    def on_close(self):
        if self._client:
            self._producer.stop()
            self.log.info("Kafka producer stopped.")
            # Can't find any way to close the connection or ask it to release resources, so I try a 'del'.
            del self._client
            self._client = None
            self.log.debug("Connection to Kafka deleted.")

    def _incoming(self, document):
        if document == None:
            return

        data = document
        msg_type = None
        if isinstance(document, basestring):
            msg_type = type(document).__name__
        elif isinstance(document, (int, long, float)):
            msg_type = type(document).__name__
        elif isinstance(document, (list, dict)):
            data = document
            msg_type = "json"
        else:
            data = str(document)
            msg_type = "str" #type(document).__name__
            self.doclog.warning("Writing document of unsupported type '%s' as type 'str'." % type(document).__name__)

        kafka_data = None
        try:
            kafka_data = tojson({"type": msg_type, "data": data})
        except TypeError as e:
            self.doclog.error("JSON serialization failed: %s" % e.message)
            return

        self._producer.produce(kafka_data)
        self.count += 1
