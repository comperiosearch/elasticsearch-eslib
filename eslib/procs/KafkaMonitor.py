__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
from pykafka import KafkaClient
import json, time
import logging


class KafkaMonitor(Monitor):
    """
    Monitor a Kafka topic.
    Assumes data with type 'str', 'unicode', 'int', 'float' or 'json' from RabbitMQ.
    Incoming documents are attempted deserialized into these types. Unknown types are passed as 'str'.

    Sockets:
        output     (*)       : Document received on monitored queue.

    Config:
        hosts             = ["localhost:9292"]    : List of Kafka hosts.
        zookeeper_hosts   = ["localhost:2181"]    : For balanced consumption via zookeeper.
        topic             = "default_topic"       :
        consumer_group    = "default_group"       : Balanced consumer group.
    """

    CONGESTION_SLEEP_TIME = 10.0
    WORK_TIME             = 5.0

    def __init__(self, **kwargs):
        super(KafkaMonitor, self).__init__(**kwargs)

        self.output = self.create_socket("output", None, "Document received on monitored queue.")

        self.config.set_default(
            hosts           = ["localhost:9092"],
            zookeeper_hosts = ["localhost:2181"],
            topic           = "default_topic",
            consumer_group  = "default_group"
        )

        self._client   = None
        self._consumer = None

    #region Processor stuff

    def on_open(self):
        self.count = 0
        self._client = KafkaClient(",".join(self.config.hosts))
        topic = self._client.topics[self.config.topic]
        self._consumer = topic.get_balanced_consumer(
            auto_commit_enable = True,
            consumer_group     = self.config.consumer_group,
            zookeeper_connect  = ",".join(self.config.zookeeper_hosts)
        )

        self.log.info("Connected to Kafka topic '%s', balanced via zookeeper." % self.config.topic)

    def on_close(self):
        if self._client:
            self._consumer.stop()
            #del self._consumer
            self.log.info("Kafka consumer stopped.")
            # Can't find any way to close the connection or ask it to release resources, so I try a 'del'.
            del self._client
            self._client = None
            self.log.debug("Connection to Kafka deleted.")

    #endregion Processor stuff

    #region Generator stuff

    def on_startup(self):
        self.count = 0

    def on_tick(self):

        congested = self.congestion()
        if congested:
            self.log.debug("Congestion in dependent processor '%s'; sleeping %d seconds." % (congested.name, self.CONGESTION_SLEEP_TIME))
            self.congestion_sleep(self.CONGESTION_SLEEP_TIME)
        else:
            # Read as much as we can for WORK_TIME seconds, then return to controlling
            # loop. This way this processor should hang a maximum of WORK_TIME seconds
            # before accepting control commands.
            start_time = time.time()
            while True:
                if self.end_tick_reason:
                    return
                if time.time() - start_time > self.WORK_TIME:
                    self.log.debug("Work time exceeded %s seconds. Returning to control loop." % self.WORK_TIME)
                try:
                    kafka_message = self._consumer.consume(block=False)
                except Exception as e:
                    self.log.error("Error consuming Kafka. Aborting. [%s]" % e.__class__.__name__)
                    self.abort()
                    return
                if kafka_message is None:
                    return

                self.count += 1

                if not self.output.has_output: # Don't bother with further message processing, in this case.
                    return

                document = self._decode_message(kafka_message.value)
                if document is not None:
                    self.output.send(document)

    def _decode_message(self, kafka_data):

        # print "INCOMING KAFKA DATA: [%s]" % kafka_data

        if not kafka_data:
            return None

        msg_type = None
        document = None
        try:
            jj = json.loads(kafka_data)
#            kafka_data = tojson({"type": msg_type, "data": data})
        except TypeError as e:
            self.doclog.warning("JSON deserialization failed: %s" % e.message)
            return None
        msg_type = jj.get("type")
        document = jj.get("data")
        if not msg_type or document is None:
            return None

        if self.log.isEnabledFor(logging.TRACE):
            self.log.trace("Received message of type '%s', Kafka payload size = %d." % (msg_type, len(kafka_data)))
        return document

    #endregion Generator stuff
