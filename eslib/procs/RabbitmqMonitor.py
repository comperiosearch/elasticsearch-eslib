__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
from .RabbitmqBase import RabbitmqBase
import pika
import json, time

class RabbitmqMonitor(Monitor, RabbitmqBase):
    """
    Monitor a queue in RabbitMQ.
    Assumes data with type 'str', 'unicode', 'int', 'float' or 'json' from RabbitMQ.
    Incoming documents are attempted deserialized into these types. Unknown types are passed as 'str'.

    Sockets:
        output     (*)       : Document received on monitored queue.

    Config:
        host              = localhost  :
        port              = 5672       :
        admin_port        = 15672      :
        username          = guest      :
        password          = guest      :
        virtual_host      = None       :
        exchange          = None       :
        queue             = "default"  : Not used if 'exchange' is specified.
        consuming         = True       : Consume from the queue, rather than to listen on an
                                         exclusive queue that will be deleted when disconnect.
                                         Non-consuming behaviour only works with an 'exchange'.
        max_reconnects    = 3          :
        reconnect_timeout = 3          :
    """

    CALC_TOTAL          = True  # Whether to check our the RabbitMQ at intervals and calculate a total
                                # from current count and remaining in queue. It thus becomes a moving
                                # target for ETA calculations.
    CALC_TOTAL_INTERVAL = 10.0  # seconds

    _is_reader = True

    def __init__(self, **kwargs):
        super(RabbitmqMonitor, self).__init__(**kwargs)

        self.output = self.create_socket("output", None, "Document received on monitored queue.")

        self.config.set_default(
            max_reconnects    = 3,
            reconnect_timeout = 3
        )

        self._reconnecting = 0
        self._last_calc_total = 0

    #region Processor stuff

    def on_open(self):
        self._open_connection()
        self.log.info("Connected to RabbitMQ.")

    def on_close(self):
        self._calc_total()
        if self._close_connection():
            self.log.info("Connection to RabbitMQ closed.")

    #endregion Processor stuff

    #region Generator stuff

    def _start_consuming(self):
        self._consumer_tag = self._channel.basic_consume(self._callback, queue=self._queue_name, no_ack=True)

    def _stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._consumer_tag)

    def on_startup(self):
        if self.CALC_TOTAL:
            self.total = 0  # We will collect this from message queue, otherwise it should be set to None
            self._last_calc_total = 0
        self.count = 0
        self._start_consuming()

    def on_shutdown(self):
        self._stop_consuming()

    def on_abort(self):
        self._stop_consuming()

    def on_suspend(self):
        self._stop_consuming()

    def on_resume(self):
        self._start_consuming()

    def on_tick(self):
        if self._reconnecting > 0:
            self._reconnecting -= 1
            # Try to reconnect
            ok = False
            try:
                self._close_connection()
                self._open_connection()
                self.log.info("Successfully reconnected to RabbitMQ.")
                self.reconnecting = 0  # No longer attempting reconnects
                self._start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                if self._reconnecting > 0:
                    timeout = self.config.reconnect_timeout
                    self.log.warning("Reconnect to RabbitMQ failed. Waiting %d seconds." % timeout)
                    time.sleep(timeout)
                else:
                    self.log.critical("Missing connection to RabbitMQ. Max retries exceeded. Aborting.")
                    self.abort()  # We give up and abort
            return

        try:
            self._calc_total()
            congested = self.congestion()
            if congested:
                self.log.debug("Congestion in dependent processor '%s'; sleeping 10 seconds." % congested.name)
                self.congestion_sleep(10.0)
            else:
                self._channel.connection.process_data_events()
        except Exception as e:
            if self._reconnecting >= 0:
                self.log.info("No open connection to RabbitMQ. Trying to reconnect.")
                self._reconnecting = self.config.max_reconnects  # Number of reconnect attempts; will start reconnecting on next tick

    def _calc_total(self):
        """
        Calculate total number of messages.
        That is the sum of what is processed so far, and what remains in the queue.
        """
        if not self.CALC_TOTAL:
            return

        now = time.time()
        if now - self._last_calc_total > self.CALC_TOTAL_INTERVAL:
            try:
                self.total = self.get_queue_size() + self.count
            except Exception as e:
                self.log.warning("Failed to get queue size for queue '%s': %s" % (self._queue_name, e))
            self._last_calc_total = now

    def _callback(self, callback, method, properties, body):
        #print "*** RabbitmqMonitor received:"
        #print "***    Properties:", properties
        #print "***    Body: ", body

        self.count += 1

        if not self.output.has_output: # Don't bother deserializing, etc, in this case
            return

        try:
            msg_type = properties.type
            document = None
            if msg_type == "json":
                try:
                    document = json.loads(body)
                except TypeError as e:
                    self.doclog.warning(e.message)
                    return
            elif msg_type in ["str", "unicode"]:
                document = body
            elif msg_type == "int":
                document = int(str(body))
            elif msg_type == "float":
                document = float(str(body))
            elif body:
                self.doclog.debug("Received document of type='%s'; converting to str.", msg_type)
                document = str(body)

            if document != None:
                self.output.send(document)
            else:
                self.doclog.warning("Received empty document from RabbitMQ.")
        except Exception as e:
            self.log.error("An exception occurred inside the callback: %s" % e.message)

    #endregion Generator stuff
