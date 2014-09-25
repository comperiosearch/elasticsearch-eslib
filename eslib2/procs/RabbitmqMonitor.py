__author__ = 'Hans Terje Bakke'

# NOTE: Currently using blocking connection and basic_consume.
#       For a good example on asynchronous consumer using select and ioloop, see:
#           http://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html

from ..Generator import Generator
from .RabbitmqBase import RabbitmqBase
import json

class RabbitmqMonitor(Generator, RabbitmqBase):
    """

    """

    def __init__(self, name=None):
        Generator.__init__(self, name)
        RabbitmqBase.__init__(self)

        self.output = self.create_socket("output", None, "Document received on monitored queue.")

    #region Processor stuff

    def on_open(self):
        self.log.info("open")
        self._open_connection()

    def on_close(self):
        self.log.info("close")
        self._close_connection()

    #endregion Processor stuff

    #region Generator stuff

    def on_startup(self):
        self.total = 0
        self.count = 0
        self._consumer_tag = self._channel.basic_consume(self._callback, queue=self.config.queue, no_ack=True)
        self._channel.start_consuming()

    def on_shutdown(self):
        self._channel.basic_cancel(self._consumer_tag)
        self._channel.stop_consuming()

    def on_abort(self):
        self._channel.basic_cancel(self._consumer_tag)
        self._channel.stop_consuming()

    def on_tick(self):
        pass

    def on_suspend(self):
        self._channel.stop_consuming()

    def on_resume(self):
        self._channel.start_consuming()

    def _callback(self, callback, method, properties, body):
        #print "*** RabbitmqMonitor received:"
        #print "***    Properties:", properties
        #print "***    Body: ", body

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
            self.log.error("An exception occured inside the callback: %s" % e.message)

    #endregion Generator stuff
