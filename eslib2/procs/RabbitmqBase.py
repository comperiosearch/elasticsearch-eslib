__author__ = 'Hans Terje Bakke'

import pyrabbit.api as rabbit
import pika

class RabbitmqBase(object):
    def __init__(self):

        # NOTE: Assumes the existence of self.config from other inherited object (Processor)

        self.config.host         = "localhost"
        self.config.port         = 5672
        self.config.admin_port   = 15672
        self.config.username     = None
        self.config.password     = None
        self.config.virtual_host = None
        self.config.queue        = "default"

        # Pika connection and channel
        self._connection = None
        self._channel = None

    #region Admin

    # returns (host, queue_name, vhost_name)
    def _get_addr(self, vhost, name):

        if not vhost and not self.config.virtual_host:
            raise ValueError("Virtual host must be specified either explicitly or through 'self.config.virtual_host'.")

        return (
            "%s:%d" % (self.config.host, self.config.admin_port),
            vhost or self.config.virtual_host,
            name or self.config.queue
        )

    def create_queue(self, vhost=None, name=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        client.create_queue(vh, q)

    def delete_queue(self, vhost=None, name=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        client.delete_queue(vh, q)

    def purge_queue(self, vhost=None, name=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        client.purge_queue(vh, q)

    def get_queues(self, vhost=None, *args):
        (h, vh, q) = self._get_addr(vhost, None)
        client = rabbit.Client(h, self.config.username, self.config.password)
        queues = [q for q in client.get_queues() if not args or q["name"] in args]
        return queues

    def DUMP_QUEUES(self, vhost=None, *args):
        queues = self.get_queues(vhost, *args)
        # Some potentially interesting stuff:
        # idle_since (date as string)
        # messages_unacknowledged
        # consumers
        # durable
        # auto_delete
        # memory
        # status
        # incoming (list)
        # name
        # len
        # pending_acks#
        # messages
        # messages_ready

        fmt_h = "%-20s %-10s %-5s %-5s %-5s"
        fmt_d = "%-20s %-10s %5d %5d %5d"
        u = "-"
        print fmt_h % ("Name", "Status", "Ready", "Unack", "Total")
        print fmt_h % (u*20, u*10, u*5, u*5, u*5)
        for q in queues:
            print fmt_d % (q["name"], q["status"], q.get("messages_ready") or 0, q.get("messages_unacknowledged") or 0, q.get("messages") or 0)

    #endregion Admin

    #region Pika connection management helpers

    def _open_connection(self):
        credentials = None
        if self.config.username:
            credentials = pika.PlainCredentials(self.config.username, self.config.password)

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            credentials=credentials,
            virtual_host=self.config.virtual_host))
        self._channel = self._connection.channel()
        # Make sure the queue exists
        self._channel.queue_declare(queue=self.config.queue, durable=True)

    def _close_connection(self):
        if self._connection:
            self._channel.close()
            self._channel = None
            self._connection.close()
            self._connection = None

    #endregion Pika connection management helpers
