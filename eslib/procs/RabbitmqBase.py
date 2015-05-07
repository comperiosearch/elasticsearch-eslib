__author__ = 'Hans Terje Bakke'

# TODO: Perhaps rewrite with a better async implementation. Example here:
#   http://pika.readthedocs.org/en/latest/examples/asynchronous_consumer_example.html

from ..Configurable import Configurable
import pyrabbit.api as rabbit
from pyrabbit.http import HTTPError
import pika
import time

class RabbitmqBase(Configurable):

    _is_reader = True  # For inheriting classes to specify.

    def __init__(self, **kwargs):
        super(RabbitmqBase, self).__init__(**kwargs)

        self.config.set_default(
            host         = "localhost",
            port         = 5672,
            admin_port   = 15672,
            username     = None,
            password     = None,
            virtual_host = None,
            queue        = "default",
            exchange     = None,
            consuming    = True,  # Used by monitor/reader
            persisting   = True,  # Used by writer
        )

        self.config.max_reconnects    = 3
        self.config.reconnect_timeout = 3

        # Pika connection and channel
        self._connection = None
        self._channel = None
        # Queue name assigned by server
        self._queue_name = None

    #region Admin

    # returns (host, queue_name, vhost_name)
    def _get_addr(self, vhost, name):
        if not vhost and not self.config.virtual_host:
            raise ValueError("Virtual host must be specified either explicitly or through 'self.config.virtual_host'.")

        return (
            "%s:%d" % (self.config.host, self.config.admin_port),
            vhost or self.config.virtual_host,
            name or self._queue_name or self.config.queue
        )

    def delete_exchange(self, name=None, vhost=None):
        exchange = name or self.config.exchange
        if not exchange:
            return
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        try:
            client.delete_exchange(vh, exchange)
        except HTTPError as e:
            if e.status == 404:
               self.log.debug("Could not delete exchange '%s', not found." % q)

    def create_queue(self, name=None, vhost=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        client.create_queue(vh, q)

    def delete_queue(self, name=None, vhost=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        try:
            client.delete_queue(vh, q)
        except HTTPError as e:
            if e.status == 404:
               self.log.debug("Could not delete queue '%s', not found." % q)

    def purge_queue(self, name=None, vhost=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        try:
            client.purge_queue(vh, q)
        except HTTPError as e:
            if e.status == 404:
               self.log.debug("Could not purge queue '%s', not found." % q)

    def get_queue_size(self, name=None, vhost=None):
        return self.get_queue(name, vhost)["messages_ready"]

    def get_queue(self, name=None, vhost=None):
        (h, vh, q) = self._get_addr(vhost, name)
        client = rabbit.Client(h, self.config.username, self.config.password)
        return client.get_queue(vh, q)

    def get_queues(self, queues=None, vhost=None):
        (h, vh, q) = self._get_addr(vhost, None)
        client = rabbit.Client(h, self.config.username, self.config.password)
        qq = [q for q in client.get_queues(vh) if not queues or q["name"] in queues]
        return qq

    def DUMP_QUEUES(self, queues=None, vhost=None):
        queues = self.get_queues(vhost, queues)
        ##print queues
        # Some potentially interesting stuff:
        # idle_since (date as string)
        # messages_unacknowledged
        # consumers
        # durable
        # auto_delete
        # memory
        # state
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
            print fmt_d % (q["name"], q["state"], q.get("messages_ready") or 0, q.get("messages_unacknowledged") or 0, q.get("messages") or 0)

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
            virtual_host=self.config.virtual_host,
            heartbeat_interval=60))
        self._channel = self._connection.channel()

        if self.config.exchange:
            # Make sure the exchange exists
            self._channel.exchange_declare(exchange=self.config.exchange, type="fanout")

            if self._is_reader:
                self._open_connection_reader()
            else:
                self._open_connection_writer()

        else:
            # Make sure the queue exists
            result = self._channel.queue_declare(queue=self.config.queue, durable=True)
            self._queue_name = result.method.queue

    def _open_connection_reader(self):

        # Exchange mode only handled here. (Not queue-only mode.)

        if self.config.consuming:
            # Shared/consuming mode.
            result = self._channel.queue_declare(queue=self.config.exchange + "_shared", durable=True)
        else:
            # Exclusive/non-consuming mode.
            # This queue will be deleted when we close the connection.
            # TODO: Perhaps create the queue ID ourselves so it is easier to see which exchange it belongs to?
            result = self._channel.queue_declare(exclusive=True)

        # Bind queue to exchange.
        self._queue_name = result.method.queue
        self._channel.queue_bind(exchange=self.config.exchange, queue=self._queue_name)

    def _open_connection_writer(self):

        # Exchange mode only handled here. (Not queue-only mode.)

        if self.config.persisting:
            # Make sure one durable queue exists.
            result = self._channel.queue_declare(queue=self.config.exchange + "_shared", durable=True)
            # Bind queue to exchange.
            self._queue_name = result.method.queue
            self._channel.queue_bind(exchange=self.config.exchange, queue=self._queue_name)
        else:
            # Exclusive/non-persisting mode: do nothing.
            pass


    def _close_connection(self):
        self._queue_name = None
        if self._connection:
            try:
                self._channel.close()
            except:
                pass
            self._channel = None
            try:
                self._connection.close()
            except:
                pass
            self._connection = None
            return True

    def _reconnect(self, attempts, timeout):
        attempts = 3
        while attempts >= 0:
            attempts -= 1
            try:
                self._open_connection()
            except pika.exceptions.AMQPConnectionError as e:
                if attempts > 0:
                    self.log.warning("Reconnect to RabbitMQ failed. Waiting %d seconds." % timeout)
                    time.sleep(timeout)
            else:
                return True
        return False

    def _publish(self, msg_type, data):

        while (not self._connection or not self._connection.is_open) and self.running and not self.aborted:
            self.log.debug("No open connection to RabbitMQ. Trying to reconnect.")
            try:
                self._open_connection()
                self.log.debug("Successfully reconnected to RabbitMQ.")
            except pika.exceptions.AMQPConnectionError as e:
                timeout = 3
                self.log.warning("Reconnect to RabbitMQ failed. Waiting %d seconds." % timeout)
                time.sleep(timeout)

        properties = pika.BasicProperties(
            delivery_mode = 2,  # make messages persistent
            type = msg_type
        )

        ok = False
        try_again = True
        while not ok and try_again:
            try:
                self._channel.basic_publish(
                    exchange=self.config.exchange or "",
                    routing_key=self._queue_name or "",  # Fanout exchange will ignore this
                    body=data,
                    properties=properties)
                ok = True
            except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed) as e:
                if try_again:
                    self.log.debug("No open connection to RabbitMQ. Trying to reconnect.")
                    try_again = self._reconnect(3, 3)

        if not ok:
            self.log.warning("Missing connection to RabbitMQ. Max retries exceeded. Document lost. Aborting.")
            self.abort()
            return False
        else:
            return True

    #endregion Pika connection management helpers
