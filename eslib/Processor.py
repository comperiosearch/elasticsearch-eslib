# -*- coding: utf-8 -*-

# Note: Destroying terminals not implemented. Possibly no use for that..

from __future__ import absolute_import

import logging
import threading, time
from .Configurable import Configurable
from .Terminal import Terminal
from .Terminal import TerminalProtocolException
from .TerminalInfo import TerminalInfo
from .Connector import Connector
from .Socket import Socket

class Processor(Configurable):
    "Base class for workflow processing object."

    def __init__(self, **kwargs):
        super(Processor, self).__init__(**kwargs)
        self.sleep = 0.001

        self.config.set_default(name=self.__class__.__name__)

        self.is_generator = False

        # Terminals
        self.sockets    = {}
        self.connectors = {}
        self.default_connector = None
        self.default_socket    = None

        self._setup_logging()

        # Execution control status, needed by generators and monitors
        self.accepting = False
        self.stopping = False
        self.running = False
        self.suspended = False
        self.aborted = False
        self.keepalive = False  # True means that this processor will not be stopped automatically when a producer stops.

        self._thread = None
        self._runchan_count = 0 # Number of running producers, whether connector or local monitor/generator thread
        self._initialized = False # Set only by _setup() and _close() methods! (To avoid infinite circular setup of processor graph.)

    @property
    def name(self):
        return self.config.name

    def _setup_logging(self):  # TODO: MIGHT WANT TO REDO ALL OF THIS...
        # Set up logging
        parts = []
        if not self.__module__ == "__main__": parts.append(self.__module__)
        className = self.__class__.__name__
        parts.append(className)

        name = self.name
        if name:
            if name.endswith(".py"):
                name = name[:-3]
            if not name == className:
                parts.append(name)
        fullPath = ".".join(parts)
        #print "FULL=[%s]" % fullPath
        self.doclog  = logging.getLogger("doclog.%s"  % fullPath)
        self.log     = logging.getLogger("proclog.%s" % fullPath)

    def _iter_subscribers(self):
        for socket in self.sockets.itervalues():
            for connector in socket.connections:
                yield connector.owner

    #region Terminal creation

    def create_connector(self, method, name=None, protocol=None, description=None, is_default=False):
        """
        Create a connector (input) for this processor.

        :param        method      : Method that will be called for processing incoming items.
        :param str    name        : Name of the connector.
        :param str    protocol    : Protocol that must comply to the protocol of sockets it will connect to.
        :param str    description : Description text for the connector.
        :param bool   is_default  : Whether this should be registered as the default connector that can be addressed
                                    without a name. Only one can exist for a set of connectors for a processor.
        :return Connector : Returns the new connector.
        """
        terminal = Connector(name, protocol, method)
        if terminal.name in self.connectors:
            raise Exception("Connector name '%s' already exists for processor '%s'." % (terminal.name, self.name))
        terminal.owner = self
        terminal.description = description
        self.connectors.update({terminal.name: terminal})
        if is_default:
            self.default_connector = terminal
        return terminal

    def create_socket(self, name=None, protocol=None, description=None, is_default=False, mimic=None):
        """
        Create a socket (output) for this processor.

        :param str    name        : Name of the socket.
        :param str    protocol    : Protocol that connecting connectors must comply with.
        :param str    description : Description text for the socket.
        :param bool   is_default  : Whether this should be registered as the default socket that can be addressed
                                    without a name. Only one can exist for a set of sockets for a processor.
        :param Connector mimic    : A connector whose connections' (if any) protocol to mimic.
        :return Socket : Returns the new socket.
        """
        terminal = Socket(name, protocol, mimic)
        if terminal.name in self.sockets:
            raise Exception("Socket name '%s' already exists for processor '%s'." % (terminal.name, self.name))
        terminal.owner = self
        terminal.description = description
        self.sockets.update({terminal.name: terminal})
        if is_default:
            self.default_socket = terminal
        return terminal

    #endregion Terminal creation

    #region Connection management

    @staticmethod
    def _get_connector(subscriber, connector_name):
        """
        :return socket:
        """

        connector = None
        if len(subscriber.connectors) == 0:
            raise Exception("Processor '%s' has no connectors." % subscriber.name)

        if not connector_name:
            if len(subscriber.connectors) > 1:
                default = subscriber.default_connector
                if default:
                    connector = default
                else:
                    raise Exception("Processor '%s' has more than one connector and no default set, requires connector_name to be specified." % subscriber.name)
            else:
                connector = subscriber.connectors.itervalues().next()
        else:
            connector = subscriber.connectors._get(connector_name)
            if not connector:
                raise Exception("Connector named '%s' not found in processor '%s'." % (connector_name, subscriber.name))

        return connector

    @staticmethod
    def _get_socket(producer, socket_name):
        """
        :return socket:
        """

        socket = None
        if not socket_name:
            if len(producer.sockets) > 1:
                default = producer.default_socket
                if default:
                    socket = default
                else:
                    raise Exception("More than one socket for '%s' and no default, requires socket_name to be specified." % producer.name)
            else:
                socket = producer.sockets.itervalues().next()
        else:
            socket = producer.sockets._get(socket_name)
            if not socket:
                raise Exception("Socket named '%s' not found in processor '%s'." % (socket_name, producer.name))

        return socket

    @staticmethod
    def _get_terminals(producer, socket_name, subscriber, connector_name):
        """
        :return (socket, connector):
        """

        if len(producer.sockets) == 0:
            raise Exception("Processor '%s' has no sockets to connect to." % producer.name)

        connector = Processor._get_connector(subscriber, connector_name)
        socket = Processor._get_socket(producer, socket_name)
        return (socket, connector)

    def subscribe(self, producer, socket_name=None, connector_name=None):
        """
        Connect this named connector to a named socket on a Processor."
        :param Processor producer: Processor which socket to subscribe to.
        :param str socket_name: Optional if only one socket exists on the other processor (producer).
        :param str connector_name: Optional if only one connector exists for this processor.
        :return self: For fluent programming style.
        """

        # Find the specified terminals
        socket, connector = Processor._get_terminals(producer, socket_name, self, connector_name)

        # Verify protocol compliance
        if not Terminal.protocol_compliance(socket, connector):
            raise TerminalProtocolException(socket, connector)

        # Attach connector as output target for socket
        socket.attach(connector)
        connector.attach(socket)

        return self # For fluent programming

    def unsubscribe(self, producer=None, socket_name=None, connector_name=None):
        """
        Remove a named connector as output target for specified producer socket.
        :param Processor producer: If missing, socket_name and connector_name are ignored, and all subscriptions are removed.
        :param str socket_name: Optional if only one socket exists in the other processor (producer).
        :param str connector_name: If missing, detach all connections to given socket.
        :return self: For fluent programming style.
        """

        for connector in self.connectors.itervalues():
            if not connector_name or connector.name == connector.name:
                for socket in connector.get_connections(producer, socket_name):
                    socket.detach(connector)
                    connector.detach(socket)

        return self # For fluent programming

    def attach(self, subscriber, socket_name=None, connector_name=None):
        """
        Connect this named connector to a named socket on a Processor."
        :param Processor subscriber: Processor that will subscribe to output from this processor.
        :param str socket_name: Optional if only one socket exists for this processor.
        :param str connector_name: Optional if only one connector exists on the other processor (subscriber).
        :return self: For fluent programming style.
        """

        # Find the specified terminals
        socket, connector = Processor._get_terminals(self, socket_name, subscriber, connector_name)

        # Verify protocol compliance
        if not Terminal.protocol_compliance(socket, connector):
            raise TerminalProtocolException(socket, connector)

        # Attach connector as output target for socket
        socket.attach(connector)
        connector.attach(socket)

        return self # For fluent programming

    def detach(self, subscriber=None, socket_name=None, connector_name=None):
        """
        Detach a subscriber.
        :param str socket_name: If missing, detach from all sockets.
        :param Processor subscriber: If missing, detach all subscribers.
        :param str connector_name: If missing, detach all connections from subscriber. Ignored if missing subscriber.
        :return self: For fluent programming style.
        """

        for socket in self.sockets.itervalues():
            if not socket_name or socket.name == socket_name:
                for connector in socket.get_connections(subscriber, connector_name):
                    socket.detach(connector)
                    connector.detach(socket)

        return self # For fluent programming

    @property
    def has_output(self):
        for socket in self.sockets.itervalues():
            if socket.has_output: return True
        return False

    def connector_info(self, *args):
        "Return list of info for connectors named in *args, or all connectors."
        return [TerminalInfo(self.connectors[n]) for n in self.connectors if not args or n in args ]

    def socket_info(self, *args):
        "Return list of info for sockets named in *args, or all sockets."
        return [TerminalInfo(self.sockets[n]) for n in self.sockets if not args or n in args ]

    #endregion Connection management

    #region Handlers for all processors types

    def on_open    (self): pass

    def on_abort   (self): pass

    def on_close   (self): pass

    #endregion Handlers for all processor types

    #region Handlers for Generator/Monitor type Processor

    # self.is_generator = True

    def on_startup (self):
        """
        This method is called at the beginning of the worker thread. No on_tick or other generator events
        will be called before it has completed. It is NOT GUARANTEED to have finished before
        connectors start delivering documents. on_open, however, is always called before connectors are started."
        """
        pass

    def on_shutdown(self): pass

    def on_tick    (self): pass

    def on_suspend (self): pass

    def on_resume  (self): pass

    #endregion Handlers for Generator/Monitor type Processor

    #region Operation management

    def _run(self):

        self._runchan_count += 1

        self.on_startup()

        while self.running:
            if self.sleep:
                time.sleep(self.sleep)

            if self.stopping:
                if self._runchan_count == 1: # Now it is only us left...
                    self.on_shutdown()
                    self.production_stopped() # Ready to close down
            elif not self.suspended:
                self.on_tick()

        if self.aborted:
            #self.on_abort()
            self._close()
            self._runchan_count -= 1 # If stopped normally, it was decreased in call to production_stopped()

    def start(self):
        "Start running (if not already so) and start accepting and/or generating new data. Cascading to all subscribers."

        if self.stopping:
            raise Exception("Processor '%s' is stopping, cannot restart yet." % self.name)

        # Make sure we and all subscribers are properly initialized
        self._setup()
        # Accept incoming from connectors and tell subscribers to accept incoming
        self._accept_incoming()
        # Now we can finally tell all parts to start running/processing without missing data...
        self._start_running()

    def _setup(self):
        if self._initialized:
            return
        self.on_open()
        self._initialized = True
        # Tell all subscribers, cascading, to run setup
        for subscriber in self._iter_subscribers():
            subscriber._setup()

    def _close(self):
        if not self._initialized:
            print "*** %s WAS NOT INITIALIZED BEFORE CALL TO _close()!" % self.name # DEBUG
        self.on_close()
        self._initialized = False
        # Note: Do NOT tell subscribers to close. They will do this themselves after they have been stopped or aborted.

    def _accept_incoming(self):

        if self.accepting:
            return

        if self.stopping:
            raise Exception("Processor '%s' is stopping. Refusing to accept new incoming again until fully stopped.")

        self.accepting = True
        # Tell all connectors to accept
        for connector in self.connectors.itervalues():
            connector.accept_incoming()
        # Tell all subscribers, cascading, to accept incoming
        for subscriber in self._iter_subscribers():
            subscriber._accept_incoming()

    def _start_running(self):

        if self.running:
            return

        self._runchan_count = 0  # Should not really be necessary if all is well..
        # Tell all connectors to start running
        for connector in self.connectors.itervalues():
            connector.run()
            self._runchan_count += 1
        # Tell all subscribers to start running
        for subscriber in self._iter_subscribers():
            subscriber._start_running()
        # Start running this, if generator/monitor
        self.aborted = False
        self.stopping = False
        self.suspended = False
        self.running = True
        if self.is_generator:
            self._thread = threading.Thread(target=self._run)
            self._thread.start()

    def stop(self):
        """
        Stop accepting new data, reading input and/or generating new data. Finish processing and write to output sockets.
        Cascade to subscribers once all data is processed.
        :param bool cascading: Tell all subscribers to stop once this is done stopping.
        """

        if self.stopping or not self.running:
            return

        #print "*** STOPPING %s" % self.name # DEBUG

        # Do not generate any more data or fetch any more from remote sources
        self.accepting = False
        self.stopping = True
        # Continue processing input in connector queues, but do not accept more incoming data on the connectors
        # Note: It is first when all connector queues are empty (and thus processed) that we can tell subscribers to stop.
        for connector in self.connectors.itervalues():
            connector.stop()

    def production_stopped(self):
        self._runchan_count -= 1
        if self._runchan_count == 0:
            # We are all done... no longer generating, and no longer receiving
            self.stopping = False
            self.running = False
            self._close()
            # Now we can finally tell subscribers to stop
            for subscriber in self._iter_subscribers():
                if not subscriber.keepalive:  # Honour 'keepalive', i.e. do not propagate stop-signal to subscriber
                    subscriber.stop()

    def abort(self):
        "Abort all input reading and data processing immediately. Stop accepting new data. Empty queues and stop running."

        if self.aborted or not self.running:
            return

        # Abort connectors
        for connector in self.connectors.itervalues():
            connector.abort()

        # Abort monitoring remote source or generating output.
        self.aborted = True
        self.accepting = False
        self.running = False
        self.stopping = False  # We will stop immediately
        self.on_abort()

        if not self.is_generator:  # Otherwise handled in the _run() loop
            self._close()

        # Cascade abort to subscribers
        for subscriber in self._iter_subscribers():
            subscriber.abort()

    def suspend(self):
        "Suspend processing data and writing output. Continue to accept incoming input."

        # Suspend monitoring remote source or generating output.
        self.suspended = True
        self.on_suspend()
        # Suspend all connectors from polling items from its queue and requesting processing
        for connector in self.connectors.itervalues():
            connector.suspend()

    def resume(self):
        "Resume data processing."

        # Resume monitoring remote source or generating output.
        self.suspended = False
        self.on_resume()
        # Resume all connectors
        for connector in self.connectors.itervalues():
            connector.resume()

    def wait(self):
        "Wait for this processor to finish."

        while self.running:
            time.sleep(0.1)  # wait 0.1 seconds

        if self._thread and self._thread.isAlive():
            self._thread.join()
        self._thread = None

    #endregion Operation management

    #region Send and receive data with external methods

    def put(self, document, connector_name=None):
        connector = self._get_connector(self, connector_name)
        if not connector:
            raise Exception("Connector not found.")
        if not connector.accepting:
            raise Exception("Connector %s.%s is not currently accepting input." % (self.name, connector.name))
        connector.receive(document)

    def add_callback(self, method, socket_name=None):
        socket = self._get_socket(self, socket_name)
        if not socket:
            raise Exception("Socket not found.")
        socket.callbacks.append(method)

    #endregion Send and receive data with external methods

    #region Debugging

    def DUMP_connectors(self, include_connections=True, verbose=False, indent=0, *args):
        ii = self.connector_info(*args)
        [i.DUMP(include_connections, verbose, indent) for i in ii]

    def DUMP_sockets(self, include_connections=True, verbose=False, indent=0, *args):
        ii = self.socket_info(*args)
        [i.DUMP(include_connections, verbose, indent) for i in ii]

    def DUMP(self, include_connections=True, verbose=False):
        print "PROCESSOR %s (type=%s)" % (self.name or "???", self.__class__.__name__)
        print "  todo: description and special config..."
        print "TERMINALS:"
        self.DUMP_connectors(include_connections, verbose, 1)
        self.DUMP_sockets(include_connections, verbose, 1)

    #endregion Debugging
