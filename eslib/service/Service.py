__author__ = 'Hans Terje Bakke'

import logging
import time
from ..Configurable import Configurable

class ServiceOperationError(Exception):
    def __init__(self, *args, **kwargs):
        super(ServiceOperationError, self).__init__(self, *args, **kwargs)

# Service states
class status(object):
    PENDING    = "pending"        # service running, but waiting for metadata configuration
    IDLE       = "idle"           # service running, but not processing documents
    PROCESSING = "processing"     # document processing currently in progress
    STOPPING   = "stopping"       # processing is stopping
    ABORTED    = "aborted"        # document processing aborted
    FAILED     = "failed"         # service failure, no longer running
    CLOSING    = "shutting_down"  # service is shutting down
    DEAD       = "dead"           # service has been shut down (or was never started)


class Service(Configurable):
    def __init__(self, **kwargs):
        super(Service, self).__init__(**kwargs)

        self.config.set_default(name=self.__class__.__name__)

        self._setup_logging()

        self._running = False
        self._failed = False
        self._processing = False
        self._processing_aborted = False
        self._processing_stopping = False
        self._closing = False

        self._metadata_initialized = False
        self._registered_procs = []

        # Overriding classes should set this to True if the service needs metadata
        self.requires_metadata = False

    def __str__(self):
        return "%s|%s (%s)" % (self.__class__.__name__, self.name, self.status)

    @property
    def name(self):
        return self.config.name

    @property
    def status(self):
        if self._failed:
            return status.FAILED
        if not self._running:
            return status.DEAD
        if self._closing:
            return status.CLOSING
        if self._processing_aborted:
            return status.ABORTED
        if self._processing_stopping:
            return status.STOPPING
        if self.processing:
            return status.PROCESSING
        if self.requires_metadata and not self._metadata_initialized:
            return status.PENDING
        return status.IDLE

    @property
    def processing(self):
        if not self._processing:
            return False
        return self.is_processing()  # TODO: What the hell to do if this call fails??

    def _setup_logging(self):
        serviceName = self.name

        self.log = logging.getLogger("servicelog.%s"  % serviceName)
        self.log.serviceName  = serviceName
        self.log.className    = self.__class__.__name__
        self.log.instanceName = serviceName

    #region Debugging

    def DUMP(self):
        fmt = "%-15s %-15s %-7s %-8s %-9s %-7s %-9s %-4s %-4s %-5s %-5s"
        print fmt % ("Type", "Name", "Running", "Stopping", "Accepting", "Aborted", "Suspended", "Ins", "Outs", "Keep", "Count")
        for item in self._registered_procs:
            producers = 0
            subscribers = 0
            for p in item.connectors.itervalues():
                producers += len(p.connections)
            for p in item.sockets.itervalues():
                subscribers += len(p.connections)
            print fmt % (item.__class__.__name__, item.name, item.running, item.stopping, item.accepting, item.aborted, item.suspended, producers, subscribers, item.keepalive, item.count)

    def register_procs(self, *procs):
        "Register a processor as part of the controller. (For debugging.)"
        for proc in procs:
            self._registered_procs.append(proc)
        return len(procs)

    def unregister_procs(self, *procs):
        "Unregister a processor as part of the controller. (For debugging.)"
        registered = 0
        for proc in procs:
            if proc in self._registered_procs:
                self._registered_procs.append(proc)
                registered += 1
        return registered

    #endregion Debugging

    #region Service management commands

    def configure(self, credentials, config, global_config):
        """
        Configure the controller from configs.
        :param dict credentials:
        :param dict config:
        :param dict global_config:
        :return:
        """
        return self.on_configure(credentials, config, global_config)

    def _call_failable(self, func):
        try:
            ok = func()
        except:
            ok = False
        if not ok:
            self._failed = True
            return False
        return True

    def run(self, wait=False):
        "Start running the controller itself. (Not the document processors.)"

        if self._running:
            raise ServiceOperationError("Service is already running.")
            #return False  # Not started; was already running, or failed

        self._failed = False
        self._processing_aborted = False
        self._processing = False

        self.log.info("Setting up processors.")
        if not self._call_failable(self.on_setup):
            self.log.critical("Setup failed.")
            return False

        if not self._call_failable(self.on_run):
            self.log.critical("Startup failed.")
            return False

        self._running = True
        self.log.status("Service running.")

        if wait:
            self.log.info("Waiting until service is shut down.")
            if not self._call_failable(self.on_wait):
                return False
            self.log.status("Service stopped.")
        return True  # Started; and potentially stopped again, in case we waited here

    def shutdown(self, wait=True):
        "Shut down the controller, including document processors."

        if not self._running:
            raise ServiceOperationError("Service is not running.")
            #return False  # Not started; was already running, or failed
        if self._closing:
            raise ServiceOperationError("Service is already shutting down.")
            #return False  # Not started; was already running, or failed

        self._closing = True

        if self.processing:
            if not self._stop_processing():
                self.log.error("Processing failed to stop; service not shut down.")
                self._closing = False
                return False

        ok = self._call_failable(self.on_shutdown)
        if ok and wait and not self._running:
            self.log.info("Waiting until service is shut down.")
            ok = self._call_failable(self.on_wait)

        if ok:
            self.log.status("Service shut down.")
        else:
            # Something went wrong during shutdown. Leave run loop and mark as failed.
            self.log.error("Service shut down with error during shut down process.")

        self._closing = False
        self._running = False
        return ok

    def wait(self):
        "Wait until controller is shut down."

        if not self._running:
            return False  # Not shut down; was not running

        self.log.info("Waiting until service is shut down.")
        ok = self._call_failable(self.on_wait)
        if ok:
            self.log.status("Service shut down.")
        else:
            # Something went wrong during shutdown. Leave run loop and mark as failed.
            self.log.error("Service shut down with error during shut down process.")

        self._closing = False
        self._running = False
        return ok

    #endregion Service management commands

    #region Processing management commands

    def start_processing(self):
        # TODO
        ok = self.on_start_processing()
        self._processing = True
        return ok

    def restart_processing(self):
        # TODO
        return self.on_restart_processing()

    def stop_processing(self):
        if not self._processing:
            raise ServiceOperationError("Service is not processing.")
            #return False  # Not started; was already running, or failed
        return self._stop_processing()

    def _stop_processing(self):
        self.log.info("Stopping processing.")
        self._stopping = True
        ok = self.on_stop_processing()
        print "ON_STOP_PROCESSING RETURNS=", ok
        self._stopping = False
        return ok

    def wait_processing(self):
        # TODO
        while self.processing:
            time.sleep(0.1)
        return True

    def abort_processing(self):
        # TODO
        return self.on_abort_processing()

    def suspend_processing(self):
        # TODO
        return self.on_suspend_processing()

    def resume_processing(self):
        # TODO
        return self.on_resume_processing()

    def update_metadata(self, metadata):
        # TODO
        return self.on_update_metadata(metadata)

    #endregion Processing management commands

    #region Setup methods for override

    def on_configure(self, credentials, config, global_config):
        return True  # For when the service is set up from static config dicts

    def on_setup(self):
        return True  # Create pipeline elements based on static config

    #endregion Setup methods for override

    #region Service management methods for override

    def on_run(self):
        return True

    def on_shutdown(self):
        return True

    def on_wait(self):
        while self._running:
            time.sleep(0.1)
        return True

    #endregion Service management methods for override

    #region Processing management methods for override

    def is_processing(self):
        "Evaluate whether processing is in progress."
        return False

    def on_start_processing(self):
        return True
    def on_restart_processing(self):
        return True
    def on_stop_processing(self):
        return True
    def on_abort_processing(self):
        return True
    def on_suspend_processing(self):
        return True
    def on_resume_processing(self):
        return True
    def on_update_metadata(self, metadata):
        return True  # No update is an ok update

    #endregion Processing management methods for override


