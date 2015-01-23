__author__ = 'Hans Terje Bakke'

import logging
import time, os, threading
from ..Configurable import Configurable

class ServiceOperationError(Exception):
    def __init__(self, *args, **kwargs):
        super(ServiceOperationError, self).__init__(self, *args, **kwargs)

# Service states
class status(object):
    PENDING    = "pending"        # service running, but waiting for metadata configuration
    IDLE       = "idle"           # service running, but not processing documents
    PROCESSING = "processing"     # document processing currently in progress
    SUSPENDED  = "suspended"      # document processing currently suspended
    STOPPING   = "stopping"       # processing is stopping
    ABORTED    = "aborted"        # document processing aborted
    FAILED     = "failed"         # service failure, no longer running
    CLOSING    = "shutting_down"  # service is shutting down
    DOWN       = "down"           # service has been shut down (or was never started)
    DEAD       = "DEAD"           # service OS process is not even running


class Service(Configurable):

    # Class level properties
    config_keys = []

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
    def pid(self):
        return os.getpid()

    @property
    def name(self):
        return self.config.name

    @property
    def status(self):
        if self._failed:
            return status.FAILED
        if not self._running:
            return status.DOWN
        if self._closing:
            return status.CLOSING
        if self._processing_aborted:
            return status.ABORTED
        if self._processing_stopping:
            return status.STOPPING
        if self.processing_suspended:
            return status.SUSPENDED
        if self.processing:
            return status.PROCESSING
        if self.requires_metadata and not self._metadata_initialized:
            return status.PENDING
        return status.IDLE

    @property
    def processing(self):
        if not self._processing:
            return False
        if self._processing_stopping:
            return True  # Still considered to be working with the items..
        return self.is_processing()  # TODO: What the hell to do if this call fails??

    @property
    def processing_suspended(self):
        return self.is_suspended()  # TODO: What the hell to do if this call fails??

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
        "Register a processor as part of the service. (For debugging.)"
        for proc in procs:
            self._registered_procs.append(proc)
        return len(procs)

    def unregister_procs(self, *procs):
        "Unregister a processor as part of the service. (For debugging.)"
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
        except Exception as e:
            self.log.exception("Exception in wrapped function '%s'." % func.__name__)
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

    def shutdown(self, wait=False):
        "Shut down the controller, including document processors."

        if not self._running:
            #raise ServiceOperationError("Service is not running.")
            return False  # Not shut down; was already shut down, or failed
        if self._closing:
            #raise ServiceOperationError("Service is already shutting down.")
            return False  # Not shut down; was already closing

        if wait:
            if not self._shutdown():
                return False
            return self.wait()
        else:
            thread = threading.Thread(target=self._shutdown)
            thread.daemon = False  # Program shall not exit while this thread is running
            thread.start()
            # no waiting
            return True  # It is shutting down

    def _shutdown(self):
        self._closing = True

        if self.processing:
            if not self._processing_stop():
                self.log.error("Processing failed to stop; service not shut down.")
                self._closing = False
                return False

        ok = self._call_failable(self.on_shutdown)
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
            return True

        self.log.debug("Waiting until service is shut down.")
        if not self._call_failable(self.on_wait):
            self.log.warning("Call to on_wait failed!")
            return False
        while self._running:
            time.sleep(0.1)
        # Note: _closing and _running should have been set to False by the shutdown thread
        return True

    #endregion Service management commands

    #region Processing management commands

    def processing_start(self, raise_on_error=False):
        if self.processing:
            if raise_on_error:
                raise ServiceOperationError("Service is already processing.")
            else:
                return False  # Not started; was already running
        self.log.info("Starting processing.")
        ok = self.on_processing_start()
        if ok:
            self._processing = True
            self._processing_aborted = False
            self.log.status("Processing started.")
        elif raise_on_error:
            raise ServiceOperationError("Processing failed to start.")
        return ok

    def processing_restart(self, wait=False, raise_on_error=False):
        # NOTE: 'wait' is currently not used

        ok = False
        if self.processing:
            # RESTART

            # Note: There is currently no self._processing_restarting flag
            self.log.info("Restarting processing.")
            ok = self.on_processing_restart()
            if ok:
                self.log.status("Processing restarted.")
            elif raise_on_error:
                raise ServiceOperationError("Processing failed to restart.")
            return ok
        else:
            # START

            self.log.info("Starting processing.")
            ok = self.on_processing_start()
            if ok:
                self._processing = True
                self._processing_aborted = False
                self.log.status("Processing started.")
            elif raise_on_error:
                raise ServiceOperationError("Processing failed to start.")
        return ok

    def processing_stop(self, wait=False, raise_on_error=False):
        if not self.processing:
            if raise_on_error:
                raise ServiceOperationError("Service is not processing.")
            else:
                return False  # Not stopped; was not running

        if wait:
            ok = self._processing_stop(raise_on_error)
            if ok:
                ok = self.processing_wait()
            if not ok and raise_on_error:
                raise ServiceOperationError("Service failed to stop.")
            return ok
        else:
            thread = threading.Thread(target=self._processing_stop)
            thread.daemon = False  # Program shall not exit while this thread is running
            thread.start()
            # no waiting
            return True  # It is shutting down

    def _processing_stop(self, raise_on_error=False):

        self.log.info("Stopping processing.")
        self._processing_stopping = True
        ok = self.on_processing_stop()
        # It is no longer stopping regardless of whether it succeeds (with 'ok') or not:
        self._processing_stopping = False
        if ok:
            self.log.status("Processing stopped.")
        else:
            self.log.error("Processing failed to stop.")
        return ok

    def processing_abort(self, raise_on_error=False):
        if not self.processing:
            if raise_on_error:
                raise ServiceOperationError("Service is not processing.")
            else:
                return False  # Not aborted; was not running
        self.log.info("Aborting processing.")
        ok = self.on_processing_abort()
        if ok:
            self._processing_stopping = False
            self._processing_aborted = True
        if ok:
            self.log.status("Processing aborted.")
        elif raise_on_error:
            raise ServiceOperationError("Processing was not aborted.")
        return ok

    def processing_suspend(self, raise_on_error=False):
        if self.processing_suspended:
            if raise_on_error:
                raise ServiceOperationError("Service is already suspended.")
            else:
                return False  # Not suspended; was already suspended
        if self._processing_stopping:
            if raise_on_error:
                raise ServiceOperationError("Service is in the process of stopping.")
            else:
                return False  # Not suspended; is currently stopping
        if not self.processing:
            if raise_on_error:
                raise ServiceOperationError("Service is not processing.")
            else:
                return False  # Not suspended; was not processing
        self.log.info("Suspending processing.")
        ok = self.on_processing_suspend()
        if ok:
            self.log.status("Processing suspended.")
        elif raise_on_error:
            raise ServiceOperationError("Processing failed to suspend.")
        return ok

    def processing_resume(self, raise_on_error=False):
        if not self.processing_suspended:
            if raise_on_error:
                raise ServiceOperationError("Service was not suspended.")
            else:
                return False  # Not suspended; was already suspended
        if not self.processing:
            if raise_on_error:
                raise ServiceOperationError("Service is not processing; nothing to resume.")
            else:
                return False  # Not suspended; was not processing
        self.log.info("Resuming processing.")
        ok = self.on_processing_resume()
        if ok:
            self.log.status("Processing resumed.")
        elif raise_on_error:
            raise ServiceOperationError("Processing failed to resume.")
        return ok

    def processing_wait(self, raise_on_error=False):
        if not self.processing:
            return True
        self.log.debug("Waiting until service has stopped processing.")
        while self.processing:
            time.sleep(0.1)
        return True

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

    def is_suspended(self):
        "Evaluate whether processing is suspended."
        return False

    def on_processing_start(self):
        return True
    def on_processing_restart(self):
        return True
    def on_processing_stop(self):
        return True
    def on_processing_abort(self):
        return True
    def on_processing_suspend(self):
        return True
    def on_processing_resume(self):
        return True
    def on_update_metadata(self, metadata):
        return True  # No update is an ok update

    #endregion Processing management methods for override


