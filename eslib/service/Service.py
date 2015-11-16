__author__ = 'Hans Terje Bakke'

import logging
import time, os, threading
from ..Configurable import Configurable
from .. import esdoc
import psutil


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
    metadata_keys = []

    def __init__(self, **kwargs):
        super(Service, self).__init__(**kwargs)

        self.config.set_default(name=self.__class__.__name__)

        self._setup_logging()

        # For outer book keeping only
        self.config_file          = None   # The manager needs to know this so it can spawn processes based on the same config
        self.config_key           = None

        self._running             = False
        self._failed              = False
        self._processing          = False
        self._processing_aborted  = False
        self._processing_stopping = False
        self._closing             = False

        # For debugging
        self._registered_procs = []

        # Metadata management
        self.metadata_version        = None
        self.metadata                = {}
        self._metadata_initialized   = False

        # Initialize statistics counters
        self.stat_processing_started = 0
        self.stat_processing_ended   = 0
        self.stat_service_started    = 0
        self.stat_max_memory         = 0
        self.stat_max_threads        = 0
        self.stat_cpu_percent        = 0
        self.stat_max_cpu_percent    = 0

        self.stat_count              = 0
        self.stat_count_total        = None  # There might not be a total count..
        self.stat_dps                = None
        self.stat_eta                = None  # If there is no total count... it may never end..

        self._last_stat_tick         = 0
        self._time_at_resume         = 0
        self._count_at_resume        = 0
        self._process_cpu_time       = 0
        self._last_count             = 0

        self._stat_thread_interval   = 10  # 10 seconds
        self._stat_thread_running    = False
        self._stat_thread            = None

        # self.log.debug("Starting stat thread.")
        # self._stat_thread = threading.Thread(target=self._stat_thread_run, name="stat")
        # self._stat_thread.daemon = True
        # self._stat_thread.start()

    def __del__(self):
        # if self.stat_thread:
        #     self.log.debug("Stopping stat thread.")
        #     self._stat_thread_running = False
        #     self._stat_thread.join()
        #     self.log.debug("Stat thread stopped.")
        #     self.stat_thread = None
        pass

    def __str__(self):
        return "%s|%s (%s)" % (self.__class__.__name__, self.name, self.status)

    def _stat_thread_run(self):
        self._stat_thread_running = True
        while self._stat_thread_running:
            self._stat_tick()
            for i in range(int(self._stat_thread_interval)):
                if not self._stat_thread_running:
                    break
                time.sleep(1.0)

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
        if self.processing_aborted:
            return status.ABORTED
        if self._processing_stopping:
            return status.STOPPING
        if self.processing_suspended:
            return status.SUSPENDED
        if self.processing:
            return status.PROCESSING
        if self.metadata_keys and not self._metadata_initialized:
            return status.PENDING
        return status.IDLE

    @property
    def processing(self):
        if not self._processing:
            return False
        if self._processing_stopping:
            return True  # Still considered to be working with the items..
        is_processing = self.is_processing()  # TODO: What the hell to do if this call fails??

        # To set this correctly, we need a dependable callback from the last processor ending;
        # otherwise, it depends on us polling, like here
        if not is_processing and not self.stat_processing_ended:
            self.stat_processing_ended = time.time()

        return is_processing

    @property
    def processing_aborted(self):
        if not self._processing_aborted:
            return False
        is_processing_aborted = self.is_aborted()  # TODO: What the hell to do if this call fails??
        return is_processing_aborted

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
        self.stat_service_started = time.time()

        self.log.debug("Starting stat thread.")
        self._stat_thread = threading.Thread(target=self._stat_thread_run, name="stat")
        self._stat_thread.daemon = True
        self._stat_thread.start()

        if wait:
            self.log.info("Waiting until service is shut down.")
            if not self._call_failable(self.on_wait):
                return False
            self.log.status("Service stopped.")
            self.stat_service_ended = time.time()
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

        if self._stat_thread:
            self.log.debug("Stopping stat thread.")
            self._stat_thread_running = False
            self._stat_thread.join()
            self.log.debug("Stat thread stopped.")
            self._stat_thread = None

        ok = self._call_failable(self.on_shutdown)
        if ok:
            self.log.status("Service shut down.")
        else:
            # Something went wrong during shutdown. Leave run loop and mark as failed.
            self.log.error("Service shut down with error during shut down process.")

        self._closing = False
        self._running = False
        self.stat_service_ended = time.time()

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
        return self._processing_start(raise_on_error)

    def _processing_start(self, raise_on_error):
        if self.status == status.PENDING:
            msg = "Cannot start processing until metadata is set."
            self.log.warning(msg)
            if raise_on_error:
                raise ServiceOperationError(msg)
            else:
                return False  # Not started; was already running

        self.log.info("Starting processing.")
        ok = self.on_processing_start()
        if ok:
            self._processing             = True
            self._processing_aborted     = False
            self.log.status("Processing started.")
            self.stat_processing_started = time.time()
            self.stat_processing_ended   = 0
            self._time_at_resume         = self.stat_processing_started
            self._count_at_resume        = 0
            self._last_count             = 0
        elif raise_on_error:
            raise ServiceOperationError("Processing failed to start.")
        return ok

    def processing_restart(self, wait=False, raise_on_error=False):
        # NOTE: 'wait' is currently not used

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
            return self._processing_start(raise_on_error)

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
            self._time_at_resume         = time.time()
            self._count_at_resume        = self.on_count()
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

    def update_metadata(self, version, metadata, wait=False):
        if wait:
            return self._update_metadata(version, metadata)
        else:
            thread = threading.Thread(target=self._update_metadata, args=(version, metadata))
            thread.daemon = False  # Program shall not exit while this thread is running
            thread.start()
            # no waiting
            return True  # It is processing the after-effects of a updated metadata

    def _update_metadata(self, version, metadata):
        ok = False
        try:
            ok = self.on_metadata(metadata)
        except Exception as e:
            self.log.exception("Unhandled exception while updating metadata.")
            return False
            # Note that we may now be in a half-state where only some of the new data has been applied.
        self.metadata_version = version
        self.metadata = metadata
        self._metadata_initialized = True
        return ok

    #endregion Processing management commands

    #region Setup methods for override

    def on_configure(self, credentials, config, global_config):
        return True  # For when the service is set up from static config dicts

    def on_setup(self):
        return True  # Create pipeline elements based on static config

    #endregion Setup methods for override

    #region Statistics provider methods for override

    def on_count(self):
        return None
    def on_count_total(self):
        return None

    def on_stats(self, stats):
        """
        :param stats: Dictionary of stats data
        """
        pass

    #endregion Statistics provider methods for override

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
    def is_aborted(self):
        "Evaluate whether processing is aborted."
        return False
    def is_suspended(self):
        "Evaluate whether processing is suspended."
        return False

    def on_processing_start(self):
        return True
    def on_processing_restart(self):
        return True
    def on_processing_stop(self):
        "This method should block until the process is fully stopped."
        return True
    def on_processing_abort(self):
        return True
    def on_processing_suspend(self):
        return True
    def on_processing_resume(self):
        return True
    def on_metadata(self, metadata):
        return True  # No update is an ok update

    #endregion Processing management methods for override

    def get_stats(self):
        proc = psutil.Process()

        stats = {}
        now = time.time()

        # uptime
        uptime = 0
        if self._running:
            uptime = now - self.stat_service_started
        stats["uptime"] = int(uptime)

        # elapsed time; current or from last run:
        elapsed = None
        if self.processing:
            elapsed = (now - self.stat_processing_started)
        elif self.stat_processing_started:
            elapsed = int(self.stat_processing_ended - self.stat_processing_started)
        stats["elapsed"] = elapsed

        # memory used (supposedly in KB, but seems more like bytes..)
        mem = long(proc.memory_info()[0])
        if mem > self.stat_max_memory:
            self.stat_max_memory = mem
        stats["memory"] = mem
        stats["memory_max"] = self.stat_max_memory

        # threads
        threads = threading.active_count()
        if threads > self.stat_max_threads:
            self.stat_max_threads = threads
        stats["threads"] = threads
        stats["threads_max"] = self.stat_max_threads

        # cpu_percent
        # # This collects CPU load in % over 0.1 seconds... but it blocks for that time!
        # stats["cpu_percent"] = proc.cpu_percent(0.1)
        stats["cpu_percent"] = self.stat_cpu_percent
        stats["cpu_percent_max"] = self.stat_max_cpu_percent

        # Only the individual service can provide knowledge for these:

        # estimated time remaining ("eta")
        stats["eta"] = None if self.stat_eta is None else int(self.stat_eta)

        # counts
        stats["count"] = self.stat_count
        stats["count_total"] = self.stat_count_total

        # velocity, dps
        stats["dps"] = self.stat_dps

        # Allow implementations to add their custom stats
        self.on_stats(stats)

        return stats

    def _stat_tick(self):
        "Retrieve counts and ETA plus cpu load."

        now = time.time()
        interval = (now - self._last_stat_tick)

        # cpu
        prc = sum(os.times()[:2])
        if self._last_stat_tick and interval:
            avg_prc = (prc - self._process_cpu_time) / interval
            self.stat_cpu_percent = avg_prc * 100.0
            if self.stat_cpu_percent > self.stat_max_cpu_percent:
                self.stat_max_cpu_percent = self.stat_cpu_percent
        else:
            self.stat_cpu_percent = 0
            self.stat_max_cpu_percent = 0
        self._process_cpu_time = prc

        # Counts
        self.stat_count = self.on_count() or 0
        self.stat_count_total = self.on_count_total()

        # Velocity, dps (since last tick)
        if self.processing and self._last_stat_tick:
            self.stat_dps = (self.stat_count - self._last_count) / interval
        else:
            self.stat_dps = None
        self._last_count = self.stat_count

        # Estimated time remaining (calculated as average since last resume)
        if not self.processing:
            self.stat_eta = None
        else:
            if self.stat_count_total is None:
                self.stat_eta = None  # Infinite, or unknown
            else:
                if (now == self._time_at_resume) or (self.stat_count == self._count_at_resume):
                    self.stat_eta = None
                else:
                    speed = (self.stat_count - self._count_at_resume) / (now - self._time_at_resume)
                    self.stat_eta = max(0.0, (self.stat_count_total - self.stat_count) / speed)

        self._last_stat_tick = now

    #region Metadata helpers

    @staticmethod
    def get_meta_section(metadata, path):
        return esdoc.getfield(metadata, path)

    @staticmethod
    def get_meta_array(metadata, path, subpath, flatten=False):
        items = []
        section = esdoc.getfield(metadata, path, [])
        if section:
            for part in section:
                subpart = esdoc.getfield(part, subpath, [])
                if subpart:
                    if flatten:  # Expect an array to be joined
                        items.extend(subpart)
                    else:
                        items.append(subpart)
        return items

    #endregion Metadata helpers
