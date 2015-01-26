from . import HttpService, PipelineService
from ..procs import Timer, Transformer
from . import get_first_meta_item
from .. import esdoc
import time

class DummyService(HttpService, PipelineService):
    """
    Common static config:
        name
        manager_endpoint
        management_endpoint
    
    Static config:
        timer_frequency = 3
        lifespan        = 0     # 0=infinite

    Runtime config:
        dummy.variable
    """

    VARIABLE_CONFIG_PATH = "dummy.variable"

    metadata_keys = [VARIABLE_CONFIG_PATH]

    def __init__(self, **kwargs):
        super(DummyService, self).__init__(**kwargs)

        self.config.set_default(
            timer_frequency       = 3,
            lifespan              = 0
        )

        self._logger = None
        self._variable = "initial"

    def on_configure(self, credentials, config, global_config):
        self.config.set(
            manager_endpoint      = global_config.get("manager_host"),
            management_endpoint   = config.get("management_endpoint"),

            timer_frequency       = config["frequency"],
            lifespan              = config["lifespan"]
        )

    def on_setup(self):
        # Set up procs
        timer = Timer(
            service = self,
            name    = "timer",
            actions = [(self.config.timer_frequency, self.config.timer_frequency, "ping")])
        self._logger = Transformer(
            service = self,
            name    = "logger",
            func    = self._logfunc)
        self._logger.subscribe(timer)

        #  Register them for debug dumping
        self.register_procs(timer, self._logger)

        # Assign head and tail of pipeline
        self.head = timer
        self.tail = self._logger

        self._started = 0

        return True

    #region Controller overrides

    def is_processing(self):
        "Evaluate whether processing is in progress."
        return self.tail.running

    def is_suspended(self):
        "Evaluate whether processing is suspended."
        return self.head.suspended

    def on_processing_start(self):
        self._started = time.time()
        self.head.start()
        return True

    def on_processing_stop(self):
        self.head.stop()
        self.tail.wait()
        return True

    def on_processing_abort(self):
        self.head.abort()
        self.tail.wait()
        return True

    def on_processing_suspend(self):
        self.head.suspend()
        return True

    def on_processing_resume(self):
        self.head.resume()
        return True

    # TODO
    def on_update(self, config):
        self._variable = get_first_meta_item(config, self.VARIABLE_CONFIG_PATH)
        print "VAR=", self._variable
        if not self.head.running:
            self.processing_start()
        else:
            pass  # Note: No restart needed
        return True

    #endregion Controller overrides

    def _logfunc(self, doc):
        if self.config.lifespan and time.time() - self._started > self.config.lifespan:
            self.log.status("Life has come to an end; stopping.")
            self.processing_stop()
            return
        self.log.debug("DEBUG message.")
        self.log.warning("Service log entry, variable='%s'" % self._variable)
        self._logger.log.warning("Processor log entry, variable='%s'" % self._variable)
        self._logger.doclog.warning("Document log entry, variable='%s'" % self._variable)
        yield doc
