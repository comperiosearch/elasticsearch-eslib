from . import HttpService, PipelineService
from ..procs import Timer, Transformer
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

        procs = [timer, self._logger]

        # Link them
        self.link(*procs)

        # Register them for debug dumping
        self.register_procs(*procs)

        return True

    #region Service overrides

    def on_metadata(self, metadata):
        print "***METADATA", metadata
        self._variable = self.get_meta_section(metadata, self.VARIABLE_CONFIG_PATH)
        print "VAR=", self._variable
        self.head.restart(start=False)
        return True

    #endregion Service overrides

    def _logfunc(self, doc):
        if self.config.lifespan and time.time() - self.stat_processing_started > self.config.lifespan:
            self.log.status("Life has come to an end; stopping.")
            self.processing_stop()
            return
        self.log.debug("DEBUG message.")
        self.log.warning("Service log entry, variable='%s'" % self._variable)
        self._logger.log.warning("Processor log entry, variable='%s'" % self._variable)
        self._logger.doclog.warning("Document log entry, variable='%s'" % self._variable)
        yield doc
