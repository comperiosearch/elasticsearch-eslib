# -*- coding: utf-8 -*-

"""
eslib
~~~~~

Document processing library for Elasticsearch.
"""

__version__ = "0.0.1"
__author__ = "Hans Terje Bakke"


from .Terminal     import TerminalProtocolException, Terminal
from .TerminalInfo import TerminalInfo
from .Connector    import Connector
from .Socket       import Socket
from .Processor    import Processor
from .Generator    import Generator
from .Monitor      import Monitor
#from .esdoc import *


__all__ = (
    "TerminalProtocolException",
    "Terminal",
    "TerminalInfo",
    "Connector",
    "Socket",
    "Processor",
    "Generator",
    "Monitor"
)


#region Logging stuff

import logging
import logging.config

# Python 3 way:
# Add firstname and lastname to log record, where logger name is like "firstname.bla.bla.lastname"
#old_factory = logging.getLogRecordFactory()
#def new_factory(*args, **kwargs):
#    record = old_factory(*args, **kwargs)
#    record.firstname = record.name.split(".")[0]
#    record.lastname = record.name.split(".")[-1]
#    record.names = record.name.split(".")
#    return record
#logging.setLogRecordFactory(new_factory)

# Python 2 way:
class _MyLogRecord(logging.LogRecord):
    def __init__(self, name, level, fn, lno, msg, args, exc_info, func):
        logging.LogRecord.__init__(self, name, level, fn, lno, msg, args, exc_info, func)
        self.firstname = self.name.split(".")[0]
        self.lastname = self.name.split(".")[-1]
        self.names = self.name.split(".")

class _MyLogger(logging.getLoggerClass()):
    def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None):
        return _MyLogRecord(name, level, fn, lno, msg, args, exc_info, func)

logging.setLoggerClass(_MyLogger)


def _log_status(self, message, *args, **kws):
    if self.isEnabledFor(logging.STATUS):
        self._log(logging.STATUS, message, args, **kws)

def _log_verbose(self, message, *args, **kws):
    if self.isEnabledFor(logging.VERBOSE):
        self._log(logging.VERBOSE, message, args, **kws)

def _log_trace(self, message, *args, **kws):
    if self.isEnabledFor(logging.TRACE):
        self._log(logging.TRACE, message, args, **kws)

def _log_debug_n(self, n, message, *args, **kws):
    candidate = logging.DEBUG - n
    loglevel = min(max(candidate, logging.TRACE+1), logging.DEBUG)
    if self.isEnabledFor(loglevel):
        self._log(loglevel, message, args, **kws)

logging.STATUS  = 25
logging.VERBOSE = 15
logging.TRACE   =  1

logging.addLevelName(logging.STATUS , "STATUS")
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.addLevelName(logging.TRACE  , "TRACE")
for n in range(1,9):
    logging.addLevelName(logging.DEBUG -n, "DEBUG-%s" % n)

logging.Logger.status  = _log_status
logging.Logger.verbose = _log_verbose
logging.Logger.trace   = _log_trace
logging.Logger.debugn  = _log_debug_n

#endregion Logging stuff
