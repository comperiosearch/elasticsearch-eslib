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
from .Configurable import Configurable, Config


__all__ = (
    "TerminalProtocolException",
    "Terminal",
    "TerminalInfo",
    "Connector",
    "Socket",
    "Processor",
    "Generator",
    "Monitor",
    "Configurable",
    "Config",

    "unique"
)

#region Core stuff

def unique(seq, idfun=None):
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result

#endregion


#region Encoding of stdin/stdout

import sys, codecs

# Fix stdin and stdout encoding issues
_encoding_stdin  = sys.stdin.encoding or "UTF-8"
_encoding_stdout = sys.stdout.encoding or _encoding_stdin
#sys.stdin = codecs.getreader(_encoding_stdin)(sys.stdin)
sys.stdout = codecs.getwriter(_encoding_stdout)(sys.stdout)

#endregion Encoding of stdin/stdout

#region Logging stuff

import logging
import logging.config

class _ExtendedLogger(logging.getLoggerClass()):
    def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None):
        rec = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func)

        rec.serviceName = self.serviceName if hasattr(self, 'serviceName') else None
        rec.className = self.className if hasattr(self, 'className') else None
        rec.instanceName = self.instanceName if hasattr(self, 'instanceName') else None

        rec.firstName = name.split(".")[0]
        rec.lastName = name.split(".")[-1]
        rec.names = name.split(".")

        return rec

logging.setLoggerClass(_ExtendedLogger)


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

#region Config stuff

import os, yaml
from . import esdoc

def get_credentials(path=None, service_dir=None, credentials_file=None):
    service_dir = service_dir or os.environ.get("ESLIB_SERVICE_DIR")
    if not service_dir:
        raise ValueError("Neither service_dir given nor ESLIB_SERVICE_DIR set.")
    dir = os.path.join(service_dir, "config")

    file_path = None
    if not credentials_file:
        credentials_file = "credentials.yaml"

    if os.path.basename(credentials_file) == credentials_file:
        # Pick from dir
        file_path = os.path.join(dir, credentials_file)
    else:
        # Use absolute path
        file_path = os.path.expanduser(credentials_file)

    # Load credentials file
    with open(file_path, "r") as f:
        credentials = yaml.load(f)

    if not path:
        return credentials
    else:
        return esdoc.getfield(credentials, path)

#endregion
