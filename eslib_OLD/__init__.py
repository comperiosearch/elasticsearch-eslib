# -*- coding: utf-8 -*-

"""
eslib
~~~~~

Document processing library for Elasticsearch.
"""

__version__ = "0.0.1"
__author__ = "Hans Terje Bakke"

from .Pipeline import Pipeline
from .PipelineStage import PipelineStage
from .DocumentProcessor import DocumentProcessor

#from time import *
#from prog import *

__all__ = (
    "Pipeline",
    "PipelineStage",
    "DocumentProcessor"
)


def getfield(doc, fieldpath, default=None):
    if not doc or not fieldpath: return default
    fp = fieldpath.split(".")
    d = doc
    for f in fp[:-1]:
        if not d or not f in d or not type(d[f]) is dict: return default
        d = d[f]
    if not d: return default
    return d.get(fp[-1])


def putfield(doc, fieldpath, value):
    if not doc or not fieldpath: return
    fp = fieldpath.split(".")
    d = doc
    for i, f in enumerate(fp[:-1]):
        if f in d:
            d = d[f]
            if not type(d) is dict:
                raise Exception("Node at '%s' is not a dict." % ".".join(fp[:i+1]))
        else:
            dd = {}
            d.update({f:dd})
            d = dd
    d.update({fp[-1]: value}) # OBS: This also overwrites a node if this is was a node


def createdoc(source, index=None, doctype=None, id=None):
    doc = {"_source": source}
    if index: doc['_index']  = index
    if type : doc['_type' ]  = doctype
    if id   : doc['_id'   ]  = id
    return doc


#region Logging stuff

import logging
import logging.config

# Add firstname and lastname to log record, where logger name is like "firstname.bla.bla.lastname"
old_factory = logging.getLogRecordFactory()
def new_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.firstname = record.name.split(".")[0]
    record.lastname = record.name.split(".")[-1]
    record.names = record.name.split(".")
    return record
logging.setLogRecordFactory(new_factory)

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
