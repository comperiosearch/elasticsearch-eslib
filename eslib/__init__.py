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
