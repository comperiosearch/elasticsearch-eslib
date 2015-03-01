# -*- coding: utf-8 -*-

"""
eslib.esdoc
~~~~~~~~~~

Module containing operations on "Elasticsearch type" documents (really just a dict).
"""


__all__ = ("tojson", "createdoc", "getfield", "putfield")


from datetime import datetime
from .time import date2iso
import json

def _json_serializer_isodate(obj):
    """Default JSON serializer."""
    s = None
    if isinstance(obj, datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
            obj = obj.replace(tzinfo=None)
        s = date2iso(obj)
    return s

def tojson(doc):
    return json.dumps(doc, default=_json_serializer_isodate)


def getfield(doc, fieldpath, default=None):
    "Get value for 'fieldpath' if it exits and is not None, otherwise return the default."
    if doc is None or fieldpath is None:
        return default
    if fieldpath == "":
        return doc
    fp = fieldpath.split(".")
    d = doc
    for f in fp[:-1]:
        if not d or not f in d or not isinstance(d[f], dict):
            return default
        d = d[f]
    if d is None:
        return default
    v = d.get(fp[-1])
    return default if v is None else v


def putfield(doc, fieldpath, value):
    "Add or update 'fieldpath' with 'value'."
    if doc is None or fieldpath is None:
        return
    fp = fieldpath.split(".")
    d = doc
    for i, f in enumerate(fp[:-1]):
        if f in d:
            d = d[f]
            if not isinstance(d, dict):
                raise AttributeError("Node at '%s' is not a dict." % ".".join(fp[:i+1]))
        else:
            dd = {}
            d[f] = dd
            d = dd
    d[fp[-1]] = value  # OBS: This also overwrites a node if this is was a node

def shallowputfield(doc, fieldpath, value):
    "Clone as little as needed of 'doc' and add the field from 'fieldpath'. Returns the new cloned doc"
    if not doc or not fieldpath: return
    fp = fieldpath.split(".")
    doc_clone = doc.copy()  # Shallow clone
    d = doc
    d_clone = doc_clone
    for i, f in enumerate(fp[:-1]):
        if f in d:
            d = d[f]
            if not type(d) is dict:
                raise Exception("Node at '%s' is not a dict." % ".".join(fp[:i+1]))
            d_clone[f] = d.copy()  # Create shallow clone of the next level
            d_clone = d_clone[f]
        else:
            dd = {}  # Create a new node
            d_clone.update({f:dd})
            d_clone = dd
    d_clone[fp[-1]] = value  # OBS: This also overwrites a node if this is was a node

    return doc_clone

def createdoc(source, index=None, doctype=None, id=None):
    doc = {"_source": source}
    if index: doc['_index']  = index
    if type : doc['_type' ]  = doctype
    if id   : doc['_id'   ]  = id
    return doc


