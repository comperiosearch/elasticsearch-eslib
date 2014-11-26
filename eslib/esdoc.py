# -*- coding: utf-8 -*-

"""
eslib.text
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
    "Get value for 'fieldpath' if it exits, otherwise return the default."
    if not doc or not fieldpath:
        return default
    fp = fieldpath.split(".")
    d = doc
    for f in fp[:-1]:
        if not d or not f in d or not type(d[f]) is dict:
            return default
        d = d[f]
    if not d:
        return default
    return d.get(fp[-1]) or default


def putfield(doc, fieldpath, value):
    "Add or update 'fieldpath' with 'value'."
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
