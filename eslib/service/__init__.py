# -*- coding: utf-8 -*-

"""
eslib.service
~~~~~

Base classes for wrapping document processing processors into processing graphs/pipelines and control them.
"""

from .. import esdoc

def get_first_meta_item(config, path):
    "Returns the first encountered config item value."
    config = config["metadata"]  # BLAH!!!
    for focus in config:
        section = esdoc.getfield(focus, path, [])
        if section:
            return section
    return None

def get_meta_items(config, path):
    "Returns a list of all encountered config values for this path."
    config = config["metadata"]  # BLAH!!!
    items = []
    for focus in config:
        section = esdoc.getfield(focus, path, [])
        if section:
            items.append(section)
    return items

def get_meta_items_joined(config, path):
    "Expects values to be list of items to be joined into one common list."
    config = config["metadata"]  # BLAH!!!
    items = []
    for focus in config:
        section = esdoc.getfield(focus, path, [])
        if section:
            items.extend(section)
    return items


from .Service            import Service, status
from .HttpService        import HttpService
from .PipelineService    import PipelineService
from .ServiceManager     import ServiceManager
from .DummyService       import DummyService


__all__ = (
    "Service",
    "HttpService",
    "PipelineService",
    "ServiceManager",
    "DummyService"
)
