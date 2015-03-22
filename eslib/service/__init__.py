# -*- coding: utf-8 -*-

"""
eslib.service
~~~~~

Base classes for wrapping document processing processors into processing graphs/pipelines and control them.
"""

from .. import esdoc


from .Service            import Service, status
from .HttpService        import HttpService
from .PipelineService    import PipelineService
from .ServiceManager     import ServiceManager
from .ServiceLauncher    import ServiceLauncher
from .DummyService       import DummyService


__all__ = (
    "Service",
    "HttpService",
    "PipelineService",
    "ServiceManager",
    "ServiceLauncher",
    "DummyService"
)
