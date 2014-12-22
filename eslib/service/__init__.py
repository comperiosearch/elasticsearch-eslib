# -*- coding: utf-8 -*-

"""
eslib.service
~~~~~

Base classes for wrapping document processing processors into processing graphs/pipelines and control them.
"""


from .Service            import Service
from .HttpService        import HttpService
from .PipelineService    import PipelineService

__all__ = (
    "Service",
    "HttpService",
    "PipelineService"
)
