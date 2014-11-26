# -*- coding: utf-8 -*-

"""
eslib.service
~~~~~

Base classes for wrapping document processing processors into processing graphs/pipelines and control them.
"""


from .Controller            import Controller
from .HttpController        import HttpController
from .PipelineController    import PipelineController

__all__ = (
    "Controller",
    "HttpController",
    "PipelineController"
)
