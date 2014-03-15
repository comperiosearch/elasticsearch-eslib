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

