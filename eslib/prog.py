# -*- coding: utf-8 -*-

"""
eslib.prog
~~~~~~~~~~

Helper functions for running as an executable program.
"""


__all__ = ( "progname", )


import os, sys

def progname():
    return os.path.basename(sys.argv[0])

