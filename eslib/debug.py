# -*- coding: utf-8 -*-

"""
eslib.debug
~~~~~~~~~~~

Module containing functions useful for debugging.
"""


__all__ = ("byteSizeString", "getMemoryUsed")


import resource


def byteSizeString(bytes, decimals=1):
    kB = bytes / 1024.0
    MB = kB / 1024.0
    GB = MB / 1024.0
    s = None
    if   GB > 1.0: s = "%.*f GB" % (decimals, GB)
    elif MB > 1.0: s = "%.*f MB" % (decimals, MB)
    elif kB > 1.0: s = "%.*f kB" % (decimals, kB)
    else: s = "%s B"
    return s


def getMemoryUsed():
    "Get current memory useage by this process."
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

