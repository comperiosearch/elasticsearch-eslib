# -*- coding: utf-8 -*-

"""
eslib.debug
~~~~~~~~~~~

Module containing functions useful for debugging.
"""
import os


__all__ = ("byte_size_string", "get_memory_used")


if os.name == 'posix':
    import resource


def byte_size_string(bytes, decimals=1):
    kB = bytes / 1024.0
    MB = kB / 1024.0
    GB = MB / 1024.0
    s = None
    if   GB > 1.0: s = "%.*f GB" % (decimals, GB)
    elif MB > 1.0: s = "%.*f MB" % (decimals, MB)
    elif kB > 1.0: s = "%.*f kB" % (decimals, kB)
    else: s = "%s B" % bytes
    return s


def get_memory_used():
    """Get current memory usage by this process. Supposedly in KB."""
    if os.name == 'posix':
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    else:
        0  # Don't want to risk an exception here..
        #raise NotImplementedError
