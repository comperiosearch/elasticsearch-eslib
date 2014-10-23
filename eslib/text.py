# -*- coding: utf-8 -*-

"""
eslib.text
~~~~~~~~~~

Module containing operations on text strings.
"""


__all__ = ("remove_parts", "remove_html")


import re
from HTMLParser import HTMLParser

import sys

def remove_parts(text, sections):
    """
    Remove sections from text. Sections is a list of tuples with (start,end)
    coordinates to clip from the text string.
    """

    if not sections: return text

    c = sorted(sections)
    s = []
    s.append(text[:c[0][0]])
    for i in range(1, len(c)):
        s.append(text[c[i-1][1]:c[i][0]])
    s.append(text[c[-1][1]:])
    cleaned = "".join(s)
    return cleaned

#region remove_html

class _MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
        self.strict = False
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)


_regex_whitespace = re.compile(r'\s+', re.UNICODE)
_regex_scripts    = re.compile(r"""<script\s*(type=((".*?")|('.*?')))?>.*?</script>""", re.MULTILINE|re.DOTALL|re.UNICODE)
_regex_style      = re.compile(r"""(<style\s*(type=((".*?")|('.*?')))?>.*?</style>)""", re.MULTILINE|re.DOTALL|re.UNICODE)

def remove_html(text):
    if not text or not type(text) in [str, unicode]:
        return text

    text = re.sub(_regex_scripts, " ", text)
    text = re.sub(_regex_style  , " ", text)
    stripper = _MLStripper()
    cleaned = stripper.unescape(text)
    stripper.feed(cleaned)
    cleaned = stripper.get_data()
    cleaned = re.sub(_regex_whitespace, " ", cleaned)
    return cleaned

#endregion remove_html

