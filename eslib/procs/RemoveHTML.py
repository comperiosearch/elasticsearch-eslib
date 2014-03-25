#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Remove links from text in tweets.


import eslib.DocumentProcessor
from html.parser import HTMLParser
import re
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
        self.strict = False
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

class RemoveHTML(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)
        self.target = None
        self.field = None
        self.keep_style = False
        self.keep_scripts = False
        self.whitespace = re.compile(r'\s+')
        self.scripts = re.compile(r"""<script\s*(type=((".*?")|('.*?')))?>.*?</script>""", re.MULTILINE|re.DOTALL)
        self.style = re.compile(r"""(<style\s*(type=((".*?")|('.*?')))?>.*?</style>)""", re.MULTILINE|re.DOTALL)
    def configure(self, config=None):
        if not self.target: self.target = self.field


    def process(self, doc):
        stripper = MLStripper()
        text = eslib.getfield(doc["_source"], self.field)
        if not text or not type(text) is str: return doc
        if not self.keep_scripts:
            text = re.sub(self.scripts, " ", text)
        if not self.keep_style:
            text = re.sub(self.style, " ", text)
        cleaned = stripper.unescape(text)
        stripper.feed(cleaned) 
        cleaned = stripper.get_data()
        cleaned = re.sub(self.whitespace, " ", cleaned)
        eslib.putfield(doc["_source"], self.target, cleaned)

        if self.DEBUG:
            x += "ORIGINAL=%s\n" % text
            x += "CLEANED =%s\n\n" % cleaned
            self.dout(x)

        return doc # This must be returned, otherwise the doc is considered to be dumped


# ============================================================================
# For running as a script
# ============================================================================

import argparse
from eslib.prog import progname


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug",   action="store_true")
    parser.add_argument("--keepStyle",   action="store_true", help="Lets CDATA of style elements stay in the document")
    parser.add_argument("--keepScripts",   action="store_true", help="Lets CDATA of script elements stay in the document")
    parser.add_argument("-t", "--target",  required=False, \
        help="Write cleaned text to this field instead of overwriting input field.")
    parser.add_argument("-f", "--field",   default="page", \
        help="Field to clean. Defaults to 'page'.")

    args = parser.parse_args()
    # Set up and run this processor
    dp = RemoveHTML(progname())
    dp.field   = args.field
    dp.target  = args.target
    dp.keep_style = args.keepStyle
    dp.keep_scripts = args.keepScripts

    dp.VERBOSE = args.verbose
    dp.DEBUG   = args.debug

    dp.run()


if __name__ == "__main__": main()

