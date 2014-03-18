#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Find sentiment/mood in documents


import re, json
import eslib.DocumentProcessor


class RemovePattern(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)
        self.target = None
        self.field = None
        self.pattern = None


    def configure(self, config=None):
        if not self.target: self.target = self.field
        self._remove_regex = re.compile(r"(%s)" % self.pattern)


    def process(self, doc):
        text = eslib.getfield(doc["_source"], self.field)
        if not text or not type(text) is str: return doc

        cleaned = self._remove_regex.sub("", text)
        eslib.putfield(doc['_source'], self.target, cleaned)

        if self.DEBUG:
            id      = doc.get("_id")
            index   = doc.get("_index")
            doctype = doc.get("_type")
            num_removed = 0
            for field in fields:
                num_removed += len(remove_regex.findall(field))
            self.dout("/%s/%s/%s: #matches removed: %d" % (index, doctype, id, num_removed))

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
    parser.add_argument("-t", "--target", \
        help="The field to write the cleaned text to. Defaults to overwrite input field.")
    parser.add_argument("-f", "--field",   required=True, \
        help="The path to field to clean. Paths are assumed to start in _source. The field is not modified.")
    parser.add_argument("-p", "--pattern", required=True, \
        help="A regex pattern that should be removed from the field.")

    args = parser.parse_args()

    # Set up and run this processor
    dp = RemovePattern(progname())
    dp.field   = args.field
    dp.target  = args.target
    dp.pattern = args.pattern

    dp.VERBOSE = args.verbose
    dp.DEBUG   = args.debug

    dp.run()


if __name__ == "__main__": main()

