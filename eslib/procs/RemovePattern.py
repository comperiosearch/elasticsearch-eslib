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
        if not text or not type(text) is str: yield doc

        cleaned = self._remove_regex.sub("", text)
        eslib.putfield(doc['_source'], self.target, cleaned)

        if self.DEBUG:
            id      = doc.get("_id")
            index   = doc.get("_index")
            doctype = doc.get("_type")
            num_removed = len(self.remove_regex.findall(self.field))
            self.dout("/%s/%s/%s: #matches removed: %d" % (index, doctype, id, num_removed))

        yield doc # This must be returned, otherwise the doc is considered to be dumped


# ============================================================================
# For running as a script
# ============================================================================

import argparse, sys
from eslib.prog import progname


def main():
    help_t = "The field to write the cleaned text to. Defaults to overwrite input field."
    help_f = "The path to field to clean. Paths are assumed to start in _source. The field is not modified."
    help_p = "A regex pattern that should be removed from the field."
    parser = argparse.ArgumentParser(usage="\n  %(prog)s -f field -p pattern [-t target]")
    parser.add_argument("-f", "--field"  , required=True, help=help_f)
    parser.add_argument("-p", "--pattern", required=True, help=help_p)
    parser.add_argument("-t", "--target" , help=help_t)
    parser.add_argument(      "--debug"  , action="store_true")
    parser.add_argument(      "--name"   , help="Process name.", default=None)

    if len(sys.argv) == 1:
        parser.print_usage()
        sys.exit(0)

    args = parser.parse_args()

    # Set up and run this processor
    dp = RemovePattern(args.name or progname())
    dp.field   = args.field
    dp.target  = args.target
    dp.pattern = args.pattern

    dp.DEBUG   = args.debug

    dp.run()


if __name__ == "__main__": main()

