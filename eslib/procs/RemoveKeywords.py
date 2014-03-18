#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Find sentiment/mood in documents


import re, json, json
from jsonpath_rw import jsonpath, parse
import eslib.DocumentProcessor


class RemoveKeywords(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)
        self.target = ""
        self.fieldPath = ""
        self.keywordField = ""
        self.pattern = ""

    def configure(self, config=None):
        pass


    def load(self):
        pass

    def process(self, doc):
        path = parse(self.fieldPath)
        fields  = [match.value for match in path.find(doc)]
        remove = []
        if self.pattern:
            remove.append(self.pattern)
        if self.keywordField:
            path = parse(self.keywordField)
            remove += [match.value for match in path.find(doc)]

        remove = "|".join(remove)
        if not remove:
            return doc
        remove_regex = re.compile(r"(%s)" % remove)
        cleaned = ""
        for field in fields:
            cleaned += remove_regex.sub("", field)

        # Add sentiment to the document
        doc['_source'].update({self.target : cleaned})
        if self.DEBUG:
            id      = doc.get("_id")
            index   = doc.get("_index")
            doctype = doc.get("_type")
            num_removed = 0
            for field in fields:
                num_removed += len(remove_regex.findall(field))
            self.dout("/%s/%s/%s: matches for '%s': %d" % (index, doctype, id, remove, num_removed))
        return doc # This must be returned, otherwise the doc is considered to be dumped

# ============================================================================
# For running as a script
# ============================================================================

import sys, argparse
from eslib.prog import progname


OUT = sys.stderr

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug",   action="store_true")
    parser.add_argument("-t", "--target",  required=True, help="The field to write the cleaned text to.")
    parser.add_argument("-f", "--field",   required=True, help="The path to field to clean. Paths are assumed to start in _source. The field is not modified.")
    parser.add_argument("-p", "--pattern",   help="A regex pattern that should be removed from the FIELD.")
    parser.add_argument("-k", "--keywordField",   help="The path to the field(s) with the text to remove.")
    parser.epilog = "Paths are assumed to start from _source. Specified using JSONPath( https://github.com/kennknowles/python-jsonpath-rw)"
    args = parser.parse_args()
    if not (args.pattern or args.keywordField):
        raise TypeError('You must specify either a keyword field (-k) or a pattern (-p)')
    # Set up and run this processor
    dp = RemoveKeywords(progname())
    dp.target = args.target
    dp.fieldPath = "_source." + args.field
    if args.keywordField:
        dp.keywordField = "_source." + args.keywordField
    dp.pattern = args.pattern

    dp.VERBOSE = args.verbose
    dp.DEBUG = args.debug

    dp.run()
if __name__ == "__main__": main()

