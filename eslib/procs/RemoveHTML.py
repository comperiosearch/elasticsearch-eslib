#!/usr/bin/python3
# -*- coding: utf-8 -*-


import eslib.DocumentProcessor
import eslib.text


class RemoveHTML(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)
        self.target = None
        self.field = None

    def configure(self, config=None):
        if not self.target: self.target = self.field


    def process(self, doc):
        text = eslib.getfield(doc["_source"], self.field)
        if not text or not type(text) is str: yield doc
        cleaned = eslib.text.remove_html(text)
        eslib.putfield(doc["_source"], self.target, cleaned)

        if self.DEBUG:
            x = "\nORIGINAL=%s\n" % text
            x += "CLEANED =%s\n\n" % cleaned
            self.dout(x)

        yield doc # This must be returned, otherwise the doc is considered to be dumped


# ============================================================================
# For running as a script
# ============================================================================

import sys, argparse
from eslib.prog import progname


def main():
    help_t = "Write cleaned text to this field instead of overwriting input field."

    parser = argparse.ArgumentParser(usage="\n  %(prog)s -f FIELD [-t TARGET]")
    parser.add_argument("-t", "--target",  required=False, help=help_t)
    parser.add_argument("-f", "--field" ,  required=True , help="Field to clean.")
    parser.add_argument(      "--debug" ,  action="store_true")
    parser.add_argument(      "--name"  , help="Process name.", default=None)
    if len(sys.argv) == 1:
        parser.print_usage()
        sys.exit(0)

    args = parser.parse_args()

    # Set up and run this processor
    dp = RemoveHTML(args.name or progname())
    dp.field   = args.field
    dp.target  = args.target

    #dp.VERBOSE = args.verbose
    dp.DEBUG   = args.debug

    dp.run()


if __name__ == "__main__": main()

