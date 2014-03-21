#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Remove links from text in tweets.


import eslib.DocumentProcessor
import eslib.text


class TweetRemoveLinks(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)
        self.target = None
        self.field = None


    def configure(self, config=None):
        if not self.target: self.target = self.field


    def process(self, doc):
        text = eslib.getfield(doc["_source"], self.field)
        if not text or not type(text) is str: return doc
        cleaned = text
        linkinfos = eslib.getfield(doc["_source"], "link", [])
        linkcoords = [(l.get("start"), l.get("end")) for l in linkinfos]
        cleaned = eslib.text.remove_parts(text, linkcoords)
        eslib.putfield(doc["_source"], self.target, cleaned)

        if self.DEBUG:
            x = "\n#LINKS=%d\n" % len(links)
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
    parser = argparse.ArgumentParser(usage="%(prog)s -f FIELD [-t TARGET]")
    parser.add_argument("--debug",   action="store_true")
    parser.add_argument("-t", "--target",  required=False, \
        help="Write cleaned text to this field instead of overwriting input field.")
    parser.add_argument("-f", "--field",   default="text", \
        help="Field to clean. Defaults to 'text'.")

    args = parser.parse_args()

    # Set up and run this processor
    dp = TweetRemoveLinks(progname())
    dp.field   = args.field
    dp.target  = args.target

    dp.DEBUG   = args.debug

    dp.run()


if __name__ == "__main__": main()

