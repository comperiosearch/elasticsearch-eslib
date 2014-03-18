#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Remove links from text in tweets.


import eslib.DocumentProcessor


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
        linkinfos = eslib.getfield(doc["_source"], "link")
        if not linkinfos: return doc

        links = sorted([(l.get("start"), l.get("end")) for l in linkinfos])
        ss = []
        ss.append(text[:links[0][0]])
        for i in range(1, len(links)):
            ss.append(text[links[i-1][1]:links[i][0]])
        ss.append(text[links[-1][1]:])
        
        cleaned = "".join(ss)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug",   action="store_true")
    parser.add_argument("-t", "--target",  required=False, \
        help="Write cleaned text to this field instead of overwriting input field.")
    parser.add_argument("-f", "--field",   default="text", \
        help="Field to clean. Defaults to 'text'.")

    args = parser.parse_args()

    # Set up and run this processor
    dp = TweetRemoveLinks(progname())
    dp.field   = args.field
    dp.target  = args.target

    dp.VERBOSE = args.verbose
    dp.DEBUG   = args.debug

    dp.run()


if __name__ == "__main__": main()

