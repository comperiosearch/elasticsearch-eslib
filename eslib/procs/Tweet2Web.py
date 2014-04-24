#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Get web pages from links in tweets and create documents from them


import time
import eslib, eslib.debug, eslib.web


class Tweet2Web(eslib.DocumentProcessor):

    ALLOWED_CONTENT_TYPES = ["text/html", "text/plain"]
    ALLOWED_SIZE = 1024*1024 # > 1 MB


    def __init__(self, name):
        super().__init__(name)

        self.delay = 0
        self.delay_ms = self.delay / 1000.0
        self.index = None
        self.doctype = "webpage"
        self.link_prefix_file = None
        self.link_prefixes  = []
        self.web_getter = eslib.web.WebGetter(self.ALLOWED_SIZE, self.ALLOWED_CONTENT_TYPES)


    def configure(self, config=None):
        self.link_prefixes = [x.lower() for x in self.link_prefixes]
        self.delay_ms = self.delay/1000.0


    def load(self):
        if self.link_prefix_file:
            self.log.info("Loading link prefix file: %s" % self.link_prefix_file)
            ss = set(self.link_prefixes)
            f = open(self.link_prefix_file)
            for line in f:
                line = line.lower().strip()
                if line.startswith("#"): continue
                ss.add(line)
            self.link_prefixes = list(ss)
            f.close()


    def _follow_link(self, url):
        if not url: return False
        if not self.link_prefixes: return True
        url = url.lower()
        for prefix in self.link_prefixes:
            if str(url).startswith(prefix): return True;
        return False


    def process(self, doc):
        links  = eslib.getfield(doc, "_source.link")
        created_at = eslib.time.iso2date(eslib.getfield(doc, "_source.created_at"))

        for link in links:
            url = link.get("expand_url")

            # Check if we want to follow this link
            if not self._follow_link(url): continue

            try:
                webdoc = self.web_getter.get(url, self.index, self.doctype, created_at=created_at)
                if not webdoc: continue
                if self.debuglevel >= 0:
                    self.doclog(doc, "Created doc with content size=%-8s as /%s/%s/%s" % \
                        (eslib.debug.byteSizeString(len(webdoc["_source"]["content"]), 1), self.index, self.doctype, webdoc.get("_id")))
            except IOError as e:
                self.doclog(doc, e.args[0], loglevel=logger.WARNING)
                continue
            except ValueError as e:
                self.doclog(doc, e.args[0], loglevel=logger.ERROR)
                continue

            yield webdoc

            if self.delay_ms:
                time.sleep(self.delay_ms)


# ============================================================================
# For running as a script
# ============================================================================

import argparse
from eslib.prog import progname


def main():
    help_i = "Index for the created web page document. Defaults to None, expecting to be set later."
    help_t = "Type of created web page document. Defaults to 'webpage'."
    help_l = "File containing URL prefixes to fetch documents from. Merged with link arguments, if any."
    help_d = "Number of milliseconds to sleep between each page get."
    epilog = """If there are no link prefixes specified either as direct argument or through
        a link prefix file then all links will be followed."""
    
    parser = argparse.ArgumentParser(
        usage="\n  %(prog)s [-i index] [-t type] [-d delay] [-l linkfile] [links ...]",
        epilog=epilog)
    parser._actions[0].help = argparse.SUPPRESS
    parser.add_argument("-i", "--index" , help=help_i, required=False , metavar="index", default=None)
    parser.add_argument("-t", "--type"  , help=help_t, required=False , metavar="type" , default="webpage")
    parser.add_argument("-l", "--links" , help=help_l, required=False , metavar="linkfile", default=[], dest="linkfile")
    parser.add_argument("-d", "--delay" , help=help_d, type=int, default=0)
    parser.add_argument(      "--debug" , help="Display debug info." , action="store_true")
    parser.add_argument(      "--name"  , help="Process name.", default=None)
    parser.add_argument("links", nargs="*", help="URL prefixes to fetch documents from.")

    args = parser.parse_args()

    # Set up and run this processor
    dp = Tweet2Web(args.name or progname())
    dp.index = args.index
    dp.doctype = args.type
    dp.link_prefix_file = args.linkfile
    dp.link_prefixes  = args.links
    dp.delay = args.delay

    if args.debug: dp.debuglevel = 0

    dp.run()


if __name__ == "__main__": main()
