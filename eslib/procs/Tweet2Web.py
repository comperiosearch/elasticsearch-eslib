#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Get web pages from links in tweets and create documents from them


import eslib, eslib.debug, eslib.web


class Tweet2Web(eslib.DocumentProcessor):

    ALLOWED_CONTENT_TYPES = ["text/html", "text/plain"]
    ALLOWED_SIZE = 1024*1024 # > 1 MB


    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

        self.index = None
        self.doctype = "webpage"
        self.link_prefix_file = None
        self.link_prefixes  = []
        self.web_getter = eslib.web.WebGetter(self.ALLOWED_SIZE, self.ALLOWED_CONTENT_TYPES)


    def configure(self, config=None):
        self.link_prefixes = [x.lower() for x in self.link_prefixes]


    def load(self):
        if self.link_prefix_file:
            if self.VERBOSE: self.vout("Loading link prefix file: %s" % self.link_prefix_file)
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
        for link in links:
            url = link.get("expand_url")

            # Check if we want to follow this link
            if not self._follow_link(url): continue

            try:
                webdoc = self.web_getter.get(url, self.index, self.doctype)
                if not webdoc: continue
                if self.DEBUG:
                    # TODO: Log this to console instead
                    self.dout("Created doc with content size=%-8s as %s/%s/%s" % \
                        (eslib.debug.byteSizeString(len(webdoc["_source"]["content"]), 1), self.index, self.doctype, webdoc.get("_id")))
            except IOError as e:
                self.eout(e.args[0]) # TODO: Log to doclog with WARNING  (WARNING, index/type/id, msg)
                continue
            except ValueError as e:
                self.dout(e.args[0]) # TODO: Log to doclog with DEBUG (DEBUG, index/type/id, msg)
                continue

            yield webdoc


# ============================================================================
# For running as a script
# ============================================================================

import argparse
from eslib.prog import progname


def main():
    help_i = "Index for the created web page document. Defaults to None, expecting to be set later."
    help_t = "Type of created web page document. Defaults to 'webpage'."
    help_l = "File containing URL prefixes to fetch documents from. Merged with link arguments, if any."
    epilog = """If there are no link prefixes specified either as direct argument or through
        a link prefix file then all links will be followed."""
    
    parser = argparse.ArgumentParser(
        usage="\n  %(prog)s [-i index] [-t type] [-l linkfile] [links ...]",
        epilog=epilog)
    parser._actions[0].help = argparse.SUPPRESS
    parser.add_argument("-i", "--index" , help=help_i, required=False , metavar="index", default=None)
    parser.add_argument("-t", "--type"  , help=help_t, required=False , metavar="type" , default="webpage")
    parser.add_argument("-l", "--links" , help=help_l, required=False , metavar="linkfile", default=[], dest="linkfile")
    parser.add_argument(      "--debug"   , help="Display debug info." , action="store_true")
    parser.add_argument("links", nargs="*", help="URL prefixes to fetch documents from.")

    args = parser.parse_args()

    # Set up and run this processor
    dp = Tweet2Web(progname())
    dp.index = args.index
    dp.doctype = args.type
    dp.link_prefix_file = args.linkfile
    dp.link_prefixes  = args.links

    dp.DEBUG = args.debug

    dp.run()


if __name__ == "__main__": main()
