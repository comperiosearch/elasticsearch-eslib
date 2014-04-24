#!/usr/bin/python3

__author__ = 'eelseth'

import eslib
import json


class FilterDocuments(eslib.DocumentProcessor):

    def __init__(self, name):
        super().__init__(name)

        self.filter_file = None
        self.filter_fields = None
        self.filter = {}


    def configure(self, config=None):
        pass # TODO: Throw exception if mandatory attributes are not configured


    def load(self):
        # Load filter file. Ok to fail with exception here if file is not found
        self.log.info("Loading filter file: %s" % self.filter)
        f = open(self.filter_file)
        self.filter = json.load(f)
        f.close()


    def process(self, doc):
        fields  = doc.get("_source")
        filtered = False
        for field in self.filter_fields:
            text = eslib.getfield(fields, field, "")
            for keyword, blacklist in self.filter.items():
                #self.console.debug("%s: %s" % (keyword, blacklist))
                if keyword in text:
                    for item in blacklist:
                        if item in text:
                            if self.debuglevel >= 0:
                                self.doclog(doc, 'Document with id %s contained both %s and %s and was removed' % (id, keyword, item))
                            filtered = True
                    if filtered: break
            if filtered: break
        if not filtered:
            yield doc # This must be returned, otherwise the doc is considered to be dumped


# ============================================================================
# For running as a script
# ============================================================================

import argparse
from eslib.prog import progname


def main():
    """

    """
    help_f = "A single or comma separated list of fields to filter on."
    help_F = "The path to a JSON filter file with the format {'keyword': ['remove','doc','if','exists']}"
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--field', required=True, help=help_f)
    parser.add_argument('-F', '--filterFile', required=True, help=help_F)
    parser.add_argument(      "--name"         , help="Process name.", default=None)
    parser.add_argument(      "--debug"        , action="store_true")
    parser.add_argument("filenames", nargs="*", help="If not specified stdin will be used instead.")



    args = parser.parse_args()

    dp = FilterDocuments(args.name or progname())
    dp.filter_file = args.filterFile
    dp.filter_fields = args.field.split(',')

    if args.debug: dp.debuglevel = 0

    dp.run(args.filenames)


if __name__ == "__main__": main()
