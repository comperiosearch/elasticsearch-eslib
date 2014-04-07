#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Convert CSV format to ElasticSearch JSON


import sys, os, getopt, json
import csv
import eslib.DocumentProcessor


class CSVReader(eslib.DocumentProcessor):
    "Read CSV formatted text lines and write as Elasticsearch JSON."

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

        self.index = None
        self.doctype = None
        self.fieldList = None
        self.skipFirstLine = False


    def start(self):
        self._firstSkipped = False


    def convert(self, line):
        line = line.strip()
        if not line or line.startswith("#"): return None

        if self.skipFirstLine and not self._firstSkipped:
            self._firstSkipped = True
            return None
        return line


    def process(self, line):

        for csvrow in csv.reader([line]): # Although there is at max one in 'line'..

            if not len(self.fieldList) == len(csvrow):
                self.error(exception = Exception("Column count does not match number of fields, row =\n%s" % csvrow))

            doc = {}
            id = None
            for i in range(len(self.fieldList)):
                if not self.fieldList[i]:
                    continue # Skip non-specified fields
                elif self.fieldList[i] == "_id":
                    id = csvrow[i]
                elif self.fieldList[i] == "_type": # Override doctype
                    doctype = csvrow[i]
                else:
                    doc.update({self.fieldList[i]: csvrow[i]})
            # Convert to Elasticsearch json document
            outerDoc = {"_index":self.index, "_type":self.doctype, "_id":id, "_source":doc}
            yield outerDoc


# ============================================================================
# For running as a script
# ============================================================================

import sys, getopt
from eslib.prog import progname


OUT = sys.stderr


def usage(err = None, rich= False):
    if err:
        print("Argument error: %s" % err, file=OUT)

    p = progname()
    print("Usage:", file=OUT)
    print("  %s -h" % p, file=OUT)
    print("  %s [options] -i <index> -t <type> -f <fieldNames> [fileNames]" % p, file=OUT)

    if rich:
        print(file=OUT)
        print("Options:", file=OUT)
        print("  -s             Skip first line of input. (Commonly a header with column names.)", file=OUT)
        print("  -v             Verbose, display progress.", file=OUT)
        print("  --debug        Display debug info.", file=OUT)
        print(file=OUT)
        print("Field names are separated by commas. File names are normal command line arguments.", file=OUT)
        print("If no file name is given then stdin is used instead.", file=OUT)
        print(file=OUT)
        print("A field called '_id' will be used as the document id instead of a one generated", file=OUT)
        print("by ElasticSearch. A field named '_type' will override the document type specified", file=OUT)
        print("with the -t option.", file=OUT)

    if err:
        sys.exit(-1)
    else:
        sys.exit(0)


def main():

    # Default values
    index = None
    doctype = None
    fieldListStr = None
    fieldList = []
    filenames = []
    verbose = False
    debug = False
    skipFirstLine = False

    # Parse command line input
    if len(sys.argv) == 1: usage()
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], ':i:t:f:svh', ["debug"])
    except:
        usage()
    for (o, a) in optlist:
        if   o == "-h": usage(rich=True)
        elif o == "-f": fieldListStr = a
        elif o == "-t": doctype = a
        elif o == "-i": index = a
        elif o == "-s": skipFirstLine = True
        elif o == "-v": verbose = True
        elif o == "--debug": debug = True
    filenames = args

    if not index: usage("no index specified")
    if not doctype: usage("doc type not specified (specify a dummy if overriden by csv column)")
    if not fieldListStr: usage("field names to map to must be specified")

    fieldList = [x.strip() for x in fieldListStr.split(",")]
    
    # Set up and run this processor
    dp = CSVReader(progname())
    dp.index = index
    dp.doctype = doctype
    dp.fieldList = fieldList
    dp.skipFirstLine = skipFirstLine

    dp.VERBOSE = verbose
    dp.DEBUG   = debug

    dp.run(filenames)


if __name__ == "__main__": main()

