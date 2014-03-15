#!/usr/bin/python
# -*- coding: utf-8 -*-

# Feed documents to Eleasticsearch


import elasticsearch, json
import eslib.DocumentProcessor


class ElasticsearchWriter(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

        self.index = None
        self.doctype = None
        self.updateFieldList = []
        self.readOnly = False
        self.terminal = False


    def configure(self, config=None):
        pass # TODO: Throw exception if mandatory attributes are not configured


    def process(self, doc):

        # Note: 'updateFieldList' contains the list of fields for a partial update.
        # A full index request is submitted unless fields are given in 'updateFieldList'.

        es = None
        if not self.readOnly:
            es = elasticsearch.Elasticsearch()

        id = doc.get("_id")
        index = self.index or doc.get("_index")
        doctype = self.doctype or doc.get("_type")
        fields = doc.get("_source")
        #if not id:
        #    self.eout(exception=ValueError("Missing '_id' field in input."))
        #    return None
        if not index:
            self.eout(exception=ValueError("Missing '_index' field in input and no override."))
            return None
        if not doctype:
            self.eout(exception=Exception("Missing '_type' field in input and no override."))
            return None

        debugFields = ""
        res = None

        if self.updateFieldList: # This means to use the partial 'update' API
            updateFields = {}
            for f in fields:
                if f in self.updateFieldList:
                    updateFields.update({f: fields[f]})
            debugFields = updateFields
            if not self.readOnly:
                try:
                    res = es.update(index=index, doc_type=doctype, id=id, body={"doc": updateFields})
                except Exception as e:
                    self.eout("Elasticsearch update operation failed for id '%s'." % id, exception=e)
                    return None
        else:
            debugFields = fields
            if not self.readOnly:
                try:
                    res = es.index(index=index, doc_type=doctype, id=id, body=fields)
                except Exception as e:
                    self.eout(exception=e)
                    return None

        if self.DEBUG: self.dout("/%s/%s/%s:" % (index,doctype,(id or "")) + json.dumps(debugFields))
        if self.DEBUG and self.VERBOSE and res: self.dout_raw(res)

        if self.terminal: return None # Do not write the new document to output

        doc.update({"_index"  : index  }) # Might have changed to self.index
        doc.update({"_type"   : doctype}) # Might have changed to self.doctype
        if not self.readOnly:
            doc.update({"_id"     : res["_id"]})
            doc.update({"_version": res["_version"]})
        return doc


    def finish(self):
        pass # TODO: If sending batches to Elasticsearch, send the remaining documents here


# ============================================================================
# For running as a script
# ============================================================================

import sys, getopt
from eslib.prog import progname


OUT = sys.stderr


def usage(err = None, rich= False):
    if err:
        print >>OUT, "Argument error: %s" % err

    p = progname()
    print >>OUT, "Usage:"
    print >>OUT, "  %s -h" % p
    print >>OUT, "  %s [options] [fileNames]" % p

    if rich:
        print >>OUT
        print >>OUT, "Options:"
        print >>OUT, "  -i <index>     Feed to this index instead of the original in '_index'."
        print >>OUT, "  -t <type>      Feed as this document type instead of the original in '_type'."
        print >>OUT, "  -r             Read only and dump output to stdout."
        print >>OUT, "  -v             Verbose, display progress."
        print >>OUT, "  -f <fieldList> Write only these fields, using partial update instead full docs."
        print >>OUT, "  --terminal     Do not write output."
        print >>OUT, "  --debug        Display debug info."
        print >>OUT
        print >>OUT, "Field names are separated by commas. File names are normal command normal line"
        print >>OUT, "arguments. If no file name is given then stdin is used instead."

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
    readOnly = False
    debug = False
    terminal = False

    # Parse command line input
    #if len(sys.argv) == 1: usage()
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], ':i:t:f:vrh', ["debug", "terminal"])
    except:
        usage()
    for (o, a) in optlist:
        if   o == "-h": usage(rich=True)
        elif o == "-f": fieldListStr = a
        elif o == "-t": doctype = a
        elif o == "-i": index = a
        elif o == "-v": verbose = True
        elif o == "-r": readOnly = True
        elif o == "--debug": debug = True
        elif o == "--terminal": terminal = True
    filenames = args

    #if not index: usage("no index specified")
    #if not doctype: usage("doc type not specified")

    if fieldListStr:
        fieldList = [x.strip() for x in fieldListStr.split(",")]
    
    # Set up and run this processor
    dp = ElasticsearchWriter(progname())
    dp.index = index
    dp.doctype = doctype
    dp.updateFieldList = fieldList
    dp.readOnly = readOnly
    dp.terminal = terminal

    dp.VERBOSE = verbose
    dp.DEBUG = debug

    dp.run(filenames)


if __name__ == "__main__": main()

