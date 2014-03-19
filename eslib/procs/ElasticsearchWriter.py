#!/usr/bin/python3
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

        if self.DEBUG: self.dout("/%s/%s/%s:" % (index,doctype,(id or "")) + json.dumps(debugFields, ensure_ascii=False))
        if self.DEBUG and self.VERBOSE and res: self.dout_raw(res)

        # Note: The following line is a performance optimization. The pipeline runner or self.write()
        # will not write output regardless, if self.terminal.
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

from eslib.prog import progname
import argparse
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index",     help="Feed to this index instead of the original in '_index'")
    parser.add_argument("-t", "--type",      help="Feed as this document type instead of the original in '_type'")
    parser.add_argument("-r", "--read",      help="Read only and dump output to stdout")
    parser.add_argument("-f", "--fieldList", help="Write only these fields, using partial update instead full docs")
    parser.add_argument("--terminal",        help="Do not write output", action="store_true")
    parser.add_argument("--debug",           help="Display debug info", action="store_true")
    parser.add_argument("--verbose",         help="Verbose, display progress", action="store_true")
    parser.add_argument("filenames",         help="If no input files are specified stdin will be used as input", nargs="*")

    args = parser.parse_args()
    fieldList = []
    if args.fieldList:
        fieldList = [field.strip() for field in args.fieldList.split(",")]
    
    # Set up and run this processor
    dp = ElasticsearchWriter(progname())
    dp.index = args.index
    dp.doctype = args.type
    dp.updateFieldList = fieldList
    dp.readOnly = args.read
    dp.terminal = args.terminal

    dp.VERBOSE = args.verbose
    dp.DEBUG = args.debug

    dp.run(args.filenames)


if __name__ == "__main__": main()

