#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Feed documents to Eleasticsearch


import elasticsearch, json, queue
import eslib.DocumentProcessor


class ElasticsearchWriter(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

        self.index = None
        self.doctype = None
        self.updateFieldList = []
        self.readOnly = False
        self.batchsize = 1000

        self._queue = queue.Queue()


    def configure(self, config=None):
        pass # TODO: Throw exception if mandatory attributes are not configured


    def _add(self, doc, part1, part2):
        self._queue.put((doc, part1, part2))
        if not self.batchsize or self._queue.qsize() >= self.batchsize:
            self._send()

    def _send(self):
        docs = []
        payload = []
        while not self._queue.empty():
            (doc,l1,l2) = self._queue.get()
            docs.append(doc)
            payload.append(l1)
            payload.append(l2)
        es = elasticsearch.Elasticsearch()
        res = es.bulk(payload)
        for i, docop in enumerate(res["items"]):
            doc = None
            if   "index"  in docop: doc = docop["index"]
            elif "update" in docop: doc = docop["update"]
            if doc:
                if self.DEBUG:
                    self.dout("ID : OLD=%s, NEW=%s" % (docs[i].get("_id"), doc["_id"]))
                    #self.dout("VER: OLD=%s, NEW=%s" % (docs[i].get("_version"), doc["_version"]))
                docs[i].update({"_id"     : doc["_id"]})
                docs[i].update({"_version": doc["_version"]})

#        import sys
#        self.dout(json.dumps(res))
#        sys.exit(0)
         
         # TODO: Now we can report the documents in 'doc' as presisted using a callback mechanism


    def process(self, doc):

        # Note: 'updateFieldList' contains the list of fields for a partial update.
        # A full index request is submitted unless fields are given in 'updateFieldList'.

#        es = None
#        if not self.readOnly:
#            es = elasticsearch.Elasticsearch()

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

        doc.update({"_index"  : index  }) # Might have changed to self.index
        doc.update({"_type"   : doctype}) # Might have changed to self.doctype

        if self.updateFieldList: # This means to use the partial 'update' API
            updateFields = {}
            for f in fields:
                if f in self.updateFieldList:
                    updateFields.update({f: fields[f]})
            debugFields = updateFields
            if not self.readOnly:
                meta = {"_index": index, "_type": doctype, "_id" : id}
                self._add(doc, {"update": meta}, {"doc": updateFields})
#                try:
#                    res = es.update(index=index, doc_type=doctype, id=id, body={"doc": updateFields})
#                except Exception as e:
#                    self.eout("Elasticsearch update operation failed for id '%s'." % id, exception=e)
#                    return None
        else:
            debugFields = fields
            if not self.readOnly:
                meta = {"_index": index, "_type": doctype}
                if id: meta.update({"_id": id})
                self._add(doc, {"index": meta}, fields)
#                try:
#                    res = es.index(index=index, doc_type=doctype, id=id, body=fields)
#                except Exception as e:
#                    self.eout(exception=e)
#                    return None

#        if self.DEBUG: self.dout("/%s/%s/%s:" % (index,doctype,(id or "")) + json.dumps(debugFields, ensure_ascii=False))
#        if self.DEBUG and self.VERBOSE and res: self.dout_raw(res)

        # Note: The following line is a performance optimization. The pipeline runner or self.write()
        # will not write output regardless, if self.terminal.
        if self.terminal: return None # Do not write the new document to output

#        if not self.readOnly:
#            doc.update({"_id"     : res["_id"]})
#            doc.update({"_version": res["_version"]})
        return doc


    def finish(self):
        # Send remaining queue to Elasticsearch
        if not self.readOnly:
            if not self.batchsize or self._queue.qsize() > 0:
                self._send()


# ============================================================================
# For running as a script
# ============================================================================

from eslib.prog import progname
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index",     help="Feed to this index instead of the original in '_index'")
    parser.add_argument("-t", "--type",      help="Feed as this document type instead of the original in '_type'")
    parser.add_argument("-r", "--readonly",  help="Read only and dump output to stdout")
    parser.add_argument("-f", "--fieldList", help="Write only these fields, using partial update instead full docs")
    parser.add_argument("--batchsize", type=int, default=1000, help="Batch size for bulk shipments to Elasticsearch")
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
    dp.readOnly = args.readonly
    dp.batchsize = args.batchsize

    dp.terminal = args.terminal

    dp.VERBOSE = args.verbose
    dp.DEBUG = args.debug

    dp.run(args.filenames)


if __name__ == "__main__": main()

