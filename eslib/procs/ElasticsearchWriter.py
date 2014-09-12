#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Feed documents to Eleasticsearch


import elasticsearch, queue
import eslib.DocumentProcessor


class ElasticsearchWriter(eslib.DocumentProcessor):

    def __init__(self, name):
        super().__init__(name)

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
        es = elasticsearch.Elasticsearch(self.hosts if self.hosts else None)
        res = es.bulk(payload)
        for i, docop in enumerate(res["items"]):
            doc = None
            if   "index"  in docop: doc = docop["index"]
            elif "update" in docop: doc = docop["update"]
            if doc:
                if self.debuglevel >= 0:
                    self.print(("ID : OLD=%s, NEW=%s" % (docs[i].get("_id"), doc["_id"])))
                docs[i].update({"_id"     : doc["_id"]})
                docs[i].update({"_version": doc["_version"]})

         # TODO: Now we can report the documents in 'doc' as presisted using a callback mechanism


    def process(self, doc):

        # Note: 'updateFieldList' contains the list of fields for a partial update.
        # A full index request is submitted unless fields are given in 'updateFieldList'.

        id = doc.get("_id")
        index = self.index or doc.get("_index")
        doctype = self.doctype or doc.get("_type")
        fields = doc.get("_source")

        if not index:
            self.doclog(doc, "Missing '_index' field in input and no override.", loglevel=logging.ERROR)
        elif not doctype:
            self.doclog(doc, "Missing '_type' field in input and no override.", loglevel=logging.ERROR)
        else:
            doc.update({"_index"  : index  }) # Might have changed to self.index
            doc.update({"_type"   : doctype}) # Might have changed to self.doctype

            if self.updateFieldList: # This means to use the partial 'update' API
                updateFields = {}
                for f in fields:
                    if f in self.updateFieldList:
                        updateFields.update({f: fields[f]})
                if not self.readOnly:
                    meta = {"_index": index, "_type": doctype, "_id" : id}
                    self._add(doc, {"update": meta}, {"doc": updateFields})
            else:
                if not self.readOnly:
                    meta = {"_index": index, "_type": doctype}
                    if id: meta.update({"_id": id})
                    self._add(doc, {"index": meta}, fields)

            if not self.terminal: yield doc


    def finish(self):
        # Send remaining queue to Elasticsearch
        if not self.readOnly:
            if not self.batchsize or self._queue.qsize() > 0:
                self._send()
