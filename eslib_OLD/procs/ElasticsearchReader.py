#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Loop through INDEX and dump item ID and FIELD

import elasticsearch
import eslib, eslib.time


# TODO: REPORT ["_shards"]["successful", "total", "failed"...


class ElasticsearchReader(eslib.DocumentProcessor):

    SCROLL_TTL = "10m"

    def __init__(self, name):
        super().__init__(name)

        self.index = None
        self.doctype = None
        self.field = None
        self.limit = 0
        self.filters = []
        self.since = None
        self.before = None
        self.timefield = "_timestamp"
        self.outputFormat = "json"
      

    def configure(self, config=None):
        # TODO: Throw exception if mandatory attributes are not configured
        if type(self.since)  is str: self.since  = eslib.time.ago2date(self.since)
        if type(self.before) is str: self.before = eslib.time.ago2date(self.before)
        if self.field: self.outputFormat = "field"

    def _createQueryFilter(self, filter):
        return {"query":{"filtered":{"filter":filter}}}


    def _getbody(self):
        body = {}
        andParts = []

        if self.filters:
            ff = {}
            for f in self.filters: ff.update({f:[self.filters[f]]}) # Transform values to list of one entry
            andParts.append({"terms": ff})

        rangePart = {}
        if self.since:
            isoSince = eslib.time.date2iso(self.since)
            rangePart.update({"from": isoSince})
        if self.before:
            isoBefore = eslib.time.date2iso(self.before)
            rangePart.update({"to": isoBefore})
        if rangePart:
            andParts.append({"range": {self.timefield: rangePart}})

        if andParts:
            qf = self._createQueryFilter({"and": andParts})
            body.update(self._createQueryFilter({"and": andParts}))
        else:
            body.update({"query": {"match_all": {}}})

        if self.outputFormat == "id" or self.field:
            fields = []
            if self.field: fields.append(self.field)
            body.update({"fields": fields})

        #print >>sys.stderr, json.dumps(body,indent=2)
        return body


    def read(self, filenames):
        # Note: ignore filenames; this fetched from Elasticsearch, not file or stdin

        body = self._getbody()

        es = elasticsearch.Elasticsearch(self.hosts if self.hosts else None)
        res = es.search(index=self.index, doc_type=self.doctype, search_type="scan", scroll=self.SCROLL_TTL, size=50, body=body)
        scrollid = res["_scroll_id"]
        nhits = res["hits"]["total"]
        remaining = nhits
        count = 0

        if self.debuglevel >= 0:
            self.print("Total number of items to fetch: %d" % remaining)

        while remaining > 0:
            if self.report_soft_abort():
                return

            res = es.scroll(scroll=self.SCROLL_TTL, scroll_id=scrollid)
            scrollid = res["_scroll_id"]
            hits = res["hits"]["hits"]
            remaining -= len(hits)

            for hit in hits:
                if self.report_soft_abort():
                    return
                count += 1
                if self.limit and count > self.limit: return
                yield hit


    def write(self, doc):
        if self.terminal: return
        id = doc["_id"]
        t = doc["_type"]
        if self.outputFormat == "json":
            # Dump full ElasticSearch document object (on one line)
            eslib.DocumentProcessor.write(self, doc)
        elif self.field:
            # Dump ID and fields
            fieldvalue = " | ".join(eslib.getfield(doc["fields"], self.field, []))
            if self.doctype:
                eslib.PipelineStage.write(self, "[%-40s] %s" % (id, fieldvalue))
            else:
                eslib.PipelineStage.write(self, "[%-40s] [%-10s] %s" % (id, t, fieldvalue))
        else:
            # Dump pure ID only
            eslib.PipelineStage.write(self, id)
