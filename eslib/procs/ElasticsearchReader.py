#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Loop through INDEX and dump item ID and FIELD

import elasticsearch
import eslib, eslib.time


# TODO: REPORT ["_shards"]["successful", "total", "failed"...


class ElasticsearchReader(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

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

        es = elasticsearch.Elasticsearch()
        res = es.search(index=self.index, doc_type=self.doctype, search_type="scan", scroll="1m", size=50, body=body)
        scrollid = res["_scroll_id"]
        nhits = res["hits"]["total"]
        remaining = nhits
        count = 0

        self.dout("Total number of items to fetch: %d" % remaining)

        while remaining > 0:
            res = es.scroll(scroll="2m", scroll_id=scrollid)
            scrollid = res["_scroll_id"]
            hits = res["hits"]["hits"]
            remaining -= len(hits)

            for hit in hits:
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


# ============================================================================
# For running as a script
# ============================================================================

from eslib.prog import progname
import eslib.time
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index", required=True, help="Which index to return documents from")
    parser.add_argument("-t", "--type", help="Which type of document to return")
    parser.add_argument("-l", "--limit", default=0, type=int, help="The maximum number of documents to return. Will by default return all documents")
    parser.add_argument("-f", "--field", help="Return only the specified field")
    parser.add_argument("-s", "--since", help="Returns all documents added after SINCE. Specified in the 'ago' format(1d,3w,1y etc.)")
    parser.add_argument("-b", "--before", help="Returns all documents added after BEFORE. Specified in the 'ago' format(1d,3w,1y etc.)")
    parser.add_argument("--timefield", default="_timestamp", help="The field that contains the relavant date information.Default 'timefield' to slice on is '_timestamp'.")
    parser.add_argument("--filter", help="Format for filter is, by example: 'category:politicians,party:democrats'.")
    parser.add_argument("--format", default="json", help="Available formats for 'format': json (default), id, field (used by default if -f given)")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    # Time validation conversion and checks
    if args.before:
        try:
            before = eslib.time.ago2date(args.before)
        except:
            print("Illegal 'ago' time format to 'before' argument, '%s'" % args.before)
    if args.since:
        try:
            since = eslib.time.ago2date(args.since)
        except:
           print("illegal 'ago' time format to 'since' argument, '%s'" % args.since)
    filters = {}

    # Parse filter string
    if args.filter:
        parts = [{part[0]:part[1]} for part in [filter.split(":") for filter in args.filter.split(",")]]
        for part in parts:
            filters.update(part)

    # Set up and run this processor
    dp = ElasticsearchReader(progname())
    dp.index = args.index
    dp.doctype = args.type
    dp.field = args.field
    dp.limit = args.limit
    dp.filters = filters
    dp.since = args.since
    dp.before = args.before
    dp.timefield = args.timefield
    dp.outputFormat = args.format

    dp.DEBUG = args.debug

    dp.run()


if __name__ == "__main__": main()
