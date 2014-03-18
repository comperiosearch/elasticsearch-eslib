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

import sys, getopt
from eslib.prog import progname
import eslib.time


OUT = sys.stderr


def usage(err = None, rich= False):
    if err:
        print("Argument error: %s" % err, file=OUT)

    p = progname()
    print("Usage:", file=OUT)
    print("  %s -h                                                 More help" % p, file=OUT)
    print("  %s -i <index> [-t <type>] [-l limit>] [-f <field>]    Show feed info" % p, file=OUT)

    if rich:
        print(file=OUT)
        print("  Additional options:", file=OUT)
        print("    --since <ago>", file=OUT)
        print("    --before <ago>", file=OUT)
        print("    --timefield <field>", file=OUT)
        print("    --filter <filters>", file=OUT)
        print("    --format <format>", file=OUT)
        print("    --debug", file=OUT)
        print(file=OUT)
        print("  'ago' format is '1d', '2w', etc. Default 'timefield' to slice on is '_timestamp'.", file=OUT)
        print("  Format for filter is, by example: 'category:politicians,party:democrats'.", file=OUT)
        print(file=OUT)
        print("  Available formats for 'format':", file=OUT)
        print("    json (default), id, field (used by default if -f given)", file=OUT)

    if err:
        sys.exit(-1)
    else:
        sys.exit(0)


def main():

    # Default values
    field = None
    index = None
    doctype = None
    limit = 0
    filter = None
    sinceStr = None
    beforeStr = None
    since = None
    before = None
    timefield = None
    filters = {}
    filterStr = None
    outputFormat = "json"
    debug = False

    # Parse command line input
    if len(sys.argv) == 1: usage()
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], ':l:t:i:f:h', \
            ["filter=","before=","since=","timefield=","format=","debug"])
    except:
        usage()
    for (o, a) in optlist:
        if   o == "-h": usage(rich=True)
        elif o == "-f": field = a
        elif o == "-l": limit = int(a)
        elif o == "-t": doctype = a
        elif o == "-i": index = a
        elif o == "--filter": filterStr = a
        elif o == "--before": beforeStr = a
        elif o == "--since": sinceStr = a
        elif o == "--timefield": timefield = a
        elif o == "--format": outputFormat = a
        elif o == "--debug": debug = a
    if len(args) > 0: usage("unknown option '%s'" % args[0])

    if not index: usage("no index specified")

    # Time validation conversion and checks
    if beforeStr:
        try:
            before = eslib.time.ago2date(beforeStr)
        except:
            usage("illegal 'ago' time format to 'before' argument, '%s'" % beforeStr)
    if sinceStr:
        try:
            since = eslib.time.ago2date(sinceStr)
        except:
            usage("illegal 'ago' time format to 'since' argument, '%s'" % sinceStr)

    # Parse filter string
    if filterStr:
        parts = [{y[0]:y[1]} for y in [x.split(":") for x in filterStr.split(",")]]
        for f in parts:
            filters.update(f)

    # Set up and run this processor
    dp = ElasticsearchReader(progname())
    dp.index = index
    dp.doctype = doctype
    dp.field = field
    dp.limit = limit
    dp.filters = filters
    dp.since = since
    dp.before = before
    dp.timefield = timefield
    dp.outputFormat = outputFormat

    dp.DEBUG = debug

    dp.run()


if __name__ == "__main__": main()

