__author__ = 'Hans Terje Bakke'

import elasticsearch
from ..Generator import Generator
from ..time import date2iso
from ..esdoc import getfield
from time import sleep

class ElasticsearchReader(Generator):
    """
    Reads data from Elasticsearch.

    Sockets:
        output            (esdoc)   : Documents retrieved from Elasticsearch.

    Config:
        hosts             = None    : List of Elasticsearch hosts to write to.
        index             = None    : Index override. If set, use this index instead of documents' '_index' (if any).
        doctype           = False   : Document type override. If set, use this type instead of documents' '_type' (if any).
        update_fields     = []      : If specified, only this list of fields will be updated in existing documents.
        batchsize         = 1000    : Size of batch to send to Elasticsearch; will queue up until batch is ready to send.
    """

    def __init__(self, **kwargs):
        super(ElasticsearchReader, self).__init__(**kwargs)
        self.output = self.create_socket("output", "esdoc", "Documents retrieved from Elasticsearch.")

        self.config.set_default(
            hosts      = None,
            index      = None,
            doctype    = None,
            limit      = 0,
            filters    = [],
            since      = None,
            before     = None,
            timefield  = "_timestamp",
            size       = 50, # Number of items to retrieve *per shard* per call to Elasticsearch
            scroll_ttl = "10m", # Must be long enough to process one batch of results (and suspend..)
            scan       = True # For efficiency; disable this when sorting
        )

        self._es = None
        self._scroll_id = None

    def _get_es_conn(self):
        return elasticsearch.Elasticsearch(self.config.hosts if self.config.hosts else None)

    def _release_scroll_context(self):
        if self._es and self._scroll_id:
            self._es.clear_scroll(self._scroll_id)
            self._scroll_id = None

    def _get_es_query(self):

        body = {}
        and_parts = []

        # Add filters, if any
        if self.config.filters:
            ff = {}
            for key,value in self.config.filters.iteritems():
                ff.update({key:[value]})
            and_parts.append({"terms": ff})

        # Add time window, if any
        range_part = {}
        if self.config.since:
            iso = date2iso(self.config.since)
            range_part.update({"from": iso})
        if self.config.before:
            iso = date2iso(self.config.before)
            range_part.update({"to": iso})
        if range_part:
            and_parts.append({"range": {self.timefield: range_part}})

        # Create query from parts (if any) or a simple match_all query
        if and_parts:
            qf = self._create_query_filter({"and": and_parts})
            body.update(qf)
        else:
            body.update({"query": {"match_all": {}}})
        body["fields"] = ["_source", "_parent"]

        return body

    #region Extra utility methods

    def get_open_contexts(self):
        "Get number of open contexts. Useful for debugging open scan connections when using this reader."
        es = self._get_es_conn()
        esres = es.nodes.stats(metric="indices", index_metric="search")
        res = {}
        for r in esres["nodes"].itervalues():
            res.update({r["name"]: r["indices"]["search"]["open_contexts"]})
        return res

    #endregion Extra utility methods

    def on_startup(self):
        self.total = 0
        self.count = 0
        self._scroll_id = None

    # Serve this as one big tick yielding documents
    def on_tick(self):

        body = self._get_es_query()

        self._es = self._get_es_conn()
        self._scroll_id = None

        if self.end_tick_reason:
            self._es = None
            return

        self.log.info("Fetching initial scan batch from Elasticsearch.")
        try:
            res = self._es.search(
                index=self.config.index,
                doc_type = self.config.doctype,
                scroll=self.config.scroll_ttl,
                size=self.config.size,
                search_type=("scan" if self.config.scan else None),
                body=body
            )
        except Exception as e:
            self.log.critical("Initial search (scan) failed. Aborting. %s: %s" % (e.__class__.__name__, e))
            self.abort()
            return

        self._scroll_id = res["_scroll_id"]
        remaining = res["hits"]["total"]

        self.total = remaining if not self.config.limit else self.config.limit
        self.count = 0

        # DEBUG:
        #print "Total number of items to fetch: %d" % remaining

        while remaining > 0:
            if self.end_tick_reason:
                self._release_scroll_context()
                self._es = None
                return
            if self.suspended:
                sleep(self.sleep)
            else:
                congested = self.congestion()
                if congested:
                    self.log.debug("Congestion in dependent processor '%s'; sleeping 10 seconds." % congested.name)
                    self.congestion_sleep(10.0)
                else:
                    self.log.trace("Fetching follow-up scan batch from Elasticsearch.")
                    # TODO: What do do if we get an exception here? (It has happened...)
                    res = self._es.scroll(scroll=self.config.scroll_ttl, scroll_id=self._scroll_id)
                    self._scroll_id = res["_scroll_id"]
                    hits = res["hits"]["hits"]
                    remaining -= len(hits)

                    for hit in hits:
                        if self.end_tick_reason:
                            return

                        # Transform the parent weirdness into our format:
                        parent = getfield(hit, "fields._parent")
                        if parent is not None:
                            hit["_parent"] = parent
                        # Get rid of this whole section; we only wanted it for the _parent, and it is weird, anyway:
                        if "fields" in hit:
                            del hit["fields"]

                        self.output.send(hit)
                        self.count += 1
                        if self.config.limit and self.count >= self.config.limit:
                            if (remaining > 0):
                                self._release_scroll_context()
                            self._es = None
                            self.stop()
                            return

        # Since we finished properly, no need to delete this on server
        self._scroll_id = None
        self._es = None

        self.log.info("All documents retrieved from Elasticsearch.")

        # We are done, but this is a generator and will not stop by itself unless explicitly stopped, so:
        self.stop()
