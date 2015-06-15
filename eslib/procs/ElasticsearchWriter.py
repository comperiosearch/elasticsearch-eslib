__author__ = 'Hans Terje Bakke'

# TODO: 'config.hosts': WTF... what happens with the indexing result if there are multiple hosts?? Is only the first
# TODO:  one answering really used?

# TODO: Test update_fields with new documents, and see if all fields are created or only those listed.
# TODO: Also verify that only mentioned fields are changed in existing documents.

import elasticsearch
from Queue import Queue
from threading import Lock
import copy, time
from ..Generator import Generator


class ElasticsearchWriter(Generator):
    """
    Write data to Elasticsearch.
    It expects incoming data in Elasticsearch document format.
    Index and document types can be overridden by the config.

    NOTE: If the index/type does not already exist, Elasticsearch will generate a mapping based on the incoming data.
    NOTE: When using a parent/child relationship, parent id must be listed in the document._parent field.
          (This is an eslib syntax, not Elasticsearch (which is a bit weird here).

    Connectors:
        input      (esdoc)   : Incoming documents for writing to configured index.
    Sockets:
        output     (esdoc)   : Modified documents (attempted) written to Elasticsearch.

    Config:
        hosts             = None    : List of Elasticsearch hosts to write to.
        index             = None    : Index override. If set, use this index instead of documents' '_index' (if any).
        doctype           = None    : Document type override. If set, use this type instead of documents' '_type' (if any).
        update_fields     = []      : If specified, only this list of fields will be updated in existing documents.
        batchsize         = 1000    : Size of batch to send to Elasticsearch; will queue up until batch is ready to send.
        batchtime         = 5.0     : Submit an incomplete batch if 'batchtime' seconds have elapsed since last shipment.
    """

    def __init__(self, **kwargs):
        super(ElasticsearchWriter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", "esdoc", "Incoming documents for writing to configured index.")
        self.output = self.create_socket("output", "esdoc", "Modified documents (attempted) written to Elasticsearch.")

        self.config.set_default(
            hosts         = None,
            index         = None,
            doctype       = None,
            update_fields = [],
            batchsize     = 1000,
            batchtime     = 5.0
            # TODO: SHALL WE USE AN OPTIONAL ALTERNATIVE TIMESTAMP FIELD FOR PROCESSED/INDEXED TIME? (OR NOT?)
            #timefield    = "_timestamp"
        )

        self._queue = Queue()
        self._queue_lock = Lock()
        self._last_batch_time = 0

    def is_congested(self):
        if super(ElasticsearchWriter, self).is_congested():
            return True
        if self.config.batchsize:
            if self._queue.qsize() > self.config.batchsize * 10:
                return True
        elif self._queue.qsize() > 10000:
            return True
        return False

    def _incoming(self, document):
        id = document.get("_id")
        index = self.config.index or document.get("_index")
        doctype = self.config.doctype or document.get("_type")

        if not index:
            self.doclog.error("Missing '_index' field in input and no override.")
        elif not doctype:
            self.doclog.error("Missing '_type' field in input and no override.")
        else:
            # NOTE: This sends the original incoming 'document' as reference to _add()
            #       It only does a shallow copy of the original document and replace the meta data '_index' and '_type'
            #       if there are subscribers!

            fields = document.get("_source")

            meta = {"_index": index, "_type": doctype}
            parent = document.get("_parent")
            if parent:
                meta["_parent"] = parent

            if self.config.update_fields:
                # Use the partial 'update' API
                update_fields = {}
                for key, value in fields.iteritems():
                    if key in self.config.update_fields:
                        update_fields.update({key: value})
                meta["_id"] = id
                self._add(document, {"update": meta}, {"doc": update_fields})
            else:
                # Use the normal partial API
                if id: meta.update({"_id": id})
                self._add(document, {"index": meta}, fields)

    def _add(self, doc, part1, part2):
        self._queue_lock.acquire()
        self._queue.put((doc, part1, part2))
        self._queue_lock.release()

    def _send(self):

        # Create a batch
        self._queue_lock.acquire()
        docs = []
        payload = []
        while (self.config.batchsize and len(docs) <= self.config.batchsize) and not self._queue.empty():
            (doc,l1,l2) = self._queue.get() # TODO: Or get_nowait() ?
            self._queue.task_done()
            docs.append(doc)
            payload.append(l1)
            payload.append(l2)
        self._queue_lock.release()

        if not len(payload):
            self._last_batch_time = time.time()
            return # Nothing to do

        self.log.trace("Sending batch to Elasticsearch.")

        es = elasticsearch.Elasticsearch(self.config.hosts if self.config.hosts else None)
        res = es.bulk(payload)

        self.log.trace("Processing batch result.")

        for i, docop in enumerate(res["items"]):
            if   "index"  in docop: resdoc = docop["index"]
            if   "create" in docop: resdoc = docop["create"]
            elif "update" in docop: resdoc = docop["update"]
            if resdoc:
                self.count += 1

                id      = resdoc["_id"]
                version = resdoc.get("_version")
                index   = resdoc["_index"]
                doctype = resdoc["_type"]

                #print "*** ID : OLD=%s, NEW=%s" % (docs[i].get("_id"), id) # DEBUG

                # Only do the following cloning etc if there are actual subscribers:
                if self.output.has_output:
                    original_doc = docs[i]
                    doc = copy.copy(original_doc) # Shallow clone
                    doc.update({"_id"     : id     }) # Might have changed, in case of new document created, without id
                    doc.update({"_index"  : index  }) # Might have changed to self.config.index
                    doc.update({"_type"   : doctype}) # Might have changed to self.config.doctype
                    doc.update({"_version": version}) # Might have changed, in case of update
                    # Send to socket
                    self.output.send(doc)
            else:
                # TODO: Perhaps send failed documents to another (error) socket(?)
                self.doclog.debug("No document %d" % i)

        self._last_batch_time = time.time()

    #region Generator

    def on_start(self):
        self.count = 0
        self._last_batch_time = time.time()  # Not 0, in that case we would attempt a zero batch immediately upon start

    def on_shutdown(self):
        # Send remaining queue to Elasticsearch (still in batches)
        self.log.info("Submitting all remaining batches.")
        while self._queue.qsize():
            self._send()

    def on_tick(self):
        if self._queue.qsize() and not self.config.batchsize and not self.config.batchtime:
            self.log.trace("Submitting single document.")
            self._send()
        elif self.config.batchsize and (self._queue.qsize() >= self.config.batchsize):
            self.log.debug("Submitting full batch (%d)." % self.config.batchsize)
            self._send()
            self.log.trace("Batch submitted.")  # TODO: DEBUG: REMOVE
        elif self.config.batchtime and self._queue.qsize() and (time.time() - self._last_batch_time > self.config.batchtime):
            self.log.trace("Submitting partial batch (%d) due to batch timeout." % self._queue.qsize())
            self._send()

    #endregion Generator

    #region Utility methods

    def flush(self):
        self.log.info("Submitting all (%d) queued documents with 'flush'." % self._queue.qsize())
        while self._queue.qsize():
            self._send()
        self.log.info("Flush completed.")

    #endregion Utility methods
