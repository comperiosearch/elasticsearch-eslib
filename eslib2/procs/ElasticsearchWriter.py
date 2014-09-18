import elasticsearch
import logging
from Queue import Queue
from threading import Lock
import copy
from ..Generator import Generator
from time import sleep

class Config:
    pass

class ElasticsearchWriter(Generator):

    def __init__(self, name=None):
        super(ElasticsearchWriter, self).__init__(name)
        self.create_connector(self.incoming, "input", "esdoc", "Incoming documents for writing to configured index.")
        self.output = self.create_socket("output", "esdoc", "Modified documents (attempted) written to Elasticsearch.")

        self.config = Config()
        self.config.hosts = None
        self.config.index = None
        self.config.doctype = None
        self.config.update_fields = []
        self.config.batchsize = 1000
        # TODO: SHALL WE USE AN OPTIONAL ALTERNATIVE TIMESTAMP FIELD FOR PROCESSED/INDEXED TIME? (OR NOT?)
        #self.config.timefield = "_timestamp"

        self._queue = Queue()
        self._queue_lock = Lock()

    def incoming(self, document):
        id = document.get("_id")
        index = self.config.index or document.get("_index")
        doctype = self.config.doctype or document.get("_type")

        if not index:
            self.doclog(document, "Missing '_index' field in input and no override.", loglevel=logging.ERROR)
        elif not doctype:
            self.doclog(document, "Missing '_type' field in input and no override.", loglevel=logging.ERROR)
        else:
            # NOTE: This sends the original incoming 'document' as reference to _add()
            #       It only does a shallow copy of the original document and replace the meta data '_index' and '_type'
            #       if there are subscribers!

            fields = document.get("_source")

            if self.config.update_fields:
                # Use the partial 'update' API
                update_fields = {}
                for key, value in fields.iteritems():
                    if key in self.config.update_fields:
                        update_fields.update({key: value})
                meta = {"_index": index, "_type": doctype, "_id" : id}
                self._add(document, {"update": meta}, {"doc": update_fields})
            else:
                # Use the normal partial API
                meta = {"_index": index, "_type": doctype}
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
        while (self.config.batchsize or len(docs) <= self.config.batchsize) and not self._queue.empty():
            (doc,l1,l2) = self._queue.get() # TODO: Or get_nowait() ?
            self._queue.task_done()
            docs.append(doc)
            payload.append(l1)
            payload.append(l2)
        self._queue_lock.release()

        if not len(payload):
            return # Nothing to do

        es = elasticsearch.Elasticsearch(self.config.hosts if self.config.hosts else None)
        res = es.bulk(payload)
        for i, docop in enumerate(res["items"]):
            resdoc = None
            if   "index"  in docop: resdoc = docop["index"]
            elif "update" in docop: resdoc = docop["update"]
            if doc:
                # NOTE: *HOPE* this gives me what I want...:
                id      = resdoc["_id"]
                version = resdoc["_version"]
                index   = resdoc["_index"]
                doctype = resdoc["_type"]

                print "*** ID : OLD=%s, NEW=%s" % (docs[i].get("_id"), doc["_id"]) # DEBUG

                # Only do the following cloning etc if there are actual subscribers:
                if len(self.output.connections):
                    original_doc = docs[i]
                    doc = copy.copy(original_doc) # Shallow clone
                    doc.update({"_id"     : id     }) # Might have changed, in case of new document created, without id
                    doc.update({"_index"  : index  }) # Might have changed to self.config.index
                    doc.update({"_type"   : doctype}) # Might have changed to self.config.doctype
                    doc.update({"_version": doctype}) # Might have changed, in case of update
                    # Send to socket
                    self.output.send(doc)
            else:
                # TODO: Perhaps send failed documents to another (error) socket
                print "*** NO DOC %d" % i # DEBUG

    #region Generator

    def on_shutdown(self):
        # Send remaining queue to Elasticsearch (still in batches)
        if not self.suspended: # Hm... do we want to complete if suspended, or stop where we are?
            while self._queue.qsize():
                self._send()

    def on_tick(self):
        if not self.config.batchsize or self._queue.qsize() >= self.config.batchsize:
            self._send()

    #endregion Generator
