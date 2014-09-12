#!/usr/bin/python3

# Removes documents from pipeline that match blacklisted word combinations.
# The filter file contains a json dict with keyword and a corresponding list of words that if found,
# should lead to dropping this document.
# Example:
# {"nets": ["brooklyn"]}
# will drop all docs where "nets" is found in the same field together with "brooklyn".

__author__ = 'eelseth'

import eslib
import json


class DocumentFilter(eslib.DocumentProcessor):

    def __init__(self, name):
        super().__init__(name)

        self.filter_file = None
        self.filter_fields = None
        self.filter = {}


    def configure(self, config=None):
        pass # TODO: Throw exception if mandatory attributes are not configured


    def load(self):
        # Load filter file. Ok to fail with exception here if file is not found
        self.log.info("Loading filter file: %s" % self.filter)
        f = open(self.filter_file)
        self.filter = json.load(f)
        f.close()


    def process(self, doc):
        fields  = doc.get("_source")
        filtered = False
        for field in self.filter_fields:
            text = eslib.getfield(fields, field, "")
            for keyword, blacklist in self.filter.items():
                #self.console.debug("%s: %s" % (keyword, blacklist))
                if keyword in text:
                    for item in blacklist:
                        if item in text:
                            if self.debuglevel >= 0:
                                self.doclog(doc, 'Document with id %s contained both %s and %s and was removed' % (id, keyword, item))
                            filtered = True
                    if filtered: break
            if filtered: break
        if not filtered:
            yield doc # This must be returned, otherwise the doc is considered to be dumped
