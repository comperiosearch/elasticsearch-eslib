#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Convert CSV format to ElasticSearch JSON


import csv
import eslib.DocumentProcessor


class CSVReader(eslib.DocumentProcessor):
    "Read CSV formatted text lines and write as Elasticsearch JSON."

    def __init__(self, name):
        super().__init__(name)

        self.index = None
        self.doctype = None
        self.fieldList = None
        self.skipFirstLine = False
        self.delimiter = ","


    def start(self):
        self._firstSkipped = False


    def convert(self, line):
        line = line.strip()
        if not line or line.startswith("#"): return None

        if self.skipFirstLine and not self._firstSkipped:
            self._firstSkipped = True
            return None
        return line


    def process(self, line):

        for csvrow in csv.reader([line], delimiter=self.delimiter): # Although there is at max one in 'line'..

            if not len(self.fieldList) == len(csvrow):
                self.error(exception = Exception("Column count does not match number of fields, row =\n%s" % csvrow))

            doc = {}
            id = None
            for i in range(len(self.fieldList)):
                if not self.fieldList[i]:
                    continue # Skip non-specified fields
                elif self.fieldList[i] == "_id":
                    id = csvrow[i]
                elif self.fieldList[i] == "_type": # Override doctype
                    doctype = csvrow[i]
                else:
                    doc.update({self.fieldList[i]: csvrow[i]})
            # Convert to Elasticsearch json document
            outerDoc = {"_index":self.index, "_type":self.doctype, "_id":id, "_source":doc}
            yield outerDoc
