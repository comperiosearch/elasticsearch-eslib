__author__ = 'Eivind Eidheim Elseth'

import unittest
from eslib.procs.EntityExtractor import EntityExtractor
from eslib import esdoc


class TestEntityExtractor(unittest.TestCase):
    entities = \
    [
        {
            "category": "webpage",
            "name": "nrk",
            "match": [
                { "type": "exact", "pattern": "nrk.no" },
                #{ "type": "iprange", "value": "160.68.205.231/16" }
            ]
        },
        {
            "category": "targets",
            "name": "comperio",
            "match": [
                { "type": "exact", "pattern": "hans terje bakke", "weight": 0.8 },
                { "type": "exact", "pattern": "10.0.0.100", "weight": 0.5 },
                { "type": "exact", "pattern": "comperio" }
            ]
        },
        {
            "category": "targets",
            "name": "IBM",
            "match": [
                { "type": "exact", "pattern": "ibm" }
            ]
        },
        {
            "category": "creditcards",
            "name": "creditcard",  # The name should become the credit card number
            "match": [ { "type": "creditcard" } ]
        },
        {
            "category": "emails",
            "name": "email",  # The email should become the email address
            "match": [ { "type": "email" } ]
        },
    ]

# TODO: TEST CASE INSENSITIVE
# TODO: TEST UNICODE/SPECIAL CHARS
# TODO: TEST NOT OVERWRITING EXISTING ENTITIES

    def test_defaults(self):
        ex = EntityExtractor()
        ex.on_open()

        self.assertEqual(ex.config.fields, [])
        self.assertEqual(ex.config.target, "entities")
        self.assertEqual(ex.config.entities, [])

    def test_extract_str(self):
        ex = EntityExtractor()
        ex.config.entities = self.entities
        ex.on_open()

        s = "As mentioned on nrk.no, Hans Terje Bakke works for Comperio. His PC has IP address 10.0.0.100. " + \
       "He never uses his credit card: 1234.5678.9876.5432. You can contact him on " + \
       "hans.terje.bakke@gmail.com. But balle.klorin@wesenlund.no will not work for IBM."

        extracted = ex._extract(None, s)
        elist = list(extracted)

        for e in elist:
            print e

        self.assertEqual(len(elist), 8)


    def _verify(self, entities):
        webpage = entities["webpage"]
        targets = entities["targets"]
        emails = entities["emails"]
        creditcards = entities["creditcards"]

        print "WEBPAGE:",webpage.keys()
        print "TARGETS:",targets.keys()
        print "EMAILS :",emails.keys()
        print "CREDITC:",creditcards.keys()

        self.assertEqual(['nrk'], webpage.keys())
        self.assertEqual(['comperio', 'IBM'], targets.keys())
        self.assertEqual(['hans.terje.bakke@gmail.com', 'balle.klorin@wesenlund.no'], emails.keys())
        self.assertEqual(['1234.5678.9876.5432'], creditcards.keys())

    def test_merge(self):
        ex = EntityExtractor()
        ex.config.entities = self.entities
        ex.on_open()

        s = "As mentioned on nrk.no, Hans Terje Bakke works for Comperio. His PC has IP address 10.0.0.100. " + \
       "He never uses his credit card: 1234.5678.9876.5432. You can contact him on " + \
       "hans.terje.bakke@gmail.com. But balle.klorin@wesenlund.no will not work for IBM."

        extracted = ex._extract(None, s)
        entities = ex._merge(extracted)

        self._verify(entities)

    def test_doc_through(self):

        ex = EntityExtractor()
        ex.config.entities = self.entities

        doc = {"_id": "123", "_source": {
            "field1": "As mentioned on nrk.no, Hans Terje Bakke works for Comperio.",
            "field2": "He never uses his credit card: 1234.5678.9876.5432.",
            "field3": "You can contact him on hans.terje.bakke@gmail.com.",
            "subsection" : {
                "subfield": "But balle.klorin@wesenlund.no will not work for IBM."
            },
            "entities": { "old" : "stuff" }
        }}

        ex.config.fields = ["field1", "field2", "field3", "subsection.subfield"]

        output = []
        ex.add_callback(lambda doc: output.append(doc))
        ex.start()
        ex.put(doc)
        ex.stop()
        ex.wait()

        #print output[0]

        new_doc = output[0]
        entities = new_doc["_source"]["entities"]

        self._verify(entities)

        # Check that old and new doc are not the same
        self.assertFalse(doc is new_doc)

        # Check that the previous entities still exist in the new document
        old = esdoc.getfield(new_doc, "_source.entities.old")
        self.assertEqual(old, "stuff")

        # Check that the new entities do not exist in the original document
        self.assertTrue(esdoc.getfield(doc, "_source.entities.webpage") is None)
        self.assertTrue(esdoc.getfield(new_doc, "_source.entities.webpage") is not None)
