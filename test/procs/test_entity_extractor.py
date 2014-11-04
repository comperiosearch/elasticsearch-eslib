__author__ = 'Eivind Eidheim Elseth'

import unittest, json
from eslib.procs.EntityExtractor import EntityExtractor

class TestEntityExtractor(unittest.TestCase):
    entities = \
        [
            {
                "category": "webpage",
                "name": "nrk.no",
                "match": [
                    { "type": "exact", "value": "nrk" },
                    #{ "type": "iprange", "value": "160.68.205.231/16" }
                ]
            },
            {
                "category": "agent",
                "name": "18036928545",
                "match": [
                    { "type": "exact", "value": "hans terje bakke" },
                ]
            }
        ]
    entity = {
                "category": "webpage",
                "name": "comperio",
                "match": [
                    { "type": "exact", "value": "comperio" },
                    #{ "type": "iprange", "value": "81.27.32.186/16" }
                ]
            }


    def setUp(self):
        self.extractor = EntityExtractor()
        self.extractor.on_open()



    def test_defaults(self):
        self.assertEqual(self.extractor.config.source_field, "text")
        self.assertEqual(self.extractor.config.target_field, "entities")
        self.assertEqual(self.extractor.config.entities, [])
        pass

    def test_add_entities(self):
        self.assertEqual(self.extractor._entities, [])
        self.extractor.add_entity(self.entity)
        expected = [self.entity]
        actual = self.extractor._entities
        self.assertEqual(expected, actual)

        self.extractor.add_entities(self.entities)

        expected.extend(self.entities)
        actual = self.extractor._entities
        self.assertEqual(expected, actual)

    def test_extract_exact(self):
        self.extractor.add_entity(self.entity)
        comperio = self.entity
        match_comperio = comperio["match"][0]
        input = "We all love " + match_comperio["value"]
        expected = {}
        expected[comperio["category"]] = [self._create_result_object(comperio, match_comperio)]
        actual = self.extractor._extract_exact(input)
        self.assertEqual(expected, actual)

        nrk = self.entities[0]
        self.extractor.add_entity(nrk)
        match_nrk = nrk["match"][0]
        expected[nrk["category"]].append(self._create_result_object(nrk,match_nrk))
        input = "we at" + match_comperio["value"] + " love watching " + match_nrk["value"]
        actual = self.extractor._extract_exact(input)
        self.assertEqual(expected, actual)

        hans_terje = self.entities[1]
        self.extractor.add_entity(hans_terje)
        match_hans_terje = hans_terje["match"][0]
        expected[hans_terje["category"]] = [self._create_result_object(hans_terje,match_hans_terje)]
        input =  match_hans_terje["value"] + "works at " + match_comperio["value"] + "and  loves watching " + match_nrk["value"]
        actual = self.extractor._extract_exact(input)
        self.assertEqual(expected, actual)


    def _create_result_object(self, entity, match):
        return {
                entity["name"] : {
                    match["type"]: [
                        match["value"]
                    ]
                }
            }

