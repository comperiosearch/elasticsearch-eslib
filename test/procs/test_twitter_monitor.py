# -*- coding: utf-8 -*-

import unittest, json
from eslib.procs import TwitterMonitor

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class TestTwitterMonitor(unittest.TestCase):

    def test_simple(self):

        # Load test data
        f = open("data/twitter_raw_mock.json")
        j = json.load(f)
        f.close()

        m = TwitterMonitor()
        raw, tweet = m._decode(j)

        # Test tweet
        self.assertTrue(tweet["_id"] == "520149420122578944")
        self.assertTrue(tweet["_source"]["source"] == u"Twitter for BlackBerry®")
        self.assertTrue(tweet["_source"]["text"] == u'These clowns must hope that we never cum under attack from any force-r we capable of protecting ourselves?')
        self.assertTrue(str(tweet["_source"]["created_at"]) == "2014-10-09 09:51:00.328000")
        self.assertTrue("geo" in tweet["_source"])
        self.assertTrue(tweet["_source"]["lang"] == "en")
        self.assertTrue(tweet["_source"]["place"]["country"] == "South Africa")
        self.assertFalse("in_reply_to" in tweet["_source"])
        # User
        self.assertTrue(tweet["_source"]["user"]["id"] == "2196916282")
        self.assertTrue(tweet["_source"]["user"]["lang"] == "en")
        self.assertTrue(tweet["_source"]["user"]["name"] == "mark fester")
        self.assertFalse("description" in tweet["_source"]["user"])
        self.assertTrue(str(tweet["_source"]["user"]["created_at"]) == "2013-11-26 14:21:35+00:00")

        # Entities
        # // TODO

def main():
    unittest.main()

if __name__ == "__main__":
    main()
