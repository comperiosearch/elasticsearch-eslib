# -*- coding: utf-8 -*-
import os

import unittest, json
from eslib.procs import TweetEntityRemover
from eslib import esdoc

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class TestTweetEntityRemoval(unittest.TestCase):

    def test_all(self):

        # Load test data
        self_dir, _ = os.path.split(__file__)
        f = open(os.path.join(self_dir, "data/tweet_entity_removal.json"))
        doc = json.load(f)
        f.close()

        p_none    = TweetEntityRemover(remove_urls=False, remove_mentions=False)
        p_url     = TweetEntityRemover(remove_urls=True , remove_mentions=False)
        p_mention = TweetEntityRemover(remove_urls=False, remove_mentions=True)
        p_both    = TweetEntityRemover(remove_urls=True , remove_mentions=True, target_field="cleaned")

        cleaned_none    = p_none   ._clean(doc)
        cleaned_url     = p_url    ._clean(doc)
        cleaned_mention = p_mention._clean(doc)
        cleaned_both    = p_both   ._clean(doc)

        self.assertTrue(esdoc.getfield(cleaned_none   , "_source.text")    == "Me &amp; the lovely @stellachuuuuu @ Jacob K Javits Convention Center http://t.co/x6BUjNY0jv")
        self.assertTrue(esdoc.getfield(cleaned_url    , "_source.text")    == "Me &amp; the lovely @stellachuuuuu @ Jacob K Javits Convention Center")
        self.assertTrue(esdoc.getfield(cleaned_mention, "_source.text")    == "Me &amp; the lovely @ Jacob K Javits Convention Center http://t.co/x6BUjNY0jv")
        # Original text should be untouched, and cleaned gone to separate field:
        self.assertTrue(esdoc.getfield(cleaned_both   , "_source.text")    == "Me &amp; the lovely @stellachuuuuu @ Jacob K Javits Convention Center http://t.co/x6BUjNY0jv")
        self.assertTrue(esdoc.getfield(cleaned_both   , "_source.cleaned") == "Me &amp; the lovely @ Jacob K Javits Convention Center")

        # Verify that minimal cloning works:
        self.assertFalse(esdoc.getfield(doc, "_source")          == esdoc.getfield(cleaned_url, "_source"         ), "Expected _source old!=new")
        self.assertTrue (esdoc.getfield(doc, "_source.entities") == esdoc.getfield(cleaned_url, "_source.entities"), "Expected _source old==new")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
