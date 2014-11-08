# -*- coding: utf-8 -*-

import unittest
from eslib.procs import RssMonitor
from eslib import esdoc
from eslib import prog

import logging
LOG_FORMAT = ('%(name) -8s %(levelname) -10s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

console = logging.StreamHandler()
console.setLevel(logging.TRACE)
console.setFormatter(logging.Formatter("%(firstname) -8s %(lineno) -5d %(levelname) -10s %(message)s"))

proclog = logging.getLogger("proclog")
proclog.setLevel(logging.TRACE)
proclog.addHandler(console)

doclog  = logging.getLogger("doclog")
doclog.setLevel(logging.TRACE)
doclog.addHandler(console)


class TestRSS(unittest.TestCase):

    def test_config(self):
        p = RssMonitor(index="rss_test", simulate=True)
        self.assertTrue(p.config.simulate)
        self.assertEqual(p.config.channel_index, "rss")
        self.assertIsNone(p.config.item_index)
        self.assertEqual(p.config.index, "rss_test")
        self.assertEqual(p._channel_index, "rss_test")
        self.assertEqual(p._item_index, "rss_test")

    def test_create_index(self):
        p = RssMonitor(index="rss_test", simulate=True)
        ok = p.create_index()
        #self.assertTrue(ok)

    def test_delete_index(self):
        p = RssMonitor(index="rss_test", simulate=True)
        ok = p.delete_index()
        #self.assertTrue(ok)

    def test_add_channels(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.add_channels(
            ("digi", "http://feeds.allerinternett.no/articles/digi/rss.rss"),
            ("norsis", "https://norsis.no/feed")
        )
        print "tot =", n
        #self.assertEqual(n, 2);

    def test_list_channels_all(self):
        p = RssMonitor(index="rss_test", simulate=True)
        channels = p.list_channels(["digi"])
        for channel in channels:
            print "--- channel ---"
            print "name    =", channel.get("channel")  # TODO: RENAME TO "name"
            print "count   =", channel.get("count")
            print "updated =", channel.get("updated")
        #self.assertEqual(len(channels), 2)

    def test_list_channels_one(self):
        p = RssMonitor(index="rss_test", simulate=True)
        channels = p.list_channels(["digi"])
        for channel in channels:
            print "--- channel ---"
            print "name    =", channel.get("channel")  # TODO: RENAME TO "name"
            print "count   =", channel.get("count")
            print "updated =", channel.get("updated")
        #self.assertEqual(len(channels), 1)

    def test_delete_channels(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.delete_channels()
        #self.assertEqual(n, 2)

    def test_delete_channels_too_many(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.delete_channels(["bull", "digi", "shit"])
        print "tot =", n
        #self.assertEqual(n, 1)

    def test_delete_items(self):
        pass

    def test_delete_items_before_date(self):
        pass

    def test_delete_channels_cascading(self):
        pass

    def test_delete_channels_cascading(self):
        pass

    def test_list_items(self):
        pass

    def test_list_items_limit(self):
        pass

    def test_list_items_range(self):
        pass

    def test_fetch_items_socket(self):
        pass

    def test_fetch_items_self_write(self):
        pass

    def test_fetch_items_again(self):
        pass

    def test_fetch_items_again_force(self):
        pass

    def test_running_fetch_by_interval(self):
        pass

def main():
    unittest.main()

if __name__ == "__main__":
    main()
