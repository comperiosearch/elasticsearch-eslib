# -*- coding: utf-8 -*-

import unittest
from eslib.procs import RssMonitor
from eslib import esdoc
from eslib import prog
from eslib import debug
from eslib.time import ago2date

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

# NOTE: The following integration tests are expected to be ran manually and in a controlled sequence

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

    def test_add_channels(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.add_channels(
            ("digi", "http://feeds.allerinternett.no/articles/digi/rss.rss"),
            ("norsis", "https://norsis.no/feed"),
            ("vg_sport", "http://www.vg.no/rss/create.php?categories=20")
        )
        print "tot =", n
        #self.assertEqual(n, 2);

    def test_list_channels_all(self):
        p = RssMonitor(index="rss_test", simulate=True)
        channels = p.list_channels()
        for channel in channels:
            print "--- channel ---"
            print "name    =", channel.get("channel")  # TODO: RENAME TO "name"
            print "count   =", channel.get("count")
            print "updated =", channel.get("updated")
        #self.assertEqual(len(channels), 3)

    def test_list_channels_two(self):
        p = RssMonitor(index="rss_test", simulate=True)
        channels = p.list_channels(["digi", "norsis"])
        for channel in channels:
            print "--- channel ---"
            print "name    =", channel.get("channel")  # TODO: RENAME TO "name"
            print "count   =", channel.get("count")
            print "updated =", channel.get("updated")
        #self.assertEqual(len(channels), 1)

#    def test_delete_channels(self):
#        p = RssMonitor(index="rss_test", simulate=True)
#        n = p.delete_channels()
#        #self.assertEqual(n, 2)

    def test_delete_channels_too_many(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.delete_channels(["bull", "vg_sport", "shit"])
        print "tot =", n
        #self.assertEqual(n, 1)

    def fetch_items(self, force=False):
        p = RssMonitor(index="rss_test", include_linked_page=False, simulate=False)
        items = p.fetch_items(force=force, simulate=False)
        count = 0
        for item in items:
            count += 1
            s = item["_source"]
            length = debug.byte_size_string(len(s.get("page") or ""), 2)
            print "%-10s | %10s | %s" % (s["channel"], length, s["title"])
        print "tot =", count
        return count

    def test_fetch_items(self):
        n = self.fetch_items()
        #self.assertEqual(n, 40)

    def test_fetch_items_again(self):
        n = self.fetch_items()
        #self.assertEqual(n, 40)

    def test_fetch_items_again_force(self):
        n = self.fetch_items(force=True)
        #self.assertEqual(n, 40)

    def test_delete_items(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.delete_items(["digi"])
        print "tot =", n
        #self.assertEqual(n, 20)

    def test_delete_items_before_date(self):
        p = RssMonitor(index="rss_test", simulate=True)
        n = p.delete_items(before_date=ago2date("3d"))
        print "tot =", n
        #self.assertLess(n, 40)

    def test_delete_channels_cascading(self):
        pass  # TODO

    def test_list_items(self):
        pass  # TODO

    def test_list_items_limit(self):
        pass  # TODO

    def test_list_items_range(self):
        pass  # TODO

    def test_fetch_items_socket(self):
        pass

    def test_fetch_items_self_write(self):
        pass

    def test_running_fetch_by_interval(self):
        pass

    def test_delete_index(self):
        p = RssMonitor(index="rss_test", simulate=True)
        ok = p.delete_index()
        #self.assertTrue(ok)


def main():
    unittest.main()

if __name__ == "__main__":
    main()
