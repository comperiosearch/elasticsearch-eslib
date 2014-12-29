# -*- coding: utf-8 -*-

import eslib
import unittest
from elasticsearch.client.indices import IndicesClient
from eslib.procs import RssMonitor, ElasticsearchWriter
from eslib import Processor
from eslib import debug
from eslib.time import ago2date
from time import sleep
from eslib import prog

prog.initlogs()

# NOTE: The following integration tests are expected to be executed in sequence,
#       and there must exist an Elasticsearch server to store the data.
# NOTE: They must be executed in the listed sequence!!
#       And there must be time inbetween for indexing to take place.

elasticsearch_host = "localhost:9200"
elasticsearch_index = "rss_test"

class TestRSS(unittest.TestCase):

    def test_000_delete_index_just_in_case(self):
        # Make this the first step, just in case
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        deleted = p.delete_index()
        print "deleted =", deleted

    def test_001_config(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        self.assertFalse(p.config.simulate)
        self.assertEqual(p.config.channel_index, "rss")
        self.assertIsNone(p.config.item_index)
        self.assertEqual(p.config.index, "rss_test")
        self.assertEqual(p._channel_index, "rss_test")
        self.assertEqual(p._item_index, "rss_test")

    def test_002_create_index(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        ok = p.create_index()
        self.assertTrue(ok)

    def test_003_add_channels(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        n = p.add_channels(
            ("digi", "http://feeds.allerinternett.no/articles/digi/rss.rss"),
            ("norsis", "https://norsis.no/feed"),
            ("vg_sport", "http://www.vg.no/rss/create.php?categories=20")
        )
        print "tot =", n
        self.assertEqual(n, 3);

    def test_004_list_channels_all(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        channels = p.list_channels()
        for channel in channels:
            print "--- channel ---"
            print "name    =", channel.get("name")
            print "count   =", channel.get("count")
            print "updated =", channel.get("updated")
        self.assertEqual(len(channels), 3)

    def test_005_list_channels_two(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        channels = p.list_channels(["digi", "norsis"])
        for channel in channels:
            print "--- channel ---"
            print "name    =", channel.get("name")
            print "count   =", channel.get("count")
            print "updated =", channel.get("updated")
        self.assertEqual(len(channels), 2)

    def test_006_delete_channels_too_many(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        n = p.delete_channels(["bull", "vg_sport", "shit"])
        print "tot =", n
        self.assertEqual(n, 1)

    def fetch_items(self, force=False):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index,
                       include_linked_page=False)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        items = p.fetch_items(force=force, simulate=False)
        count = 0
        for item in items:
            count += 1
            s = item["_source"]
            length = debug.byte_size_string(len(s.get("page") or ""), 2)
            print "%-10s | %10s | %s" % (s["channel"], length, s["title"])
        print "tot =", count
        return count

    def test_007_fetch_items(self):
        n = self.fetch_items()
        self.assertEqual(n, 30)

    def test_008_fetch_items_again(self):
        n = self.fetch_items()
        self.assertEqual(n, 20)

    def test_009_fetch_items_again_force(self):
        n = self.fetch_items(force=True)
        self.assertEqual(n, 30)

    def list_items(self, since_date=None, limit=10, channel_names=None):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        items = p.list_items(since_date=since_date, limit=limit, channel_names=channel_names)
        count = 0
        for item in items:
            count += 1
            s = item["_source"]
            length = debug.byte_size_string(len(s.get("page") or ""), 2)
            print "%-10s | %10s | %s" % (s["channel"], length, s["title"])
        print "tot =", count
        return count

    def test_010_list_items(self):
        n = self.list_items(limit=100)
        self.assertEqual(n, 30)

    def test_011_list_items_since(self):
        n = self.list_items(limit=100, since_date=ago2date("3d"))
        self.assertLess(n, 30)

    def test_012_list_items_limit(self):
        n = self.list_items(limit=5)
        self.assertEqual(n, 5)

    def test_013_list_items_digi(self):
        n = self.list_items(limit=100, channel_names=["digi"])
        self.assertEqual(n, 20)

    def test_014_delete_items_digi(self):
        p = RssMonitor(index="rss_test", simulate=True)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        n = p.delete_items(["digi"])
        print "tot =", n
        self.assertEqual(n, 20)

    def test_015_delete_items_before_date(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        n = p.delete_items(before_date=ago2date("3d"))
        print "tot =", n
        self.assertLess(n, 10)

    def test_016_delete_channel_recreate_items_remain(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)

        deleted = p.delete_channels(["norsis"])
        print "deleted =", deleted
        self.assertTrue(deleted, 1)

        added = p.add_channels(("norsis", "https://norsis.no/feed"))
        print "added =", added
        self.assertTrue(added, 1)

        items = p.list_items(channel_names=["norsis"])
        count = len(list(items))
        print "items =", count
        self.assertGreater(count, 0);

    def test_017_delete_channel_with_items_recreate_items_gone(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)

        deleted = p.delete_channels(["norsis"], delete_items=True)
        print "deleted =", deleted
        self.assertTrue(deleted, 1)

        added = p.add_channels(("norsis", "https://norsis.no/feed"))
        print "added =", added
        self.assertTrue(added, 1)

        items = p.list_items(channel_names=["norsis"])
        count = len(list(items))
        print "items =", count
        self.assertEqual(count, 0);

    def test_018_delete_channels(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        n = p.delete_channels()
        print "deleted =", n
        self.assertEqual(n, 2)

    def test_019_delete_index(self):
        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index)
        IndicesClient(p._get_es()).refresh(elasticsearch_index)
        ok = p.delete_index()
        self.assertTrue(ok)


class TestRSS_monitor(unittest.TestCase):

    def test_monitor_no_write(self):

        # A dead end for the items:
        w = Processor()
        w.create_connector(lambda doc: doc, "end")

        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index, interval=10)  # 10 seconds

        print "Deleting potential old index, just in case"
        deleted = p.delete_index()
        print "  deleted =", deleted
        sleep(1)

        print "Creating index"
        ok = p.create_index()
        self.assertTrue(ok)
        sleep(1)

        print "Adding channel"
        n = p.add_channels(("vg_sport", "http://www.vg.no/rss/create.php?categories=20"))
        print "  tot =", n
        self.assertEqual(n, 1)
        sleep(1)

        print "Configuring pipeline with dead end"
        # A dead end for the items:
        w = Processor()
        w.create_connector(lambda doc: doc, "end")
        w.subscribe(p)
        sleep(1)

        print "Running monitor for 5 seconds"
        p.start()
        sleep(5)
        p.stop()
        w.wait()
        print "Monitor stopped"
        sleep(1)

        print "Checking that there NO are items in the index"
        items = p.list_items()
        tot = len(list(items))
        print "  tot =", tot
        self.assertEqual(tot, 0)
        sleep(1)

        print "Deleting index"
        ok = p.delete_index()
        self.assertTrue(ok)
        sleep(1)

        print "All done"

    def test_monitor_pipeline_write(self):

        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index, interval=10)  # 10 seconds

        print "Deleting potential old index, just in case"
        deleted = p.delete_index()
        print "  deleted =", deleted
        sleep(1)

        print "Creating index"
        ok = p.create_index()
        self.assertTrue(ok)
        sleep(1)

        print "Adding channel"
        n = p.add_channels(("vg_sport", "http://www.vg.no/rss/create.php?categories=20"))
        print "  tot =", n
        self.assertEqual(n, 1)
        sleep(1)

        print "Configuring pipeline"
        w = ElasticsearchWriter(hosts=[elasticsearch_host])
        w.subscribe(p)
        sleep(1)

        print "Running monitor for 5 seconds"
        p.start()
        sleep(5)
        p.stop()
        w.wait()
        print "Monitor stopped"
        sleep(1)

        print "Checking that there are items in the index"
        items = p.list_items()
        tot = len(list(items))
        print "  tot =", tot
        self.assertEqual(tot, 10)
        sleep(1)

        print "Deleting index"
        ok = p.delete_index()
        self.assertTrue(ok)
        sleep(1)

        print "All done"

    def test_monitor_direct_write(self):

        p = RssMonitor(elasticsearch_hosts=[elasticsearch_host], index=elasticsearch_index, interval=10)  # 10 seconds

        print "Deleting potential old index, just in case"
        deleted = p.delete_index()
        print "  deleted =", deleted
        sleep(1)

        print "Creating index"
        ok = p.create_index()
        self.assertTrue(ok)
        sleep(1)

        print "Adding channel"
        n = p.add_channels(("vg_sport", "http://www.vg.no/rss/create.php?categories=20"))
        print "  tot =", n
        self.assertEqual(n, 1)
        sleep(1)

        print "Running monitor for 5 seconds"
        p.start()
        sleep(5)
        p.stop()
        p.wait()
        print "Monitor stopped"
        sleep(3)

        print "Checking that there are items in the index"
        items = p.list_items()
        tot = len(list(items))
        print "  tot =", tot
        self.assertEqual(tot, 10)
        sleep(1)

        print "Deleting index"
        ok = p.delete_index()
        self.assertTrue(ok)
        sleep(1)

        print "All done"


def main():
    unittest.main()

if __name__ == "__main__":
    main()
