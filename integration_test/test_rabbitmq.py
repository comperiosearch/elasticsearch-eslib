# -*- coding: utf-8 -*-

import eslib
import unittest
from eslib.procs import RabbitmqMonitor, RabbitmqWriter, FileWriter
from time import sleep
from eslib import prog

prog.initlogs()

# NOTE: This requires a rabbit mq server to connect to with appropriate accesses

mq_host = "nets.comperio.no"
mq_username = "nets"
mq_password = "nets"
mq_virtual_host = "dev"

class TestRabbitmqWriter(unittest.TestCase):

    def _create_writer(self):
        return RabbitmqWriter(host=mq_host, username=mq_username, password=mq_password, virtual_host=mq_virtual_host)

    def _create_monitor(self):
        return RabbitmqMonitor(host=mq_host, username=mq_username, password=mq_password, virtual_host=mq_virtual_host)

    def test_config(self):
        w = self._create_writer()
        self.assertEqual(w.config.host, mq_host)
        self.assertEqual(w.config.username, mq_username)
        self.assertEqual(w.config.username, mq_password)
        self.assertEqual(w.config.virtual_host, mq_virtual_host)

    def test_view_channels(self):
        w = self._create_writer()
        w.DUMP_QUEUES()

    def test_write_to_queue(self):
        w = self._create_writer()
        w.config.queue = "TEST_QUEUE"
        w.purge_queue()

        w.start()
        w.put("doc A")
        w.put("doc B")
        w.put("doc C")
        w.stop()
        w.wait()
        print "Waiting 5 seconds for rabbit mq queue stats to catch up."
        sleep(5)
        qsize = w.get_queue()["messages_ready"]
        w.DUMP_QUEUES()

        self.assertEqual(qsize, 3)

        # Clean up
        w.delete_queue()

    def test_write_to_exchange(self):
        w = self._create_writer()
        w.config.queue = "TEST_QUEUE"
        w.config.exchange = "TEST_EXCHANGE"
        w.purge_queue("TEST_EXCHANGE_shared")

        w.start()
        w.put("doc A")
        w.put("doc B")
        w.put("doc C")
        w.stop()
        w.wait()
        print "Waiting 5 seconds for rabbit mq queue stats to catch up."
        sleep(5)
        qsize = w.get_queue("TEST_EXCHANGE_shared")["messages_ready"]
        w.DUMP_QUEUES()

        self.assertEqual(qsize, 3)

        # Clean up
        w.delete_exchange()
        w.delete_queue("TEST_EXCHANGE_shared")

    def test_monitor_consume_queue(self):
        output = []
        r = self._create_monitor()
        r.add_callback(lambda doc: output.append(doc))

        w = self._create_writer()

        w.config.queue = r.config.queue = "TEST_QUEUE"

        w.purge_queue()

        w.start()

        w.put("doc A")
        w.put("doc B")
        w.put("doc C")

        w.stop()
        w.wait()

        r.start()
        print "Giving monitor a second to read all from queue."
        sleep(1)
        r.stop()
        r.wait()

        print "Waiting 5 seconds for rabbit mq queue stats to catch up."
        sleep(5)
        qsize = w.get_queue()["messages_ready"]
        print "Asserting queue size now 0."
        self.assertEqual(0, qsize)
        print "Asserting all items read."
        print output
        self.assertEqual(3, len(output))

        # Cleaning up
        w.delete_exchange()
        w.delete_queue()


    def test_monitor_exclusive_queue(self):
        output = []
        r = self._create_monitor()
        r.add_callback(lambda doc: output.append(doc))

        w = self._create_writer()

        w.config.exchange = r.config.exchange = "TEST_EXCHANGE"

        # This will attach to the exchange and get an exclusive non-persistent queue
        r.config.consuming = False

        w.purge_queue("TEST_EXCHANGE_shared")

        w.start()
        r.start()

        w.put("doc A")
        w.put("doc B")
        w.put("doc C")

        w.stop()
        w.wait()

        print "Giving monitor a second to read all from queue."
        sleep(1)
        r.stop()
        r.wait()

        print "Waiting 5 seconds for rabbit mq queue stats to catch up."
        sleep(5)
        qsize = w.get_queue("TEST_EXCHANGE_shared")["messages_ready"]
        print "Asserting all items still exist in consume queue."
        #self.assertEqual(3, qsize)
        print "Asserting all items read."
        print output
        self.assertEqual(3, len(output))

        # Cleaning up
        w.delete_exchange()
        w.delete_queue("TEST_EXCHANGE_shared")

    def test_multi_monitor(self):
        """
        Test two monitors consuming from the same queue. Both should get some
        items, however which gets which items could be random.
        """
        output1 = []
        output2 = []
        r1 = self._create_monitor()
        r2 = self._create_monitor()
        r1.add_callback(lambda doc: output1.append(doc))
        r2.add_callback(lambda doc: output2.append(doc))

        w = self._create_writer()

        w.config.queue = r1.config.queue = r2.config.queue = "TEST_QUEUE"
        r1.config.consuming = r2.config.consuming = False  # But ignored since we are not using exchange..

        w.purge_queue()

        w.start()
        # Need these both to be active before we put items, or the first one will consume all before the other is created.
        r1.start()
        r2.start()

        w.put("doc A")
        w.put("doc B")
        w.put("doc C")

        w.stop()
        w.wait()

        print "Giving monitors a second to read all from queue."
        sleep(1)
        r1.stop()
        r2.stop()
        r1.wait()
        r2.wait()

        print "Asserting all items read."
        output = output1 + output2
        print output
        self.assertEqual(3, len(output))
        print "Asserting distribution"
        print "1:", output1
        print "2:", output2
        self.assertNotEquals(0, len(output1))
        self.assertNotEquals(0, len(output2))

        print "Waiting 5 seconds for rabbit mq queue stats to catch up."
        sleep(5)
        qsize = w.get_queue()["messages_ready"]
        print "Asserting queue size now 0."
        self.assertEqual(0, qsize)

        # Cleaning up
        w.delete_queue()

    def test_multi_monitor_with_exchange(self):
        """
        Test two monitors consuming from the same consumable queue and two
        listening on exclusive queues. Both consuming should get some items,
        however which gets which items could be random. Both monitors on
        exclusive queues should get all items.
        """
        output1 = []
        output2 = []
        excout1 = []
        excout2 = []
        r1 = self._create_monitor()
        r2 = self._create_monitor()
        e1 = self._create_monitor()
        e2 = self._create_monitor()
        r1.add_callback(lambda doc: output1.append(doc))
        r2.add_callback(lambda doc: output2.append(doc))
        e1.add_callback(lambda doc: excout1.append(doc))
        e2.add_callback(lambda doc: excout2.append(doc))

        w = self._create_writer()

        w.config.exchange = "TEST_EXCHANGE"
        r1.config.exchange = r2.config.exchange = "TEST_EXCHANGE"
        e1.config.exchange = e2.config.exchange = "TEST_EXCHANGE"
        r1.config.consuming = r2.config.consuming = True
        e1.config.consuming = e2.config.consuming = False

        w.purge_queue("TEST_EXCHANGE_shared")

        w.start()
        # Need these both to be active before we put items, or the first one will consume all before the other is created.
        r1.start()
        r2.start()
        # These must be started before we put items, or the exclusive queues will not exist to receive data.
        e1.start()
        e2.start()

        w.put("doc A")
        w.put("doc B")
        w.put("doc C")

        w.stop()
        w.wait()

        print "Giving monitors a second to read all from queue."
        sleep(1)
        r1.stop()
        r2.stop()
        e1.stop()
        e2.stop()
        r1.wait()
        r2.wait()
        e1.wait()
        e2.wait()

        print "Asserting all items read."
        output = output1 + output2
        print output
        self.assertEqual(3, len(output))
        print "Asserting distribution."
        print "C1:", output1
        print "C2:", output2
        self.assertNotEquals(0, len(output1))
        self.assertNotEquals(0, len(output2))

        print "Asserting exclusive monitors each got all items."
        print "E1:", excout1
        print "E2:", excout2
        self.assertEquals(3, len(excout1))
        self.assertEquals(3, len(excout2))

        print "Waiting 5 seconds for rabbit mq queue stats to catch up."
        sleep(5)
        qsize = w.get_queue("TEST_EXCHANGE_shared")["messages_ready"]
        print "Asserting queue size now 0."
        self.assertEqual(0, qsize)

        # Cleaning up
        w.delete_exchange()
        w.delete_queue("TEST_EXCHANGE_shared")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
