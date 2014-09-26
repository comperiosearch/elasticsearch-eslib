import unittest, time, sys
from connections import Connections
from eslib import Processor, Generator, Controller


class MyGenerator(Generator):
    def __init__(self, name):
        super(MyGenerator, self).__init__(name)
        self.create_connector("input", "proto_str")
        self.create_socket("output", "proto_str")
        self.stop_at = 0
        self.sleep_time = 0.2

    def on_startup(self):
        print "%s: starting" % self.name
        self.serial_no = 0

    def on_shutdown(self):
        print "%s: shutdown" % self.name

    def on_abort(self):
        print "%s: abort" % self.name

    def on_tick(self):
        self.serial_no += 1
        self.count += 1
        doc = "%s.%d" % (self.name, self.serial_no)
        print "%s: generated doc: %s" % (self.name, doc)
        self.sockets["output"].send(doc)
        #print "%s: tick, now sleeping 0.2" % self.name
        time.sleep(self.sleep_time)
        print "SERIAL/STOP: %d/%d" % (self.serial_no, self.stop_at)
        if self.stop_at and self.serial_no == self.stop_at:
            self.stop()
            #time.sleep(1)


class MyInOut(Processor):
    def __init__(self, name):
        super(MyInOut, self).__init__(name)
        self.create_connector(self.proc_input, "input", "proto_str")
        self.create_socket("output", "proto_str")
        self.count = 0

    def proc_input(self, document):
        print "%s: incoming document: %s" % (self.name, document)
        self.count += 1
        self.sockets["output"].send("%s.%s" % (self.name, document))


class TestExecution(unittest.TestCase, Connections):

    def callback(self, document):
        print "CALLBACK RECEIVED: %s" % document
        self.callback_count += 1

    def test_in_out(self):

        self.callback_count = 0

        p = MyInOut("myinout")
        p.add_callback(self.callback)#, "output")
        p.start()
        for i in range(10):
            #print "wait loop tick %d" % i
            #p.connectors["input"].receive("X%d" % i)
            p.put("X%d" % i)#, "input")
            time.sleep(0.1)
        p.stop()

        self.assertEqual(self.callback_count, 10, "Expected 10 callbacks, got %d" % self.callback_count)


    def test_generator_outer_stop(self):

        self.callback_count = 0

        gen = MyGenerator("mygen")
        gen.add_callback(self.callback)
        gen.start()
        for i in range(10):
            #print "wait loop tick %d" % i
            time.sleep(0.1)
        gen.stop()

        # It lasted 10 seconds, so one gen per second should be 10 docs
        self.assertEqual(self.callback_count, 5, "Expected 5 callbacks, was %d" % self.callback_count)


    def test_generator_self_stop(self):

        self.callback_count = 0

        gen = MyGenerator("mygen")
        gen.add_callback(self.callback)
        gen.stop_at = 5
        gen.start()

        time.sleep(0.1)
        # While running, these should be:
        self.assertTrue(gen.running)
        self.assertFalse(gen.stopping)
        self.assertTrue(gen.accepting)
        self.assertFalse(gen.aborted)

        for i in range(10):
            #print "wait loop tick %d" % i
            time.sleep(0.1)
        # Expecting it to stop by itself after 5 items

        time.sleep(0.1)
        # The stopping process is delayed by one sec, meanwhile these should be:
        self.assertTrue(gen.running)
        self.assertTrue(gen.stopping)
        self.assertFalse(gen.accepting)
        self.assertFalse(gen.aborted)

        # It lasted 1 second, so one gen per 0.2 second should be 5 docs
        self.assertEqual(self.callback_count, 5, "Expected 5 callbacks, was %d" % self.callback_count)

        time.sleep(1)
        # Now it should be finished
        self.assertFalse(gen.running)
        self.assertFalse(gen.stopping)
        self.assertFalse(gen.accepting)
        self.assertFalse(gen.aborted)


    def test_generator_suspend_abort(self):

        self.callback_count = 0

        gen = MyGenerator("mygen")
        gen.add_callback(self.callback)
        gen.start()

        time.sleep(0.1)
        # While running, these should be:
        self.assertTrue(gen.running)
        self.assertFalse(gen.stopping)
        self.assertTrue(gen.accepting)
        self.assertFalse(gen.aborted)

        for i in range(10):
            #print "wait loop tick %d" % i
            time.sleep(0.1)

            if gen.serial_no == 3:
                gen.suspend()
                # It should now be suspended
                self.assertTrue(gen.suspended)

        time.sleep(0.1)
        gen.resume()
        self.assertFalse(gen.suspended)

        for i in range(10):
            time.sleep(0.1)
            if gen.serial_no == 5:
                gen.suspend()  # Just to see if it remains after abort
                gen.abort()

        # It should now be aborted
        self.assertFalse(gen.running)
        self.assertFalse(gen.stopping)
        self.assertFalse(gen.accepting)
        self.assertTrue(gen.aborted)
        self.assertTrue(gen.suspended)  # Should still remain after abort (and stop), although it serves no purpose then


    def callback_sequence(self, document):
        self.seq.append(document)
        print document

    def test_sequence(self):
        p1 = MyInOut("p1")
        p2 = MyInOut("p2")
        p3 = MyInOut("p3")
        p4 = MyInOut("p4")
        p1.attach(p2.attach(p3.attach(p4)))

        self.seq = []
        p4.add_callback(self.callback_sequence)

        p1.start()
        p1.put("hello1")
        p2.put("hello2")
        p1.stop()

        c = Controller(p1, p2)
        c.DUMP()

        self.assertTrue("p4.p3.p2.p1.hello1" in self.seq, "Expected p4.p3.p2.p1.hello1 to have been generated.")
        self.assertTrue("p4.p3.p2.hello2"    in self.seq, "Expected p4.p3.p2.hello2 to have been generated.")

    def test_no_keepalive(self):

        g = MyGenerator("generator")
        r = MyInOut("receiver")
        r.subscribe(g)
        g.stop_at = 5

        g.start()
        #g.stop()
        time.sleep(3)

        self.assertTrue(g.running == False and r.running == False, "Both g and r should now be stopped.")

    def test_keepalive1(self):

        g = MyGenerator("generator")
        r = MyInOut("receiver")
        r.subscribe(g)
        g.stop_at = 5
        r.keepalive = True

        g.start()
        time.sleep(3)

        self.assertTrue(g.running == False and r.running == True, "g should now be stopped but r keep on running.")

        r.stop()
        time.sleep(0.1)
        self.assertTrue(g.running == False and r.running == False, "Both g and r should now be stopped.")

    def test_keepalive2(self):

        g1 = MyGenerator("generator1")
        g2 = MyGenerator("generator2")
        g3 = MyGenerator("generator3")
        g1.stop_at = 1
        g2.stop_at = 2
        g3.stop_at = 3
        g1.sleep_time = 1
        g2.sleep_time = 2
        g3.sleep_time = 3

        r1 = MyInOut("receiver_keep")
        r1.keepalive = True
        r1.subscribe(g1)
        r1.subscribe(g2)
        r1.subscribe(g3)

        r2 = MyInOut("receiver_nokeep")
        r2.subscribe(g1)
        r2.subscribe(g2)
        r2.subscribe(g3)

        f1 = MyInOut("finale1")
        f1.subscribe(r1)

        f12 = MyInOut("finale12")
        f12.subscribe(r1)
        f12.subscribe(r2)

        f2 = MyInOut("finale2")
        f2.subscribe(r2)

        g1.start()
        g2.start()
        g3.start()
        time.sleep(10)

        c = Controller(g1, g2, g3, r1, r2, f1, f12, f2)
        c.DUMP()

        self.assertTrue(not (g1.running and g2.running and g3.running), "All generators should be stopped.")
        self.assertTrue(r1.running and not r2.running, "r1 should be running (keepalive), r2 stopped.")
        self.assertTrue(f1.running and not r2.running and not f2.running, "f1 should be running (only listening to r1/keepalive), f12 and f2 should be stopped.")

        r1.stop()
        c.DUMP()

        self.assertTrue(not r1.running and not f1.running, "r1 and f1 should now also be stopped.")

        self.assertTrue(g1.count == 1 and g2.count == 2 and g3.count == 3, "Generator should have processed 1, 2, 3 items.")
        self.assertTrue(r1.count == 6 and r2.count == 3, "Receivers should have produced 6, 3 items..")
        self.assertTrue(f1.count == 6 and f12.count == 6 and f2.count == 3, "Finales should have processed 6, 6, 3 items.")


if __name__ == '__main__':
    unittest.main()
