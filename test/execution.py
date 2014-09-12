import unittest, time, sys
from connections import Connections
from eslib2 import Processor, Generator

class MyGenerator(Generator):
    def __init__(self, name):
        super(MyGenerator, self).__init__(name)
        self.create_connector("input", "proto_str")
        self.create_socket("output", "proto_str")
        self.stop_at = 0

    def startup_handler(self):
        print "%s: starting" % self.name
        self.serial_no = 0

    def shutdown_handler(self):
        print "%s: shutdown" % self.name

    def abort_handler(self):
        print "%s: abort" % self.name

    def processing_tick_handler(self):
        self.serial_no += 1
        self.sockets["output"].send("generated-%d" % self.serial_no)
        #print "%s: tick, now sleeping 0.2" % self.name
        time.sleep(0.2)
        #print "SERIAL/STOP: %d/%d" % (self.serial_no, self.stop_at)
        if self.stop_at and self.serial_no == self.stop_at:
            self.stop()
            time.sleep(1)


class MyInOut(Processor):
    def __init__(self, name):
        super(MyInOut, self).__init__(name)
        self.create_connector(self.proc_input, "input", "proto_str")
        self.create_socket("output", "proto_str")

    def proc_input(self, document):
        #print "%s: incoming document: %s" % (self.name, document)
        self.sockets["output"].send("processed by %s: %s" % (self.name, document))


class TestExecution(unittest.TestCase, Connections):

    def callback(self, document):
        #print "CALLBACK RECEIVED: %s" % document
        self.callback_count += 1

    def test_in_out(self):

        self.callback_count = 0

        p = MyInOut("myinout")
        p.callback(self.callback)#, "output")
        p.start()
        for i in range(10):
            #print "wait loop tick %d" % i
            #p.connectors["input"].receive("X%d" % i)
            p.send("X%d" % i)#, "input")
            time.sleep(0.1)
        p.stop()

        self.assertEqual(self.callback_count, 10, "Expected 10 callbacks, got %d" % self.callback_count)


    def test_generator_outer_stop(self):

        self.callback_count = 0

        gen = MyGenerator("mygen")
        gen.callback(self.callback)
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
        gen.callback(self.callback)
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
        gen.callback(self.callback)
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


if __name__ == '__main__':
    unittest.main()
