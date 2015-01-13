# -*- coding: utf-8 -*-

ENDPOINT = "localhost:4000"

import unittest
from eslib.service import Service, HttpService, status
from eslib.procs import Timer, Transformer
#from eslib.service import HttpService
import requests, time, threading

import eslib.prog
eslib.prog.initlogs()

class TestService(Service):
    def __init__(self, **kwargs):
        super(TestService, self).__init__(**kwargs)

        self.requires_metadata = False

    def on_setup(self):
        self._timer = Timer(service=self, actions=[(3, 3, "ping")])
        self._pc = Transformer(service=self, func=self._func)
        self._pc.subscribe(self._timer)
        return True

    def _func(self, doc):
        print doc
        print "FUNC STOP"
        self._timer.stop()

    def is_processing(self):
        return self._pc.running

    # on_start_processing (should be ran async)
    def on_start_processing(self):
        self._timer.start()
        time.sleep(1)  # Simulate that it takes some time
        return True

    def on_stop_processing(self):
        self._timer.stop()
        #self._pc.wait()
        time.sleep(1)  # Simulate that it takes some time
        return True

    # on_abort_processing
    def on_abort_processing(self):
        self._timer.abort()
        self._pc.stop()
        return True


    # TODO: on_update_metadata


class HttpTestService(HttpService, TestService):

    def __init__(self, **kwargs):
        super(HttpTestService, self).__init__(**kwargs)

        # Add management routes to functions
        self.add_route(self._test1, "GET", "test1/{id}/{?mode}", ["mode"])

    def _test1(self, request_handler, payload, **kwargs):
        parameters = kwargs
        print "TEST1:", parameters
        return {"echo": parameters}


class TestTestService(unittest.TestCase):

    def test_run_shutdown(self):
        p = TestService()#mgmt_endpoint=ENDPOINT)  # localhost:4444 by default

        print "Starting service"
        print "Asserting '%s' (not started)" % status.DEAD
        self.assertEqual(p.status, status.DEAD)

        p.run()
        # This does not require config, thus going straight from 'dead' to 'idle'
        print "Asserting '%s'" % status.IDLE
        self.assertEqual(p.status, status.IDLE)

        print "Shutting down"
        p.shutdown(wait=True)
        print "Asserting '%s' (shut down)" % status.DEAD
        self.assertEqual(p.status, status.DEAD)


    def test_lifecycle(self):
        p = TestService()#mgmt_endpoint=ENDPOINT)  # localhost:4444 by default

        print "Starting service"
        print "Asserting '%s' (not started)" % status.DEAD
        self.assertEqual(status.DEAD, p.status)

        p.run()
        # This does not require config, thus going straight from 'dead' to 'idle'
        print "Asserting '%s'" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing"
        p.start_processing()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        # time.sleep(1)
        # print "Stopping processing"
        # p.stop_processing(wait=False)
        # print "Asserting '%s'" % status.STOPPING
        # self.assertEqual(status.STOPPING, p.status)

        # p.wait_processing()
        # print "Asserting '%s' (stopped)" % status.IDLE
        # self.assertEqual(status.IDLE, p.status)
        #
        # print "Starting processing"
        # p.start_processing()
        # print "Asserting '%s'" % status.PROCESSING
        # self.assertEqual(status.PROCESSING, p.status)
        #
        # time.sleep(1)
        # print "Aborting processing"
        # p.abort_processing()
        # print "Asserting '%s'" % status.ABORTED
        # self.assertEqual(status.ABORTED, p.status)
        #
        print "Starting processing"
        p.start_processing()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        print "Shutting down"
        threading.Thread(target=lambda : p.shutdown()).start()
        time.sleep(0.1)
        print "Asserting '%s'" % status.CLOSING
        self.assertEqual(status.CLOSING, p.status)

        p.wait()
        print "Asserting '%s' (shut down)"
        self.assertEqual(status.DEAD, p.status)

    def test_lifecycle_ending_service(self):
        p = TestService()#mgmt_endpoint=ENDPOINT)  # localhost:4444 by default

        print "Starting service"
        print "Asserting '%s' (not started)" % status.DEAD
        self.assertEqual(status.DEAD, p.status)

        p.run()
        # This does not require config, thus going straight from 'dead' to 'idle'
        print "Asserting '%s'" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing (take 1)"
        p.start_processing()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        print "Waiting for processing to finish (take 1)"
        p.wait_processing()
        print "Asserting '%s' (stopped)" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing (take 2)"
        p.start_processing()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        print "Waiting for processing to finish (take 2)"
        p.wait_processing()
        print "Asserting '%s' (stopped)" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Shutting down (waiting)"
        p.shutdown(wait=True)
        print "Asserting '%s' (shut down)" % status.DEAD
        self.assertEqual(status.DEAD, p.status)

# TODO: TEST BREAK BY KEYBOARD INTERRUPT

def main():
    unittest.main()

if __name__ == "__main__":
    main()
