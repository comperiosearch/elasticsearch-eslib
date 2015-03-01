# -*- coding: utf-8 -*-

ENDPOINT = "localhost:4000"

import unittest
from eslib.service import Service, HttpService, status
from eslib.procs import Timer, Transformer
import requests, time, threading

import eslib.prog
eslib.prog.initlogs()

class TestService(Service):
    def __init__(self, **kwargs):
        super(TestService, self).__init__(**kwargs)

        self.ending = False
        self.requires_metadata = False

    def on_setup(self):
        self._timer = Timer(service=self, actions=[(3, 3, "ping")])
        self._pc = Transformer(service=self, func=self._func)
        self._pc.subscribe(self._timer)

        self.register_procs(self._timer, self._pc)

        return True

    def _func(self, doc):
        print doc
        if self.ending:
           print "FUNC STOP"
           self._timer.stop()

    def is_processing(self):
        return self._pc.running

    def is_aborted(self):
        return self._pc.aborted

    def is_suspended(self):
        return self._pc.suspended

    # on_start_processing (should be ran async)
    def on_processing_start(self):
        self._timer.start()
        time.sleep(1)  # Simulate that it takes some time
        return True

    def on_processing_stop(self):
        time.sleep(1)  # Simulate that it takes some time
        self._timer.stop()
        self._pc.wait()
        return True

    # on_abort_processing
    def on_processing_abort(self):
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
        p.ending = False

        print "Starting service"
        print "Asserting '%s' (not started)" % status.DOWN
        self.assertEqual(p.status, status.DOWN)

        p.run()
        # This does not require config, thus going straight from 'down' to 'idle'
        print "Asserting '%s'" % status.IDLE
        self.assertEqual(p.status, status.IDLE)

        print "Shutting down"
        p.shutdown(wait=True)
        print "Asserting '%s' (shut down)" % status.DOWN
        self.assertEqual(p.status, status.DOWN)

    def test_lifecycle(self):
        p = TestService()#mgmt_endpoint=ENDPOINT)  # localhost:4444 by default
        p.ending = False

        print "Starting service"
        print "Asserting '%s' (not started)" % status.DOWN
        self.assertEqual(status.DOWN, p.status)

        p.run()
        # This does not require config, thus going straight from 'down' to 'idle'
        print "Asserting '%s'" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing"
        p.processing_start()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        time.sleep(1)
        print "Stopping processing"
        p.processing_stop()
        time.sleep(0.1)
        print "Asserting '%s'" % status.STOPPING
        self.assertEqual(status.STOPPING, p.status)

        print "Waiting for processing to stop"
        p.processing_wait()
        print "Asserting '%s' (stopped)" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing"
        p.processing_start()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        time.sleep(1)
        print "Aborting processing"
        p.processing_abort()
        print "Asserting '%s'" % status.ABORTED
        self.assertEqual(status.ABORTED, p.status)

        print "Starting processing"
        p.processing_start()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        print "Shutting down"
        p.shutdown()
        #threading.Thread(target=lambda : p.shutdown()).start()
        time.sleep(0.1)
        print "Asserting '%s'" % status.CLOSING
        self.assertEqual(status.CLOSING, p.status)

        print "Waiting for shutdown"
        p.wait()
        print "Asserting '%s' (shut down)" % status.DOWN
        self.assertEqual(status.DOWN, p.status)

    def test_lifecycle_ending_service(self):
        p = TestService()#mgmt_endpoint=ENDPOINT)  # localhost:4444 by default
        p.ending = True

        print "Starting service"
        print "Asserting '%s' (not started)" % status.DOWN
        self.assertEqual(status.DOWN, p.status)

        p.run()
        # This does not require config, thus going straight from 'down' to 'idle'
        print "Asserting '%s'" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing (take 1)"
        p.processing_start()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        print "Waiting for processing to finish (take 1)"
        p.processing_wait()
        print "Asserting '%s' (stopped)" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Starting processing (take 2)"
        p.processing_start()
        print "Asserting '%s'" % status.PROCESSING
        self.assertEqual(status.PROCESSING, p.status)

        print "Waiting for processing to finish (take 2)"
        p.processing_wait()
        print "Asserting '%s' (stopped)" % status.IDLE
        self.assertEqual(status.IDLE, p.status)

        print "Shutting down (waiting)"
        p.shutdown(wait=True)
        print "Asserting '%s' (shut down)" % status.DOWN
        self.assertEqual(status.DOWN, p.status)

def main():
    unittest.main()

if __name__ == "__main__":
    main()
