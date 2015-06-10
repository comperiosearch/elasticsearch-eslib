# -*- coding: utf-8 -*-

import unittest
from eslib.procs import HttpMonitor
import requests

import eslib.prog
eslib.prog.initlogs()

class TestHttpMonitor(unittest.TestCase):

    def test_get(self):
        self.hooked_msg = None
        output = []

        p = HttpMonitor(hook=self._hook)  # localhost:4000 by default
        p.add_callback(lambda proc, doc: output.append(doc))

        print "Starting server."
        p.start()

        print "Sending request"
        res = requests.get("http://localhost:4000/ppp?arg=aaa")
        print "RES=", res, res.content


        print "Stopping server"
        p.stop()
        p.wait()
        print "Server finished."

        self.assertEquals(self.hooked_msg, "GET_/ppp?arg=aaa")
        self.assertEquals(output[0], "ppp?arg=aaa")

    def test_post(self):
        self.hooked_msg = None
        output = []

        p = HttpMonitor(hook=self._hook)  # localhost:4000 by default
        p.add_callback(lambda proc, doc: output.append(doc))

        print "Starting server."
        p.start()

        print "Sending request (text)"
        res = requests.post("http://localhost:4000/ppp?arg=aaa", data="some data", headers={'content-type': 'text/text'})
        print "RES=", res, res.content
        print "Sending request (json)"
        res = requests.post("http://localhost:4000/ppp?arg=aaa", data="[1, 2, 3]", headers={'content-type': 'application/json'})
        print "RES=", res, res.content

        print "Stopping server"
        p.stop()
        p.wait()
        print "Server finished."

        self.assertEquals(self.hooked_msg, "POST_/ppp?arg=aaa")
        self.assertEquals(output[0], "some data")
        self.assertEquals(output[1], [1, 2, 3])

    def _hook(self, request_handler, verb, path, data, format="application/json"):
        print "Hook called: ", verb, path, data
        self.hooked_msg = "%s_%s" % (verb, path)


def main():
    unittest.main()

if __name__ == "__main__":
    main()
