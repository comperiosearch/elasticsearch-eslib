# -*- coding: utf-8 -*-

import unittest
from eslib.procs import ProtocolConverter

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

class TestProtocolConverter(unittest.TestCase):

    def test_func(self):

        csv2list = lambda doc: ",".join(doc)

        p = ProtocolConverter(csv2list, input_protocol="list", output_protocol="csv")
        p.keepalive = True

        output = []
        p.add_callback(lambda doc: output.append(doc))

        p.start()
        p.put(["a","b","c","d"])
        p.stop()
        p.wait()

        print "output=", output[0]

        self.assertEqual(output[0], "a,b,c,d")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
