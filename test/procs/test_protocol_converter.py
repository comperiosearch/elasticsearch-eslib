# -*- coding: utf-8 -*-

import unittest
from eslib.procs import ProtocolConverter

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

class TestProtocolConverter(unittest.TestCase):

    def test_func_one_lambda(self):

        csv2list = lambda doc: [",".join(doc)]

        p = ProtocolConverter(func=csv2list, input_protocol="list", output_protocol="csv")
        p.keepalive = True

        output = []
        p.add_callback(lambda doc: output.append(doc))

        p.start()
        p.put(["a","b","c","d"])
        p.stop()
        p.wait()

        print "output=", output[0]

        self.assertEqual(output[0], "a,b,c,d")


    def yieldfunc(self, doc):
        yield doc.lower()
        yield doc.upper()

    def test_func_multi_yield(self):

        p = ProtocolConverter(func=self.yieldfunc, input_protocol="str", output_protocol="str")
        p.keepalive = True

        output = []
        p.add_callback(lambda doc: output.append(doc))

        p.start()
        p.put("a")
        p.put("b")
        p.put("c")
        p.stop()
        p.wait()

        joined = ",".join(output)
        print "output=", joined

        self.assertEqual(joined, "a,A,b,B,c,C")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
