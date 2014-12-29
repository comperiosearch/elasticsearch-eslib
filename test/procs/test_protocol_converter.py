# -*- coding: utf-8 -*-

import unittest
from eslib.procs import ProtocolConverter

class TestProtocolConverter(unittest.TestCase):

    def test_func_one_lambda(self):

        csv2list = lambda doc: [",".join(doc)]

        p = ProtocolConverter(func=csv2list, input_protocol="list", output_protocol="csv")

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


    def edge2ids(self, doc):
        if doc["type"] == "author":
            yield doc["from"]
        else:
            yield doc["from"]
            yield doc["to"]

    def test_graph_edge_convertion(self):
        p = ProtocolConverter(func=self.edge2ids, input_protocol="str", output_protocol="str")

        output = []
        p.add_callback(lambda doc: output.append(doc))

        p.start()
        p.put({"type": "author" , "from": "1", "to": "1"})
        p.put({"type": "mention", "from": "2", "to": "3"})
        p.put({"type": "quote"  , "from": "4", "to": "1"})
        p.stop()
        p.wait()

        joined = ",".join(output)
        print "output=", joined

        self.assertEqual(joined, "1,2,3,4,1")



def main():
    unittest.main()

if __name__ == "__main__":
    main()
