import unittest
from eslib import Processor

class Connections(object):

    def create_processors(self):
        self.a = Processor(name="processor_a")
        self.b = Processor(name="processor_b")
        self.c = Processor(name="processor_c")
        self.d = Processor(name="processor_d")

    def create_terminals(self):
        self.a.create_connector(None, "input") # Protocol anything
        self.a.create_socket("output", "proto_doc")
        self.b.create_connector(None, "input", "proto_doc")
        self.b.create_socket("output_doc", "proto_doc")
        self.b.create_socket("output_str", "proto_str")
        self.c.create_connector(None, "input_doc", "proto_doc")
        self.c.create_connector(None, "input_str", "proto_str")
        self.c.create_socket("output_doc", "proto_doc")
        self.c.create_socket("output_ext", "proto_doc.extended")
        self.c.create_socket("output_anything")
        self.d.create_connector(None, "input_anything")
        self.d.create_connector(None, "input_doc", "proto_doc")
        self.d.create_connector(None, "input_ext", "proto_doc.extended")

    def connect_terminals(self):
        self.b.subscribe(self.a)  # Ok call, only one socket and connector
        self.c.subscribe(self.b, "output_doc", "input_doc") # Ok
        self.c.subscribe(self.a, connector_name="input_doc") # Ok, a's only socket name can be omitted
        self.d.subscribe(self.c, "output_doc", "input_anything") # Ok, any input accepted
        self.d.subscribe(self.c, "output_ext", "input_ext") # Ok, protocol exact match


class TestConnections(unittest.TestCase, Connections):

    def test_create_processors(self):
        self.create_processors()

        self.assertIsNotNone(self.a, "Processor a None")
        self.assertIsNotNone(self.b, "Processor b None")
        self.assertIsNotNone(self.c, "Processor c None")
        self.assertIsNotNone(self.d, "Processor d None")

    def test_create_terminals(self):
        self.create_processors()
        self.create_terminals()

        self.assertTrue(len(self.a.connectors) == 1, "Expected 1 connector for a")
        self.assertTrue(len(self.b.connectors) == 1, "Expected 1 connector for b")
        self.assertTrue(len(self.c.connectors) == 2, "Expected 2 connectors for c")
        self.assertTrue(len(self.d.connectors) == 3, "Expected 3 connectors for d")

        self.assertTrue(len(self.a.sockets) == 1, "Expected 1 socket for a")
        self.assertTrue(len(self.b.sockets) == 2, "Expected 2 sockets for b")
        self.assertTrue(len(self.c.sockets) == 3, "Expected 3 sockets for c")
        self.assertTrue(len(self.d.sockets) == 0, "Expected 0 sockets for d")


    def test_connect(self):
        self.create_processors()
        self.create_terminals()
        self.connect_terminals()

        # Cannot decide socket, should fail:
        self.assertRaises(Exception, self.c.subscribe, (self.b,))
        # Ok for socket, but still cannot decide which one of C's connectors:
        self.assertRaises(Exception, self.c.subscribe, (self.b, "output_doc"))
        # Protocol error:
        self.assertRaises(Exception, self.c.subscribe, (self.b, "output_doc", "input_str"))
        # Should fail on protocol error:
        self.assertRaises(Exception, self.d.subscribe, (self.c, "output_anything", "input_doc"))
         # Protocol error:
        self.assertRaises(Exception, self.d.subscribe, (self.c, "output_ext", "input_doc"))
        # Protocol error, connector more specific than socket:
        self.assertRaises(Exception, self.d.subscribe, (self.c, "output_doc", "input_ext"))

        # Do a quick check to see if expected number of connections are now ok
        self.assertTrue(len(self.a.sockets["output"].connections) == 2) # b and c
        self.assertTrue(len(self.b.connectors["input"].connections) == 1) # b
        self.assertTrue(len(self.b.sockets["output_doc"].connections) == 1) # c
        self.assertTrue(len(self.c.connectors["input_doc"].connections) == 2) # a and b
        self.assertTrue(len(self.c.sockets["output_doc"].connections) == 1) # d
        self.assertTrue(len(self.c.sockets["output_ext"].connections) == 1) # d
        self.assertTrue(len(self.d.connectors["input_anything"].connections) == 1) # c
        self.assertTrue(len(self.d.connectors["input_ext"].connections) == 1) # c


    def test_connect(self):
        self.create_processors()
        self.create_terminals()
        self.connect_terminals()

        self.b.unsubscribe() # unsubscribes all input connectors
        self.assertTrue(len(self.a.sockets["output"].connections) == 1)  # only c left
        self.assertTrue(len(self.b.connectors["input"].connections) == 0)

        self.c.unsubscribe(self.a)
        self.c.unsubscribe(self.a, connector_name="input_doc")
        self.assertTrue(len(self.a.sockets["output"].connections) == 0)
        self.assertTrue(len(self.b.sockets["output_doc"].connections) == 1) # c remains
        self.assertTrue(len(self.c.connectors["input_doc"].connections) == 1)  # only b left

        self.c.unsubscribe(connector_name="input_doc")
        self.assertTrue(len(self.b.sockets["output_doc"].connections) == 0) # c now also gone

        self.c.detach(self.d) # Should detach all connections to d
        self.assertTrue(len(self.c.sockets["output_doc"].connections) == 0)
        self.assertTrue(len(self.c.sockets["output_ext"].connections) == 0)
        self.assertTrue(len(self.d.connectors["input_anything"].connections) == 0)
        self.assertTrue(len(self.d.connectors["input_ext"].connections) == 0)


def main():
    unittest.main()

if __name__ == "__main__":
    main()
