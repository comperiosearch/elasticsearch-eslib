import unittest
from eslib import Processor, Terminal, Connector, Socket

class TestProtocolCompliance(unittest.TestCase):

# TEST mimic / passthrough protocols

    def test_protocol_equal(self):
        s = Socket("sock_a", "proto_a")
        c = Connector("conn_a", "proto_a")
        self.assertTrue(Terminal.protocol_compliance(s, c))

    def test_protocol_not_equal(self):
        s = Socket("sock_a", "proto_b")
        c = Connector("conn_a", "proto_a")
        self.assertFalse(Terminal.protocol_compliance(s, c))

    def test_protocol_general_accepts_special(self):
        s = Socket("sock_a", "general.special")
        c = Connector("conn_a", "general")
        self.assertTrue(Terminal.protocol_compliance(s, c))

    def test_protocol_special_too_strict_for_general(self):
        s = Socket("sock_a", "general")
        c = Connector("conn_a", "general.special")
        self.assertFalse(Terminal.protocol_compliance(s, c))

    def test_protocol_any_any(self):
        s = Socket("sock_a", None)
        c = Connector("conn_a", None)
        self.assertTrue(Terminal.protocol_compliance(s, c))

    def test_protocol_any_sock(self):
        s = Socket("sock_a", None)
        c = Connector("conn_a", "x")
        self.assertTrue(Terminal.protocol_compliance(s, c))

    def test_protocol_any_conn(self):
        s = Socket("sock_a", "x")
        c = Connector("conn_a", None)
        self.assertTrue(Terminal.protocol_compliance(s, c))

    def test_protocol_mimic(self):
        a_s = Socket   ("sock_a", "esdoc.tweet")
        b_c = Connector("conn_b", "esdoc")
        b_s = Socket   ("sock_b", "esdoc", mimic=b_c)  # Should end up mimicing 'esdoc.tweet' from a_s if connected
        c_c = Connector("conn_c", "esdoc.tweet")

        # Only unidirectional attachment needed for this test
        b_c.attach(a_s)

        print "b_s proto         =", b_s.protocol
        print "b_s mimiced proto =", b_s.mimiced_protocol
        comply = Terminal.protocol_compliance(b_s, c_c)
        print "compiance=", comply

        self.assertTrue(b_s.mimiced_protocol == "esdoc.tweet")

        self.assertTrue(Terminal.protocol_compliance(a_s, b_c))
        self.assertTrue(Terminal.protocol_compliance(b_s, c_c))

    def test_protocol_mimic_no_connection(self):
        a_s = Socket   ("sock_a", "esdoc.tweet")
        b_c = Connector("conn_b", "esdoc")
        b_s = Socket   ("sock_b", "esdoc", mimic=b_c)  # Should end up mimicing 'esdoc.tweet' from a_s if connected
        c_c = Connector("conn_c", "esdoc.tweet")

        print "b_s proto         =", b_s.protocol
        print "b_s mimiced proto =", b_s.mimiced_protocol
        comply = Terminal.protocol_compliance(b_s, c_c)
        print "compiance=", comply

        self.assertTrue(b_s.mimiced_protocol == "esdoc")

        self.assertTrue(Terminal.protocol_compliance(a_s, b_c))
        self.assertFalse(Terminal.protocol_compliance(b_s, c_c))

    def test_protocol_mimic_sequence(self):
        a_s = Socket   ("sock_a", "esdoc.tweet")

        b_c = Connector("conn_b", "esdoc")
        b_s = Socket   ("sock_b", "esdoc", mimic=b_c)

        c_c = Connector("conn_c", "esdoc.tweet")
        c_s = Socket   ("sock_b", "esdoc", mimic=c_c)

        print "NOT ATTACHED:"
        print "b_s         proto =", b_s.protocol
        print "c_s         proto =", b_s.protocol
        print "b_s mimiced proto =", c_s.mimiced_protocol
        print "c_s mimiced proto =", c_s.mimiced_protocol

        self.assertTrue(c_s.mimiced_protocol == "esdoc")

        # Only unidirectional attachments needed for this test
        b_c.attach(a_s)
        c_c.attach(b_s)

        print "\nATTACHED:"
        print "b_s         proto =", b_s.protocol
        print "c_s         proto =", c_s.protocol
        print "b_s mimiced proto =", b_s.mimiced_protocol
        print "c_s mimiced proto =", c_s.mimiced_protocol

        self.assertTrue(c_s.mimiced_protocol == "esdoc.tweet")

    def test_protocol_mimic_circular(self):
        a_s = Socket   ("sock_a", "esdoc.tweet")

        b_c = Connector("conn_b", "esdoc")
        b_s = Socket   ("sock_b", "esdoc", mimic=b_c)

        c_c = Connector("conn_c", "esdoc.tweet")
        c_s = Socket   ("sock_b", "esdoc", mimic=c_c)

        # Only unidirectional attachments needed for this test
        b_c.attach(c_s) # Making it circular
        c_c.attach(b_s)

        print "\nATTACHED:"
        print "b_s         proto =", b_s.protocol
        print "c_s         proto =", c_s.protocol
        print "b_s mimiced proto =", b_s.mimiced_protocol
        print "c_s mimiced proto =", c_s.mimiced_protocol

        self.assertTrue(b_s.mimiced_protocol == "esdoc")

        # And most important, it does not enter an infinite loop and finally gets here..

def main():
    unittest.main()

if __name__ == "__main__":
    main()
