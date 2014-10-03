import unittest
from eslib.procs import FileWriter
from eslib.procs import WebGetter


import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


res = []

class TestWebGetter(unittest.TestCase):

    def test_incoming_reg(self):
        domains = [
            {
                "domain_id"  : "Comperio",
                "url_prefix" : "http://comperio.no",
                "rate_number": 5,
                "rate_window": 60,
                "ttl"        : 10

            },
            {
                "domain_id"  : "UNINETT",
                "url_prefix" : "http://uninett.no",
            }
        ]

        o = {
            "url"  : "http://comperio.no/balle",
            "what" : "balle_mon",
            "who"  : "balle_user"
        }

        o2 = {
            "url"  : "http://comperio.no/tryne",
            "what" : "balle_mon",
            "who"  : "balle_user"
        }

        m = WebGetter(domains=domains)
        m.start()

        m.put(o)
        m.put(o)
        m.put(o2)
        m.get_ready()

        try:
            m.wait()
        except KeyboardInterrupt:
            print "*** KEYBOARD INTERRUPT ***"
            m.stop()
            m.wait()

def main():
    unittest.main()

if __name__ == "__main__":
    main()
