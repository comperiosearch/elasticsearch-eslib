# -*- coding: utf-8 -*-

import unittest
from eslib.procs import HtmlRemover

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class TestHtmlRemover(unittest.TestCase):

    def test_all(self):
        dirty = '<a href="http://blabla.com/bla">Lady &amp; Landstrykeren</a>'

        p = HtmlRemover()
        cleaned = p._clean(dirty)
        print "D=", dirty
        print "C=", cleaned

        self.assertTrue(cleaned == "Lady & Landstrykeren")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
