# -*- coding: utf-8 -*-

import unittest
from eslib.procs import PatternRemover
from eslib import esdoc

class TestPatternRemover(unittest.TestCase):

    def test_str(self):
        dirty = u"Oh my fucking god…"

        p = PatternRemover(patterns=["my", u"\S+…"])
        p.on_open()  # Force generation of internal regexes
        cleaned = p._clean(dirty)
        print "D=", dirty
        print "C=", cleaned

        self.assertTrue(cleaned == "Oh fucking")

    def test_field(self):
        dirty_text = u"Oh my fucking god…"

        dirty = {
            "_id": "somedoc",
            "_source": {
                "text": dirty_text
            }
        }

        p = PatternRemover(patterns=["my", u"\S+…"], target_field="cleaned")
        p.on_open()  # Force generation of internal regexes
        cleaned = p._clean(dirty)
        print "D=", esdoc.getfield(cleaned, "_source.text")
        print "C=", esdoc.getfield(cleaned, "_source.cleaned")

        self.assertTrue(esdoc.getfield(cleaned, "_source.text"   ) == dirty_text)
        self.assertTrue(esdoc.getfield(cleaned, "_source.cleaned") == "Oh fucking")

    def test_field_map(self):
        dirty = {
            "_id": "somedoc",
            "_source": {
                "A": "This was A",
                "b": { "B": "This was B"}
            }
        }

        p = PatternRemover(pattern="was", field_map={"A": "cleaned.cleaned_A", "b.B": "cleaned.cleaned_B"})
        p.on_open()  # Force generation of internal regexes
        cleaned = p._clean(dirty)

        self.assertTrue(esdoc.getfield(cleaned, "_source.cleaned.cleaned_A") == "This A")
        self.assertTrue(esdoc.getfield(cleaned, "_source.cleaned.cleaned_B") == "This B")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
