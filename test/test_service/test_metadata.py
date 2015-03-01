# -*- coding: utf-8 -*-

import unittest
import json
from eslib import esdoc
from eslib.service import dicthelp


def dump(data):
    print json.dumps(data, indent=2)


class TestMetadata_put(unittest.TestCase):

    def test_set_overwrite(self):
        orig = {"a": {"b": "c", "d": "e"}}
        dicthelp.put(orig, "a.b", "new_item")
        dump(orig)

    def test_set_new(self):
        orig = {"a": {"b": "c", "d": "e"}}
        dicthelp.put(orig, "f.g", "new_item")
        dump(orig)
        self.assertEqual("new_item", esdoc.getfield(orig, "f.g"))
        self.assertNotEqual(None, esdoc.getfield(orig, "a.b"))

    def test_set_insert_into_list(self):
        orig = {"a": ["b"]}
        dicthelp.put(orig, "a", "new_item")
        dump(orig)
        self.assertEqual(["b", "new_item"], esdoc.getfield(orig, "a"))

    def test_set_insert_into_list_constraint_failing(self):
        orig = {"a": [{"name":"b", "id":1}, {"name":"c", "id":2}]}
        dicthelp.put(orig, "a|id:3", "new_item")
        dump(orig)
        self.assertEqual(3, len(esdoc.getfield(orig, "a")))
        self.assertEqual("new_item", esdoc.getfield(orig, "a")[2])

    def test_set_insert_into_list_constraint_matching(self):
        orig = {"a": [{"name":"b", "id":1}, {"name":"c", "id":2}]}
        dicthelp.put(orig, "a|id:1", "new_item")
        dump(orig)
        self.assertEqual(2, len(esdoc.getfield(orig, "a")))
        self.assertEqual("new_item", esdoc.getfield(orig, "a")[0])

    def test_set_inset_into_list_overwrite_existing(self):
        orig = {"a": ["x", "y", "z"]}
        dicthelp.put(orig, "a", "y")
        dump(orig)
        self.assertEqual(["x", "y", "z"], esdoc.getfield(orig, "a"))

    def test_set_add_list_replace_list(self):
        orig = {"a": ["x", "y", "z"]}
        dicthelp.put(orig, "a", [1, 2, 3])
        dump(orig)
        self.assertEqual([1, 2, 3], esdoc.getfield(orig, "a"))

    def test_set_add_list_overwrite_list(self):
        orig = {"a": ["x", "y", "z"]}
        dicthelp.put(orig, "a", [1, "y", 3], merge_lists=True)
        dump(orig)
        self.assertEqual(["x", "y", "z", 1, 3], esdoc.getfield(orig, "a"))

class TestMetadata_delete(unittest.TestCase):

    def test_delete_from_empty(self):
        orig = {}
        dicthelp.delete(orig, ["a"])
        dump(orig)

    def test_delete_non_existing(self):
        orig = {"a":None}
        dicthelp.delete(orig, ["b.c"])
        dump(orig)

    def test_delete_keep_parent_tree(self):
        orig = {"a":"a_val", "b": {"c": "c_val", "d": {"e": "e_val"}}}
        dicthelp.delete(orig, ["b.d.e"])
        dump(orig)
        self.assertEqual("c_val", esdoc.getfield(orig, "b.c"))
        self.assertEqual({}, esdoc.getfield(orig, "b.d"))

    def test_delete_parent_not_empty(self):
        orig = {"a":"a_val", "b": {"c": "c_val", "d": "d_val"}}
        dicthelp.delete(orig, ["b.d"])
        dump(orig)
        self.assertEqual("c_val", esdoc.getfield(orig, "b.c"))
        self.assertEqual(None, esdoc.getfield(orig, "b.d"))

    def test_delete_parent_empty_no_collapse(self):
        orig = {"a":"a_val", "b": {"d": "d_val"}}
        dicthelp.delete(orig, ["b.d"])
        dump(orig)
        self.assertEqual(None, esdoc.getfield(orig, "b.d"))
        self.assertEqual({}, esdoc.getfield(orig, "b"))

    def test_delete_parent_empty_collapse(self):
        orig = {"a":"a_val", "b": {"d": "d_val"}}
        dicthelp.delete(orig, ["b.d"], collapse=True)
        dump(orig)
        self.assertEqual(None, esdoc.getfield(orig, "b.d"))
        self.assertEqual(None, esdoc.getfield(orig, "b"))
        self.assertEqual("a_val", esdoc.getfield(orig, "a"))

    def test_delete_constrained_list_item_no_list(self):
        orig = {"a":None, "b": {"c": {"x":"y"}}}
        dicthelp.delete(orig, ["b.c.x|domain:db|id:5"])
        dump(orig)

    def test_delete_constrained_list_item(self):
        orig = {"a":None, "b": {"c": {"x":["a", {"domain":"db", "id":4}, {"domain":"db", "id":5}, "b"]}}}
        dicthelp.delete(orig, ["b.c.x|domain:db|id:5"])
        dump(orig)
        self.assertEqual(3, len(esdoc.getfield(orig, "b.c.x")))
        self.assertEqual(4, esdoc.getfield(orig, "b.c.x")[1]["id"])

class TestMetadata_remove(unittest.TestCase):

    def test_remove_list_items(self):
        orig = {"a": None, "b": ["1", "2", "3", "4"]}
        dicthelp.remove_list_items(orig, "b", ["2", "3", "5"])
        dump(orig)
        self.assertEqual(["1", "4"], esdoc.getfield(orig, "b"))


def main():
    unittest.main()

if __name__ == "__main__":
    main()
