import unittest, json
from eslib.procs.JSONArraysToURLRequest import JSONArraysToURLRequest

class TestJSONArraysToURLRequest(unittest.TestCase):
    obj = {}
    item1 = "test1"
    list1 = ["one", "two", "three"]
    item_without_list = "test2"
    not_list = "this should be ignored"
    obj[item1] = list1
    obj[item_without_list] = not_list
    who = "who"
    what = "what"

    def setUp(self):
        self.proc = JSONArraysToURLRequest()
        self.proc.config.who = self.who
        self.proc.config.what = self.what

    def test_handle(self):
        prepend = "not "
        input = "this"
        # Will not modify input if "prepend" is not set
        actual = self.proc._handle(input)
        expected = self._create_request(input)
        self.assertEquals(actual, expected)
        # Will prepend input if "prepend" is set
        self.proc.config.prepend = prepend
        actual = self.proc._handle(input)
        expected = self._create_request(prepend + input)
        self.assertEquals(actual, expected)

    def test_handle_all(self):
        processed_list = []
        for doc in self.proc._handle_all(self.obj):
            processed_list.append(doc)
        expected = len(self.list1) # We expect that the processor will ignore the non list values
        actual = len(processed_list)
        self.assertEqual(actual, expected)

    def _create_request(self, url):
        return {"url": url, "what": self.what, "who": self.who}
