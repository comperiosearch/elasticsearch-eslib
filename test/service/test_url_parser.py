# -*- coding: utf-8 -*-

import unittest
from eslib.service.UrlParamParser import UrlParamParser

class TestUrlParamParser_Path(unittest.TestCase):

    def test_one(self):
        path_spec = "a/{arg0}"
        url = "http://blabla/a/val0"
        parser = UrlParamParser(path_specification=path_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], "val0")

    def test_one_plus_many_open(self):
        path_spec = "a/{arg0}/{*arg1}"
        url = "http://blabla/a/val0/val1/val2/val3"
        parser = UrlParamParser(path_specification=path_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], "val0")
        self.assertEqual(parsed["arg1"], ["val1", "val2", "val3"])

    def test_one_plus_many_closed(self):
        path_spec = "a/{arg0}/{*arg1}/b"
        url = "http://blabla/a/val0/val1/val2/val3/b"
        parser = UrlParamParser(path_specification=path_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], "val0")
        self.assertEqual(parsed["arg1"], ["val1", "val2", "val3"])

    def test_optional_closed(self):
        path_spec = "a/{?arg0}/b"
        url1 = "http://blabla/a/val0/b"
        url2 = "http://blabla/a/b"
        parser = UrlParamParser(path_specification=path_spec)
        parsed1 = parser.parse(url1)
        print parsed1
        parsed2 = parser.parse(url2)
        print parsed2
        self.assertEqual(parsed1["arg0"], "val0")
        self.assertEqual(parsed2["arg0"], None)

    def test_optional_open(self):
        path_spec = "a/{?arg0}"
        url1 = "http://blabla/a/val0"
        url2 = "http://blabla/a"
        parser = UrlParamParser(path_specification=path_spec)
        parsed1 = parser.parse(url1)
        print parsed1
        parsed2 = parser.parse(url2)
        print parsed2
        self.assertEqual(parsed1["arg0"], "val0")
        self.assertEqual(parsed2["arg0"], None)

    def test_ambiguous(self):
        path_spec = "a/{?arg0}/{?arg1}"
        self.assertRaises(ValueError, lambda: UrlParamParser(path_specification=path_spec))

    def test_unambiguous1(self):
        path_spec = "a/{?arg0}/b/{?arg1}"
        url = "http://blabla/a/b/val1"
        parser = UrlParamParser(path_specification=path_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], None)
        self.assertEqual(parsed["arg1"], "val1")

    def test_unambiguous2(self):
        path_spec = "a/{?arg0}/b/{arg1}"
        url = "http://blabla/a/b/val1"
        parser = UrlParamParser(path_specification=path_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], None)
        self.assertEqual(parsed["arg1"], "val1")

    def test_types_ok(self):
        path_spec = "a/{arg0:int}/{arg1:float}"
        url = "http://blabla/a/1/3.4"
        parser = UrlParamParser(path_specification=path_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], 1)
        self.assertEqual(parsed["arg1"], 3.4)
        self.assertEqual(type(parsed["arg0"]), int)
        self.assertEqual(type(parsed["arg1"]), float)

    def test_types_error(self):
        path_spec = "a/{arg0:int}/{arg1:float}"
        url = "http://blabla/a/1.2/balle"
        parser = UrlParamParser(path_specification=path_spec)
        self.assertRaises(ValueError, lambda: parser.parse(url))

    def test_path_resolving(self):
        path_specs = ["a/{arg0}/b", "{arg0}/c", "a/{*arg0}"]
        pp = [UrlParamParser(ps) for ps in path_specs]
        urls = ["due/c", "a/tre", "a/uno/b"]
        ok = []
        for url in urls:
            for p in pp:
                res = p.parse(url)
                if res is not None:
                    ok.append((url, p, res))
                    print "OK:", url, p, res
        self.assertEqual(len(ok), 4)
        self.assertEqual(ok[0][0], "due/c")
        self.assertEqual(ok[0][1].path, "{arg0}/c")
        self.assertEqual(ok[0][2], {'arg0': 'due'})
        self.assertEqual(ok[1][0], "a/tre")
        self.assertEqual(ok[1][1].path, "a/{*arg0}")
        self.assertEqual(ok[1][2], {'arg0': ['tre']})
        self.assertEqual(ok[2][0], "a/uno/b")
        self.assertEqual(ok[2][1].path, "a/{arg0}/b")
        self.assertEqual(ok[2][2], {'arg0': 'uno'})
        self.assertEqual(ok[3][0], "a/uno/b")
        self.assertEqual(ok[3][1].path, "a/{*arg0}")
        self.assertEqual(ok[3][2], {'arg0': ['uno', 'b']})

class TestUrlParamParser_Query(unittest.TestCase):

    def test_singular(self):
        path_spec = "a"
        param_spec = ["arg0", "arg1", "arg2", "arg3"]
        url = "http://blabla/a?arg0=val0&arg1=val1&arg2=&arg4=val4"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], "val0")
        self.assertEqual(parsed["arg1"], "val1")
        self.assertEqual(parsed["arg2"], None)
        self.assertEqual(parsed["arg3"], None)
        self.assertFalse("arg4" in parsed)

    def test_required(self):
        path_spec = "a"
        param_spec = ["arg0", "arg1", "arg2", "+arg3"]
        url = "http://blabla/a?arg0=val0&arg1=val1&arg2=&arg4=val4"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        self.assertRaises(AttributeError, lambda: parser.parse(url))

    def test_multi_value(self):
        path_spec = "a"
        param_spec = ["arg0", "*arg1"]
        url = "http://blabla/a?arg0=val0&arg1=val1&arg0=val2&arg1=val3"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], "val2")  # Last value overrides this singular
        self.assertEqual(parsed["arg1"], ["val1", "val3"])

    def test_types_ok(self):
        path_spec = "a"
        param_spec = ["arg0:int", "*arg1:float"]
        url = "http://blabla/a?arg0=1&arg1=2.3&arg1=4.5"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], 1)
        self.assertEqual(type(parsed["arg0"]), int)
        self.assertEqual(parsed["arg1"], [2.3, 4.5])

    def test_types_error(self):
        path_spec = "a"
        param_spec = ["arg0:int", "*arg1:float"]
        url = "http://blabla/a?arg0=1.2&arg1=2.3&arg1=balle"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        self.assertRaises(ValueError, lambda: parser.parse(url))

class TestUrlParamParser_Combined(unittest.TestCase):

    def test_singular(self):
        path_spec = "a/{arg0}/b/{arg1}/c/{arg2}"
        param_spec = ["arg0", "arg1", "+arg2"]
        url = "http://blabla/a/p0/b/p1/c/p2?arg0=q0&arg1=q1"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], "q0")
        self.assertEqual(parsed["arg1"], "q1")
        self.assertEqual(parsed["arg2"], "p2")

    def test_multi_mixed_ok(self):
        path_spec = "a/{arg0}/b/{*arg1}/c/{*arg2}"
        param_spec = ["*arg0", "*arg1", "*arg2"]
        url = "http://blabla/a/p00/b/p10/p11/p12/c/p20?arg0=q00&arg1=q10&arg1=q11&arg2=q20&arg2=q21"
        parser = UrlParamParser(path_specification=path_spec, param_specifications=param_spec)
        parsed = parser.parse(url)
        print parsed
        self.assertEqual(parsed["arg0"], ["p00", "q00"])
        self.assertEqual(parsed["arg1"], ["p10", "p11", "p12", "q10", "q11"])
        self.assertEqual(parsed["arg2"], ["p20", "q20", "q21"])

    def test_multi_mixed_error(self):
        path_spec = "a/{arg0}/b/{*arg1}/c/{*arg2}"
        param_spec = ["*arg0", "arg1", "*arg2"]
        self.assertRaises(ValueError, lambda: UrlParamParser(path_specification=path_spec, param_specifications=param_spec))

    def test_type_mismatch(self):
        path_spec = "a/{arg0:int}/b/{arg1:float}"
        param_spec = ["arg0:float", "arg1:int"]
        self.assertRaises(ValueError, lambda: UrlParamParser(path_specification=path_spec, param_specifications=param_spec))

def main():
    unittest.main()

if __name__ == "__main__":
    main()



# TODO:
# [ ] DESERIALIZE
# [ ] CREATE TESTS
