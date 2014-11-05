# -*- coding: utf-8 -*-

import unittest
from eslib.procs import BlacklistFilter

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class TestBlacklistFilter_str(unittest.TestCase):

    def test_str_nohit(self):
        s = "I am marvellous"
        p = BlacklistFilter(filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}])
        p.on_open()
        check = p._check(s)

        print "str_nohit (exp:True)=", check
        self.assertTrue(check)

    def test_str_hit_but_not_blacklisted(self):
        s = "I like girls."
        p = BlacklistFilter(filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}])
        print "filters=", p._filters
        p.on_open()
        check = p._check(s)

        print "str_hit_but_not_blacklisted (exp:True)=", check
        self.assertTrue(check)

    def test_str_hit_and_blacklisted(self):
        s = "I like young girls."
        p = BlacklistFilter(filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}])
        print "filters=", p._filters
        p.on_open()
        check = p._check(s)

        print "str_hit_and_blacklisted (exp:False)=", check  # Should have hit "young" from blacklist
        self.assertFalse(check)

    def test_str_global_whitelist_override(self):
        s = "We only like girls. Young girls are always welcome!"
        p = BlacklistFilter(filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}], whitelist=["young girls"])
        p.on_open()
        check = p._check(s)

        print "str_global_whitelist_override (exp:True)=", check
        # Should have hit "young" from blacklist, but "young girls" from whitelist should override it
        self.assertTrue(check)


    def test_brooklyn(self):
        s = "Brooklyn Nets trounce short-handed Oklahoma City Thunder 116-85 http://t.co/qJZPBEJRCT"
        p = BlacklistFilter(filters=[{"tokens": ["nets"], "blacklist": ["brooklyn"]}])
        p.on_open()
        check = p._check(s)

        print "check (expect False)=", check
        self.assertFalse(check)


class TestBlacklistFilter_esdoc(unittest.TestCase):

    def test_str_nohit(self):
        s = "I am marvellous"
        doc = {"_source": {"field1": s}}
        p = BlacklistFilter(
            field="field1",
            filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}])
        p.on_open()
        check = p._check(doc)

        print "str_nohit (exp:False)=", check
        self.assertFalse(check)

    def test_str_hit_but_not_blacklisted(self):
        s = "I like girls."
        doc = {"_source": {"field1": s}}
        p = BlacklistFilter(
            fields=["field1", "field2"],
            filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}])
        print "filters=", p._filters
        p.on_open()
        check = p._check(doc)

        print "str_hit_but_not_blacklisted (exp:False)=", check
        self.assertFalse(check)

    def test_str_hit_and_blacklisted(self):
        s1 = "I like young girls."
        s2 = "I am a boy."
        doc = {"_source": {"field1": s1, "field2": s2}}
        p = BlacklistFilter(
            fields=["field1", "field2"],
            filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}])
        print "filters=", p._filters
        p.on_open()
        check = p._check(doc)

        print "str_hit_and_blacklisted (exp:False)=", check  # Should have hit "young" from blacklist
        self.assertFalse(check)

    def test_str_global_whitelist_override(self):
        s1 = "We only like girls. Young girls are always welcome!"
        s2 = "I like young boys."
        doc = {"_source": {"field1": s1, "field2": s2}}
        p = BlacklistFilter(
            fields=["field1", "field2"],
            filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}],
            whitelist=["young girls"])
        p.on_open()
        check = p._check(doc)

        print "str_global_whitelist_override (exp:True)=", check
        # Should have hit "young" from blacklist, but "young girls" from whitelist should override it
        self.assertTrue(check)

    def test_str_global_whitelist_override_not_hitting(self):
        s1 = "We only like girls. Young girls are always welcome!"
        s2 = "I like young boys."
        doc = {"_source": {"field1": s1, "field2": s2}}
        p = BlacklistFilter(
            fields=["field2"],
            filters=[{"tokens": ["we", "like"], "blacklist": ["young"]}],
            whitelist=["young girls"])
        p.on_open()
        check = p._check(doc)

        print "str_global_whitelist_override_not_hitting (exp:False)=", check
        # Should have hit "young" from blacklist; "young girls" from whitelist does not apply to field2, so we should not override here
        self.assertFalse(check)

def main():
    unittest.main()

if __name__ == "__main__":
    main()
