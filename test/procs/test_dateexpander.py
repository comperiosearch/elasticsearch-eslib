__author__ = 'Mats Julian Olsen'

import unittest

from eslib import time
from eslib.procs import DateExpander

ok_date = '2014-10-14T14:26:30+01:00'
ok_date_no_tz = '2014-10-14T14:26:30'
wrong_date = 2013
wrong_date2 = '-120-13-142T25:61:61+30:00'

ok_date_fields = {'week': 42, 'second': 30, 'weekday': 2, 'hour': 13, 'year': 2014, 'day': 14, 'minute': 26, 'month': 10}

dict_wo_source = {'i': {'am': {'a': 'dict'}}}
dict_w_source = {'_source': dict_wo_source}
dict_wo_sourcefield = {'_source': dict_wo_source}
dict_w_sourcefield = {'_source': {'created_at': dict_wo_source}}
dict_w_ok_date = {'_source': {'created_at': ok_date, "date_fields": ok_date_fields}}
dict_wo_ok_date = {'_source': {'created_at': wrong_date}}
dict_wo_ok_date2 = {'_source': {'created_at': wrong_date2}}


class TestDateMagic(unittest.TestCase):

    def test_all(self):
        date = time.utcdate(ok_date)
        dd = time.date_dict(date)
        print dd
        self.assertEqual(dd, ok_date_fields)


class TestDateFields(unittest.TestCase):

    def setUp(self):
        self.expander = DateExpander()

    def test_missing_source_section(self):
        # if the dict doesn't have source it should be returned
        doc = self.expander._process(dict_wo_source)
        print doc
        self.assertDictEqual(doc, dict_wo_source)

    def test_missing_source_field(self):
        # if the dict has source, but no source_field, it should be returned
        doc = self.expander._process(dict_wo_sourcefield)
        print doc
        self.assertDictEqual(doc, dict_wo_sourcefield)

    def test_invalid_date(self):
        # if the date is invalid, the same doc should be returned
        doc = self.expander._process(dict_wo_ok_date)
        print doc
        self.assertDictEqual(doc, dict_wo_ok_date)

    def test_valid_date(self):
        doc = self.expander._process(dict_w_ok_date)
        print doc
        self.assertIn('date_fields', doc["_source"])

        doc = self.expander._process(dict_w_ok_date)
        print doc
        self.assertEqual(doc, dict_w_ok_date)
