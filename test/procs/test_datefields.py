__author__ = 'mats'

import datetime
import unittest
import dateutil.parser as parser

from eslib.procs import DateFields

ok_date = '2014-10-14T14:26:30+01:00'
ok_date_no_tz = '2014-10-14T14:26:30'
wrong_date = 2013
wrong_date2 = '-120-13-142T25:61:61+30:00'

dict_wo_source = {'i': {'am': {'a': 'dict'}}}
dict_w_source = {'_source': dict_wo_source}
dict_wo_sourcefield = {'_source': dict_wo_source}
dict_w_sourcefield = {'_source': {'created_at': dict_wo_source}}
dict_w_ok_date = {'_source': {'created_at': ok_date}}
dict_wo_ok_date = {'_source': {'created_at': wrong_date}}
dict_wo_ok_date2 = {'_source': {'created_at': wrong_date2}}


class TestDateFields(unittest.TestCase):

    def setUp(self):
        self.date_fields = DateFields()

    def test_incoming(self):
        # if the dict doesn't have source it should be returned
        doc = self.date_fields._incoming(dict_wo_source)
        self.assertDictEqual(doc, dict_wo_source)

        # if the dict has source, but no sourcefield, it
        # should be returned
        doc = self.date_fields._incoming(dict_wo_sourcefield)
        self.assertDictEqual(doc, dict_wo_sourcefield)

        # if the date is invalid, the same doc should be returned
        doc = self.date_fields._incoming(dict_wo_ok_date)
        self.assertDictEqual(doc, dict_wo_ok_date)

        doc = self.date_fields._incoming(dict_w_ok_date)
        self.assertIn('date_fields', doc)

        doc = self.date_fields._incoming(dict_w_ok_date)
        self.assertEqual(doc, dict_w_ok_date)


    def test_ensure_date(self):
        # an ok date string should return an ok date
        date = self.date_fields.ensure_date(ok_date)
        self.assertIsInstance(date, datetime.datetime)

        # an ok date object should return an ok date object
        parsed = parser.parse(ok_date)
        date = self.date_fields.ensure_date(parsed)
        self.assertEqual(date, parsed)

        # a wrong date string should raise ValueError
        self.assertRaises(ValueError,
                          self.date_fields.ensure_date,
                          wrong_date)

        self.assertRaises(ValueError,
                          self.date_fields.ensure_date,
                          wrong_date2)

    def test_date_fields(self):
        # an object which is not a datetime object should raise AttributeError
        self.assertRaises(AttributeError,
                          self.date_fields.date_fields,
                          ok_date)

        # an datetime object should return the correct values.
        parsed = parser.parse(ok_date)
        returned = self.date_fields.date_fields(parsed)
        self.assertEqual(parsed.year, returned['year'])
        self.assertEqual(parsed.month, returned['month'])
        self.assertEqual(parsed.day, returned['day'])
        self.assertEqual(parsed.hour, returned['hour'])
        self.assertEqual(parsed.minute, returned['minute'])
        self.assertEqual(parsed.second, returned['second'])
        self.assertEqual(parsed.weekday(), returned['day_of_the_week'])
        # Hard coded utc_offset. Oh well.
        self.assertEqual(1, returned['utc_offset'])
