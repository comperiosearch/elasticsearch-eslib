__author__ = 'mats'

from ..Processor import Processor
from .. import esdoc

import datetime
import dateutil.parser as parser


class DateFields(Processor):
    """
    This processor will use a datefield in an esdoc as a basis for
    constructing an object with year, month, day, hour, minute and
    second fields. In addition, the following fields will also be included:
    day_of_the_week (1 through 7), week_number (1 through 53)

    """
    def __init__(self, **kwargs):
        super(DateFields, self).__init__(**kwargs)
        self._input = self.create_connector(self._incoming, 'esdoc', 'esdoc')
        self._output = self.create_socket('esdoc', 'esdoc')

        self.config.set_default(
            source_field='created_at',
            target_field='date_fields'
        )

    def _incoming(self, doc):
        self._output.send(self._process(doc))

    def _process(self, doc):
        try:
            source = doc['_source']
        except KeyError:
            self.doclog.warning('No source field in document: {}'
                                .format(doc.get('_id')))
            return doc

        try:
            field = source[self.config.source_field]
        except KeyError:
            self.doclog.warning('{} not in document'
                                .format(self.config.source_field))
            return doc

        try:
            date = self.ensure_date(field)
        except (ValueError, AttributeError):
            self.doclog.warning('{} in doc {} is not a datefield'
                                .format(self.config.source_field, doc))
            return doc

        try:
            date_dict = self.date_fields(date)
        except AttributeError as ae:
            self.doclog.error('date_fields failed unexpectedly '
                              'on doc {} with {}'
                              .format(doc.get('_id'), ae))
            return doc

        return esdoc.shallowputfield(doc,
                                     '_source.' + self.config.target_field,
                                     date_dict)


    @staticmethod
    def ensure_date(field):
        """
        Ensures that field is a datetime object. Will raise ValueError if not.

        :param field: a datestr or datetime object
        :raises: ValueError if date-parsing fails.
        :return: a datetime object.

        """
        try:
            return parser.parse(field)
        except AttributeError:
            return parser.parse(field.isoformat())

    @staticmethod
    def date_fields(date):
        """
        Will make a dictionary of all the different date methods and their vals.

        :param datetime.datetime date: a datetime object
        :return: dict.
        """
        if date.tzinfo:
            offset = date.tzinfo
            delta = offset.utcoffset(offset)
        else:
            delta = datetime.timedelta()

        utc_offset = DateFields.delta_in_hours(delta)
        return {"year": date.year, "month": date.month, "day": date.day,
                "hour": date.hour, "minute": date.minute, "second": date.second,
                "week_day": date.isoweekday(), "utc_offset": utc_offset,
                "week_number": date.isocalendar()[1]}

    @staticmethod
    def delta_in_hours(delta):
        """
        Takes an integer representing a number of seconds
        :param datetime.timedelta delta:
        :return: int hours:

        """
        hours = delta.seconds/(60*60)
        if hours > 0:
            return int(hours)
        else:
            return 0