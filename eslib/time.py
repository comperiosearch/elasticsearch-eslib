# -*- coding: utf-8 -*-

"""
eslib.time
~~~~~~~~~~

Module containing time/date helpers.
"""


__all__ = ("duration_string", "date2iso", "ago2date")


import re, datetime


def duration_string(timediff):
    """
    :type timediff: datetime.timedelta
    :rtype str:
    """
    secs = timediff.seconds
    days = timediff.days
    s = secs % 60
    m = (secs / 60) % 60
    h = (secs / 60 / 60) % 24
    return "%d:%02d:%02d" % (days*24+h, m, s)


def date2iso(dateobj):
    """
    Convert datetime object to ISO 8601 string with UTC, e.g. '2014-03-10T23:32:47Z'
    :type dateobj: datetime.datetime
    :rtype str
    """
    return dateobj.strftime("%Y-%m-%dT%H:%M:%SZ") # Screw the %.f ...

def iso2date(isostr):
    """
    Convert ISO 8601 string in UTC, e.g. '2014-03-10T23.32:47Z' to datetime object.
    :type isostr: datetime.datetime
    :rtype datetime.datetime
    """
    if isostr is None:
        return None
    if "." in isostr:
        return datetime.datetime.strptime(isostr, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        return datetime.datetime.strptime(isostr, "%Y-%m-%dT%H:%M:%SZ")


_agoRegex = re.compile("^(?P<number>\d)+\s*(?P<unit>\w+)( ago)?$")

def ago2date(ago, from_date_utc=None):
    """
    Convert 'ago' style time specification string to a datetime object.
    Units are s=second, m=minute, h=hour, d=day, w=week, M=month, y=year
    :param str ago                         : "Time ago" as a string.
    :param datetime.datetime from_date_utc : Relative time to use instead of 'now'. In UTC.
    :rtype datetime.timedelta              : Time difference.
    """
    m = _agoRegex.match(ago)
    if not m:
        raise SyntaxError("Illegal 'ago' string: %s" % ago)
    number = int(m.group("number"))
    unit = m.group("unit")
    delta = None
    if   unit == "s" or unit.startswith("sec")  : delta = datetime.timedelta(seconds= number)
    elif unit == "m" or unit.startswith("min")  : delta = datetime.timedelta(minutes= number)
    elif unit == "h" or unit.startswith("hour") : delta = datetime.timedelta(hours= number)
    elif unit == "d" or unit.startswith("day")  : delta = datetime.timedelta(days= number)
    elif unit == "w" or unit.startswith("week") : delta = datetime.timedelta(weeks= number)
    elif unit == "M" or unit.startswith("month"): delta = datetime.timedelta(days= number*30)
    elif unit == "y" or unit.startswith("year") : delta = datetime.timedelta(days= number*365)
    else:
        raise SyntaxError("Illegal unit for 'ago' string in: %s" % ago)
    return (from_date_utc or datetime.datetime.utcnow()) - delta;

def json_serializer_isodate(obj):
    """Default JSON serializer."""
    s = None
    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
        s = date2iso(obj)
    return s
