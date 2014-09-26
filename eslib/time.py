# -*- coding: utf-8 -*-

"""
eslib.time
~~~~~~~~~~

Module containing time/date helpers.
"""


__all__ = ("durationString", "date2iso", "ago2date")


import re, datetime


def durationString(timediff):
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

def ago2date(agoStr):
    """
    Convert 'ago' style time specification string to a datetime object.
    Units are s=second, m=minute, h=hour, d=day, w=week, M=month, y=year
    :rtype datetime.timedelta:
    """
    m = _agoRegex.match(agoStr)
    if not m:
        raise SyntaxError("illegal 'ago' string: %s" % agoStr)
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
        raise SyntaxError("illegal unit for 'ago' string in: %s" % agoStr)
    return datetime.datetime.utcnow() - delta;
