__author__ = 'Hans Terje Bakke'

from ..Generator import Generator

# TODO: READ AND HONOUR robots.txt FROM TARGET LIST

# TODO: ADD METHODS TO ADD/REMOVE DOMAIN WATCH WHILE RUNNING

# TODO: SHOULD WE BE CASE SENSITIVE OR NOT? (SENSITIVE FOR NOW)


class DomainEntry(object):
    def __init__(self):
        self.domain_id        = None
        self.url_prefix       = None
        self.rate_number      = 0     # Allowed number of requests...
        self.rate_per_seconds = 0     # ...per this number of seconds.
        self.ttl              = 0     # Time (in seconds) before we ought to refetch a URL in this domain again,
                                      #   if one was requested.
        self.last_fetch_time  = None  # Time when this domain last had a URL fetched.
        self.deny_until_time  = None  # Time when it is again possible to fetch a URL from this domain.
        self.total_count      = 0     # How many times this domain has had URLs requested fetched, in total.
        self.fetch_count      = 0     # How many times URL in this domain has been fetched.
        self.pending_count    = 0     # A kind of "pressure" on the domain.
        self.url_infos        = {}    # Info about URLs fetched or queued for for this domain {url: UrlInfo}.
        # TODO: robots.txt info that we should honour

    def add(self, url, what, who):
        "Note: URL has already been checked to comply with the url_prefix."
        info = self.url_infos.get("url")
        if not info:
            info = UrlInfo(self.domain_id, url)
            self.url_infos.update({url: info})
        info.total_count   += 1
        info.pending_count += 1
        who_list = info.requested_by.get(what)
        if who_list:
            if not who in who_list:
                who_list.append(who)
        else:
            info.requested_by.update({what: [ who ]})

    def get_ready(self):
        """
        Will return all URLs that are ready to be fetched, and update their timing data,
        AS IF they were fetched just now.
        """
        if not self.pending_count:
            return []

        # TODO ========== WAS HERE ===========

class UrlInfo(object):
    def __init__(self, domain_id, url):
        self.url              = url
        self.domain_id        = domain_id
        self.last_fetch_time  = None
        self.total_count      = 0     # How many times this URL has been requested fetched, in total
        self.fetch_count      = 0     # How many times this URL has been fetched
        self.pending_count    = 0     # A kind of "pressure" on the URL
        self.requested_by     = {}    # Of elements { what: [ who, ... ] }

class WebGetter(Generator):
    """
    Receive URLs and other info as input and gracefully fetch the web page without exceeding configured
    rate limits.

    Config:

        What web domain prefixes to monitor are given in the config variable 'domains', which is a dict type with
        the following entry format, of which only the url_prefix is required:

        {
            "domain_id"        : "(a nice name for this entry)",
            "url_prefix"       : "(url prefix)",
            "rate_number"      : (allowed number or requests...),
            "rate_per_seconds" : (...per this number of seconds),
            "ttl"              : (time to live in seconds, before we can ask for this url again)
        }

    Input:

        Potential URL requests are matched with url_prefixes and marked as pending if it is something configured
        to be fetched. The format of an input message of protocol "urlrequest" is:

        {
            "url"  : ""
            "what" : "(e.g. twitter_mon)",
            "who"  : "(e.g. some_user_id)"
        }

    Output:

# TODO: should this be of 'esdoc' format instead?

        The protocol "webpage" is a dictionary with the following fields:

        {
            "domain_id"     : "",
            "url_prefix"    : "",
            "url"           : "",
            "timestamp"     : "",
            "requested_by": [ { "what": "(e.g. twitter_mon)" ["(who)", ...] }, ... ],
# TODO: content type, etc
            "content"       : ""
        }

    The requested_by is the total unique list of requesters that we know about so far (since this processor
    was started).

    If the processor still has pending fetch requests when it receives a stop() signal, it will keep running until
    all pending requests have been served. This may take a while, depending on the queue size and/or rate limits.

    Connectors:
        input      (urlrequest)      : Request for a URL with info about what/who is requesting it.
    Sockets:
        output     (webpage)         : Documents containing the web page content fetched from a requested URL.

    Config:
        domains           = {}       : A dict of domains to fetch urls from upon request.
    """

    def __init__(self, name=None):
        super(WebGetter, self).__init__(name)
        self.create_connector(self._incoming, "input", "urlrequest", "Request for a URL with info about what/who is requesting it.")
        self.output = self.create_socket("output", "webpage", "Documents containing the web page content fetched from a requested URL.")

        self.config.domains = {}

        self._domains = []

    def on_startup(self):
        pass  # TODO: CREATE self._domains FROM self.config.domains

    def _incoming(self, document):
        if not type(document) is dict or not "url" in document:
            self.doclog.warning("Expected dict type document with at least a 'url' attribute.")
            return

        url = document.get("url")
        if not url:
            self.doclog.warning("'url' attribute was empty.")
            return

        domain = None
        for d in self._domains:
            if url.startswith(d.url_prefix):
                domain = d
                break
        if not domain:
            return  # Incoming URL request was not in a monitored domain

        what = document.get("what")
        who = document.get("who")

        d.add(url, what, who)

# TODO: Generator implementation, looping through the domains and fetching when required
