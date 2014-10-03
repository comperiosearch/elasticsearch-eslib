__author__ = 'Hans Terje Bakke'

from ..Generator import Generator
from datetime import datetime, timedelta
from threading import Lock
import eslib.web

# TODO: READ AND HONOUR robots.txt FROM TARGET LIST

class Domain(object):
    def __init__(self, domain_id, url_prefix):
        # Config
        self.domain_id        = domain_id
        self.url_prefix       = url_prefix.lower()
        self.rate_number      = 0     # Allowed number of requests... (0 means infinite)
        self.rate_window      = 0     # ...per this number of seconds.
        self.ttl              = 0     # Time (in seconds) before we ought to refetch a URL in this domain again,
                                      #   if one was requested.
        # State
        self.last_fetch_time  = None  # Time when this domain last had a URL fetched.
        self.deny_until_time  = None  # Time when it is again possible to fetch a URL from this domain.
        self.total_count      = 0     # How many times this domain has had URLs requested fetched, in total.
        self.fetch_count      = 0     # How many times URLs in this domain has been fetched.
        self.pressure         = 0     # A kind of "pressure" on the domain. (Number of requests; not number of pending URLs)
        self.url_infos        = {}    # Info about URLs fetched or queued for for this domain {url: UrlInfo}.

        # TODO: robots.txt info that we should honour

        self._lock = Lock()
        self._flist           = []    # List of recent fetches within our allowance (will never exceed rate_number)

    def __str__(self):
        return self.domain_id

    def add(self, url, what, who):
        """
        Note: URL has already been checked to comply with the url_prefix.
        Note: URLs are case sensitive!
        """

        self._lock.acquire()

        info = self.url_infos.get(url)
        if not info:
            info = UrlInfo(self.domain_id, url)
            self.url_infos.update({url: info})
        info.total_count += 1
        info.pressure    += 1
        self.total_count += 1
        self.pressure    += 1
        who_list = info.requested_by.get(what)
        if who_list:
            if not who in who_list:
                who_list.append(who)
        else:
            info.requested_by.update({what: [ who ]})

        self._lock.release()

    def get_ready(self):
        """
        Will return all URLs that are ready to be fetched, and update their timing data,
        AS IF they were fetched just now.
        """
        if not self.pressure:
            return []

        now = datetime.utcnow()
        if self.deny_until_time and now < self.deny_until_time:
            return []  # Domain is not ready yet

        # Trim the list of recent fetches (flist) so that only those done within the allowance window (rate_per_seconds) remain
        ready = []
        if self.rate_number:
            threshold = now - timedelta(seconds=self.rate_window)
            while self._flist and self._flist[0].last_fetch_time < threshold:
                self._flist.remove(self._flist[0])

        self._lock.acquire()

        # Pick as many as we can until we fill up our rate_number within the current rate_per_seconds.
        for info in self.url_infos.itervalues():
            #print "*** trying:", info.url
            if not info.pressure:
                continue
            if self.rate_number and len(self._flist) >= self.rate_number:
                break  # We've reached our rate limit, so do not add any more items for now
            # Add the item if it is ready:
#            print "*** --------- %s ----------" % info.url
#            print "*** LAST  =", info.last_fetch_time
#            print "*** TTL   =", self.ttl
#            if info.last_fetch_time:
#                print "*** SSINCE=", (now - info.last_fetch_time).seconds
            if not info.last_fetch_time or (now - info.last_fetch_time).seconds > self.ttl:
                info.last_fetch_time = now  # We will get it now
                info.fetch_count += 1
                self.fetch_count += 1
                self.pressure -= info.pressure
                info.pressure = 0
                ready.append(info)
                if self.rate_number:
                    self._flist.append(info)

        self._lock.release()

        if ready:
            self.last_fetch_time = now

        # Set deny_until if we have exhausted our rate limit
        if self.rate_number and len(self._flist) >= self.rate_number:
            self.deny_until_time = self._flist[0].last_fetch_time + timedelta(seconds=self.rate_window)
        else:
            self.deny_until_time = None

        return ready


class UrlInfo(object):
    def __init__(self, domain_id, url):
        self.url              = url
        self.domain_id        = domain_id
        self.last_fetch_time  = None
        self.total_count      = 0     # How many times this URL has been requested fetched, in total
        self.fetch_count      = 0     # How many times this URL has been fetched
        self.pressure         = 0     # A kind of "pressure" on the URL; how many pending requests for this URL
        self.requested_by     = {}    # Of elements { what: [ who, ... ] }

    def __str__(self):
        return self.url

class WebGetter(Generator):
    """
    Receive URLs and other info as input and gracefully fetch the web page without exceeding configured
    rate limits.

    Config:

        What web domain prefixes to monitor are given in the config variable 'domains', which is a dict type with
        the following entry format, of which only the url_prefix is required:

        {
            "domain_id"     : "(a nice name for this entry)",
            "url_prefix"    : "(url prefix)",
            "rate_number"   : (allowed number or requests...),
            "rate_window"   : (...per this number of seconds),
            "ttl"           : (time to live in seconds, before we can ask for this url again)
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

    ALLOWED_SIZE = 1024*1024  # < 1 MB
    ALLOWED_CONTENT_TYPES = [ "text/html", "text/plain" ]

    def __init__(self, **kwargs):
        super(WebGetter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", "urlrequest", "Request for a URL with info about what/who is requesting it.")
        self.output = self.create_socket("output", "webpage", "Documents containing the web page content fetched from a requested URL.")

        self.config.set_default(
            domains = {}
        )

        self._domains = []
        self._lock = Lock()

        self._web_getter = eslib.web.WebGetter(WebGetter.ALLOWED_SIZE, WebGetter.ALLOWED_CONTENT_TYPES)


    #region Utility methods and properties

    def add_domain(self, domain_config):
        "Add a domain to the runtime list. OBS: Does not alter the processor's config regarding domains."

        if not type(domain_config) is dict:
            raise TypeError("'domain_config' should be a dict; was %s" % type(domain_config))
        domain_id  = domain_config.get("domain_id")
        url_prefix = domain_config.get("url_prefix")
        if not domain_id:
            raise AttributeError("'domain_config' missing mandatory 'domain_id'")
        if not url_prefix:
            raise AttributeError("'domain_config' missing mandatory 'url_prefix'")

        # In case it already exists...
        self.remove_domain(domain_id)

        # Create object
        domain = Domain(domain_id, url_prefix)
        domain.rate_number = domain_config.get("rate_number") or 0
        domain.rate_window = domain_config.get("rate_window") or 0
        domain.ttl         = domain_config.get("ttl")         or 0

        with self._lock:
            self._domains.append(domain)

    def remove_domain(self, domain_id):
        "Remove a domain from the runtime list."
        with self._lock:
            found = None
            for domain in self._domains:
                if domain.domain_id == domain_id:
                    found = domain
                    break
            if found:
                self._domains.remove(found)

    @property
    def num_domains(self):
        return len(self._domains)

    @property
    def num_urls(self):
        num = 0
        for domain in self._domains[:]:
            num += len(domain.url_infos)
        return num

    @property
    def num_fetched(self):
        num = 0
        for domain in self._domains[:]:
            num += domain.fetch_count
        return num

    @property
    def pressure(self):
        num = 0
        for domain in self._domains[:]:
            num += domain.pressure
        return num

    #endregion Utility methods and properties

    def on_open(self):
        self._domains = []
        for domain_config in self.config.domains:
            self.add_domain(domain_config)

    def _domain_match(self, url, url_prefix):
        # TODO: Might want to make this smarter; with/with-out protocol prefix, user info, etc.
        return url.startswith(url_prefix)

    def _find_matching_domain(self, url):
        url = url.lower()
        with self._lock:
            for d in self._domains:
                if self._domain_match(url, d.url_prefix):
                    domain = d
                    return domain
        return False  # Incoming URL request was not in a monitored domain

    def _incoming(self, document):
        if not type(document) is dict or not "url" in document:
            self.doclog.warning("Expected dict type document with at least a 'url' attribute.")
            return

        url = document.get("url")
        if not url:
            self.doclog.warning("'url' attribute was empty.")
            return

        domain = self._find_matching_domain(url)
        if not domain:
            return  # Incoming URL request was not in a monitored domain

        what = document.get("what")
        who = document.get("who")

        domain.add(url, what, who)

        # Some tracking, just for fun..
        self.total += 1
        self.count += 1

    def DUMP_domains(self, *args):
        for domain in self._domains[:]:
            if args and not domain.domain_id in args:
                continue
            print "-----------------------------------------------"
            print "domain_id         :", domain.domain_id
            print "url_prefix        :", domain.url_prefix
            print "rate_number       :", domain.rate_number
            print "rate_window       :", domain.rate_window
            print "ttl               :", domain.ttl
            print
            print "last_fetch_time   :", domain.last_fetch_time
            print "deny_until_time   :", domain.deny_until_time
            print "total_count       :", domain.total_count
            print "fetch_count       :", domain.fetch_count
            print "pressure          :", domain.pressure
            print "url_infos (count) :", len(domain.url_infos)
            print "_flist (count)    :", len(domain._flist)
            print
            if domain.url_infos:
                print "URL_INFOS:"
                for info in domain.url_infos.itervalues():
                    print "    ----------------"
                    print "    url             : ", info.url
                    print "    last_fetch_time : ", info.last_fetch_time
                    print "    total_count     : ", info.total_count
                    print "    fetch_count     : ", info.fetch_count
                    print "    pressure        : ", info.pressure
                    print "    requested_by    : ", info.requested_by



# TODO: Generator implementation, looping through the domains and fetching when required

    def on_abort(self): pass
    def on_shutdown(self): pass
    def on_suspend(self): pass
    def on_resume(self): pass

    def on_tick(self):
        pass

        # try:
        #     webdoc = self.web_getter.get(url, self.index, self.doctype, created_at=created_at)
        #     if not webdoc: continue
        #     if self.debuglevel >= 0:
        #         self.doclog(doc, "Created doc with content size=%-8s as /%s/%s/%s" % \
        #             (eslib.debug.byteSizeString(len(webdoc["_source"]["content"]), 1), self.index, self.doctype, webdoc.get("_id")))
        # except IOError as e:
        #     self.doclog(doc, e.args[0], loglevel=logger.WARNING)
        #     continue
        # except ValueError as e:
        #     self.doclog(doc, e.args[0], loglevel=logger.ERROR)
        #     continue

    def get_ready(self):
        with self._lock:
            for domain in self._domains:
                dd = domain.get_ready()
                print "%-10s : " % domain.domain_id, len(dd)


