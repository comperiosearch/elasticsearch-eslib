import unittest
from eslib.procs import WebGetter
from eslib import debug
import random, time

res = []

class TestWebGetter(unittest.TestCase):

    def _setup(self):
        domains = [
            {
                "domain_id"  : "Comperio",
                "url_prefix" : "http://comperio.no",
                "rate_number": 3,
                "rate_window": 5,
                "ttl"        : 0

            },
            {
                "domain_id"  : "UNINETT",
                "url_prefix" : "http://uninett.no",
                "ttl"        : 3
            },
            {
                "domain_id"  : "VG",
                "url_prefix" : "http://vg.no",
                "rate_number": 3,
                "rate_window": 4,
                "ttl"        : 2
            },
        ]

        self.domains = ["comperio", "uninett"]
        self.words = ["alfa", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "my", "ny", "ksi", "omikron", "pi", "rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega"]

        m = WebGetter(domains=domains)
        #m.start()

        self.m = m

        self.start_sec = time.time()


    def put(self, domain, word, direct=False):
        url = "http://%s.no/%s" % (domain, word)
        doc = {
            "url"  : url,
            "what" : "tester",
            "who"  : "dude"
        }

        #print "Putting url:", url
        if direct:
            self.m._incoming(doc)  # Call the connector's method directly
        else:
            self.m.put(doc)        # Put on the connector; will check whether started, etc


    def put_random(self):
        domain = random.choice(self.domains)
        word = random.choice(self.words)
        self.put(domain, word)

    def get_ready(self, domain=None):
        sec = time.time() - self.start_sec
        for d in self.m._domains:
            if not domain or domain == d:
                ready = d.get_ready()
                words = [info.url.split("/")[-1] for info,ttl_expired in ready]
                print "[%4d] %-10s: %d" % (sec, d.domain_id, len(ready)), words

    def print_status(self):
        sec = time.time() - self.start_sec
        for domain in self.m._domains:
            print "[%4d] %-10s: (%2d/%2d ttl=%2d) P=%2d F=%2d" % (
                sec,
                domain.domain_id,
                domain.rate_number,
                domain.rate_window,
                domain.ttl,
                domain.pressure,
                domain.fetch_count
            )

    def test_simple(self):

        self._setup()
        self.m.on_open()  # To make it initialize stuff first
        m = self.m

        self.put("comperio", "alfa", direct=True)
        self.put("comperio", "alfa", direct=True)
        self.put("comperio", "beta", direct=True)

        self.assertTrue(self.m._domains[0].pressure    == 3, "Pressure should be 3, was %d" % self.m._domains[0].pressure)
        self.assertTrue(self.m._domains[0].fetch_count == 0, "Nothing should be fetched, was %d" % self.m._domains[0].fetch_count)

        self.get_ready()

        self.assertTrue(self.m._domains[0].pressure    == 0, "Pressure should now be 0, was %d" % self.m._domains[0].pressure)
        self.assertTrue(self.m._domains[0].fetch_count == 2, "We should now have fetched 2, was %d" % self.m._domains[0].fetch_count)

    def do_ttl(self, domain, expect):
        self.put("uninett", "alfa", direct=True)
        self.put("uninett", "alfa", direct=True)
        self.put("uninett", "beta", direct=True)

        time.sleep(1)
        ready = domain.get_ready()
        sec = time.time() - self.start_sec
        print "[%4d] READY=(%d)" % (sec, len(ready)), [info.url.split("/")[-1] for info,ttl_expired in ready]

        self.assertTrue(len(ready) == expect, "Expected %d" % expect)

    def test_ttl(self):

        self._setup()
        self.m.on_open()  # To make it initialize stuff first

        # Note: uninett ttl is set to 3 seconds
        domain = self.m._domains[1]  # UNINETT

        self.do_ttl(domain, 2) # Get 2
        self.do_ttl(domain, 0) # One second since, nothing to get
        self.do_ttl(domain, 0) # Two seconds since, nothing to get
        self.do_ttl(domain, 0) # Three seconds since, nothing to get
        self.do_ttl(domain, 2) # Four seconds since, nothing to get
        self.do_ttl(domain, 0) # One second since

    def test_rate_limit_burst(self):

        self._setup()
        self.m.on_open()  # To make it initialize stuff first

        # Note: comperio rate limit is set to 3 per 5 sec
        domain = self.m._domains[0]  # Comperio

        # Send in all the words immediately
        for word in self.words[:len(self.words)/4]:
            self.put("comperio", word, direct=True)

        # Get the stuff...
        while domain.pressure:
            time.sleep(1)
            ready = domain.get_ready()
            sec = int(time.time() - self.start_sec)
            words = [info.url.split("/")[-1] for info,ttl_expired in ready]
            print "[%4d] %-10s: %d" % (sec, domain.domain_id, len(ready)), words

            # Expect to get a burst of rate_number every rate_window
            expected = 0
            if ((sec-1) % domain.rate_window) == 0:
                expected = domain.rate_number
            self.assertTrue(len(ready) == expected, "Expected %d items on sec %d; got %d" % (expected, sec, len(ready)))

    def test_rate_limit_slide(self):

        self._setup()
        self.m.on_open()  # To make it initialize stuff first

        # Note: VG rate limit is set to 2 per 3 sec and ttl 4
        domain = self.m._domains[2]  # VG

        received = []
        i = 0
        while domain.pressure or i < len(self.words)/4:
            if i < len(self.words)/4:
                self.put("vg", self.words[i], direct=True)
                i += 1
            if i < len(self.words)/4:
                self.put("vg", self.words[i], direct=True)
                i += 1
            time.sleep(1)
            ready = domain.get_ready()
            received.extend(ready)
            sec = int(time.time() - self.start_sec)
            words = [info.url.split("/")[-1] for info,ttl_expired in ready]
            print "[%4d] %-2s: (flist=%2d) %d" % (sec, domain.domain_id, len(domain._flist), len(ready)), words

            # Calc what to expect
            mod_expect = [2, 1, 0, 0]  # 3 per 4 over 4 seconds when adding two per second
            expected = mod_expect[((sec-1) % domain.rate_window)]
            self.assertTrue(len(ready) == expected, "Expected %d items on sec %d; got %d" % (expected, sec, len(ready)))

        print "Received words: (%d)\n" % len(received), [info.url.split("/")[-1] for info,ttl_expired in received]
        self.assertTrue(len(received) == len(self.words)/4, "Expected %d words received; got %d" % (len(self.words)/4, len(received)))


    def hook(self, doc):
        content_size = len(doc["_source"].get("content") or "")
        if doc["_id"] == u"http://www.comperio.no":
            self.last_comperio_had_content = ("content" in doc["_source"])
            self.last_comperio_was_stopping = self.is_stopping

        sec = time.time() - self.start_sec
        doc["_source"]["content"] = "CONTENT=%s" % debug.byte_size_string(content_size)
        print "[%4d] %s, content=%d" % (sec, doc["_id"], content_size), doc["_source"]["requested_by"]

    def do_fetch(self, ttl, last_comperio_user):
        self.start_sec = time.time()
        self.is_stopping = False

        g = WebGetter(domains=[{"domain_id": "*", "url_prefix": "*", "rate_number":1, "rate_window":1, "ttl":ttl}])
        g.add_callback(self.hook)
        g.start()

        g.put({"url":"http://www.comperio.no", "what":"twitter", "who":"htb2"})
        g.put({"url":"http://www.comperio.no", "what":"twitter", "who":"htb2"})
        g.put({"url":"http://www.comperio.no", "what":"twitter", "who":"htb2"})
        g.put({"url":"http://www.comperio.no", "what":"twitter", "who":"htb2"})
        g.put({"url":"http://www.comperio.no", "what":"twitter", "who":"htb2"})
        time.sleep(2)
        g.put({"url":"http://www.comperio.no", "what":"twitter", "who":last_comperio_user})
#        g.put({"url":"https://www.uninett.no", "what":"twitter", "who":"htb"})
#        g.put({"url":"https://www.uninett.no/uninett-as", "what":"twitter", "who":"htb"})
#        g.put({"url":"http://blog.comperiosearch.com/", "what":"twitter", "who":"htb2"})

        #g.DUMP_domains()
        time.sleep(4)
        #g.DUMP_domains()
        print "STOPPING"
        self.is_stopping = True
        g.stop()
        g.wait()
        #g.DUMP_domains()
        print "STOPPED"

    def test_fetch_ttl_3(self):  # OK
        self.do_fetch(3, "htb3")  # Second argument "htb2" or "htb3" should give same result
        # ttl should have expired long before we stop, and the url should be fetched prior to stop
        print "LAST COMPERIO, has content = %s, was stopping = %s" % (self.last_comperio_had_content, self.last_comperio_was_stopping)
        self.assertTrue(self.last_comperio_had_content)
        self.assertFalse(self.last_comperio_was_stopping)

    def test_fetch_ttl_5(self):  # OK
        self.do_fetch(5, "htb2")  # User not changed
        # ttl should have expired when we stop, so we get a final fetch
        print "LAST COMPERIO, has content = %s, was stopping = %s" % (self.last_comperio_had_content, self.last_comperio_was_stopping)
        self.assertTrue(self.last_comperio_had_content)
        self.assertTrue(self.last_comperio_was_stopping)

    def test_fetch_ttl_5_user_changed(self):  # OK
        self.do_fetch(5, "htb3")  # User changed
        # ttl should have expired when we stop, so we get a final fetch
        print "LAST COMPERIO, has content = %s, was stopping = %s" % (self.last_comperio_had_content, self.last_comperio_was_stopping)
        self.assertTrue(self.last_comperio_had_content)
        self.assertTrue(self.last_comperio_was_stopping)

    def test_fetch_ttl_10(self):  # OK
        self.do_fetch(10, "htb2") # User not changed
        # ttl should NOT have expired when we stop, and since there is no user change, we should not get a fetch after stop
        # (so what we check on here would be the last we got before stop, which should have content, i.e. be a true fetch)
        print "LAST COMPERIO, has content = %s, was stopping = %s" % (self.last_comperio_had_content, self.last_comperio_was_stopping)
        self.assertTrue(self.last_comperio_had_content)
        self.assertFalse(self.last_comperio_was_stopping)

    def test_fetch_ttl_10_user_changed(self):  # OK
        self.do_fetch(10, "htb3") # User changed
        # ttl should NOT have expired when we stop, and since there IS a user change, we should get a final output after stop,
        # but no fetch (i.e. no content)
        print "LAST COMPERIO, has content = %s, was stopping = %s" % (self.last_comperio_had_content, self.last_comperio_was_stopping)
        self.assertFalse(self.last_comperio_had_content)
        self.assertTrue(self.last_comperio_was_stopping)


    def add_url(self, sleep_time, what, who, url):
        print "Sleeping", sleep_time
        time.sleep(sleep_time)
        print "Adding", url
        self.m.put({"url":url, "what":what, "who":who})

    def test_MANUAL(self):

        domains = [
            {
                "domain_id"  : "Comperio",
                "url_prefix" : "http://www.comperio.no",
                "rate_number": 3,
                "rate_window": 5,
                "ttl"        : 0

            },
            {
                "domain_id"  : "UNINETT",
                "url_prefix" : "http://uninett.no",
                "ttl"        : 3
            }
        ]

        self.m = WebGetter(domains=domains)
        self.m.start()

        print "** started, waiting 3 sec"
        time.sleep(3)

        self.add_url(2, "tester", "htb", "http://www.comperio.no")
        self.add_url(2, "tester", "htb", "http://www.comperio.no/jobb-i-comperio/")
        self.add_url(2, "tester", "htb", "https://www.uninett.no")

        try:
            self.m.wait()
        except KeyboardInterrupt:
            print "*** KEYBOARD INTERRUPT ***"
            self.m.stop()
            self.m.wait()

def main():
    unittest.main()

if __name__ == "__main__":
    main()
