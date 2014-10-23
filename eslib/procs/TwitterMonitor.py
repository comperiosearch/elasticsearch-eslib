__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
import TwitterAPI
import datetime, time
from xml.etree import ElementTree as XML
from requests.exceptions import ConnectionError
import dateutil, dateutil.parser


class TwitterMonitor(Monitor):
    """
    Monitor Twitter for a list of keywords. Streaming.

    Note that new keywords to monitor cannot be added or removed while the processor is running. It must be
    reconfigured and restarted. (Reason: Twitter rate limits.)

    Also note that there cannot be two instances of this processor running with the same credentials at the same
    time. (Reason: Twitter rate limits.)

    Protocols:

        esdoc.tweet:

            # TODO

        graph-edge:

            from       str : User ID.
            type       str : Relation, one of "author", "mention", "quote".
            to         str : User ID.

        urlrequest:

            url        str
            what       str : e.g. "twitter_mon"
            who        str : e.g. some user id

    Sockets:
        tweet  (esdoc.tweet) (default) : Tweet
        raw    (*)                     : The tweet reported in raw format, exactly as returned by the TwitterAPI.
        text   (str)                   : Only the text from the tweet.
        link   (urlrequest)            : Link from the tweet, for potential follow-up.
        user   (graph-edge)            : Info about author, mentioned or retweeted users from the tweet.

    Config:
        consumer_key        = None     : Part of twitter dev credentials.
        consumer_secret     = None     : Part of twitter dev credentials.
        access_token        = None     : Part of twitter dev credentials.
        access_token_secret = None     : Part of twitter dev credentials.
        track               = []       : List of phrases to track. Must be 1-60 bytes long. Case insensitive. Exact matching not supported.
        follow              = []       : List of users to track.
        locations           = []       : List of locations to track. "longitude,latitude"
        ignore_retweets     = True     : Do not report tweets from retweets if set. User relation "quote" will still be reported.

    For more information about what you can listen to and how it is matched, see:

        https://dev.twitter.com/streaming/reference/post/statuses/filter
    """

    RELATION_AUTHOR       = "author"
    RELATION_RETWEET      = "quote"
    RELATION_MENTION      = "mention"
    MAX_CONNECT_ATTEMPTS  = 10


    def __init__(self, **kwargs):
        super(TwitterMonitor, self).__init__(**kwargs)

        self.output_tweet  = self.create_socket("tweet" , "esdoc.tweet"  , "Tweet.", is_default=True)
        self.output_raw    = self.create_socket("raw"   , None           , "Tweet in raw format, exactly as returned by the TwitterAPI.")
        self.output_text   = self.create_socket("text"  , "str"          , "Only the text from the tweet.")
        self.output_link   = self.create_socket("link"  , "urlrequest"   , "Link from the tweet, for potential follow-up.")
        self.output_user   = self.create_socket("user"  , "graph-edge"   , "Info about author, mentioned or retweeted users from the tweet.")

        self.config.set_default(
            consumer_key        = None,
            consumer_secret     = None,
            access_token        = None,
            access_token_secret = None,
            track               = [],
            follow              = [],
            locations           = [],
            ignore_retweets     = True
        )

        self._twitter_filter    = {}
        self._twitter_api       = None
        self._twitter_response  = None
        self._twitter_iterator  = None
        self._connect_delay     = 0
        self._connect_attempts  = 0
        self._connecting        = False
        self._connected         = False
        self._last_connect_attempt_ms = 0
        self._may_iterate       = False  # Prevent it from entering blocking call
        self._inside_blocking   = False


    def on_open(self):
        self.total = 0
        self.count = 0

        # Verify existence of credentials
        if not (self.config.consumer_key and self.config.consumer_secret and self.config.access_token and self.config.access_token_secret):
            raise ValueError("Missing twitter credential in config, must have all of: consumer_key, consumer_secret, access_token, access_token_secret.")

        # Verify that there is actually something we should monitor
        if not (self.config.track or self.config.follow or self.config.locations):
            raise ValueError("Config should contain either 'track', 'follow' or 'locations' to monitor. Monitoring without any is meaningless.")

        # Verify phrase format for "track"
        for phrase in self.config.track:
            length = len(phrase)
            if length < 1 or length > 60:
                raise ValueError("Phrases for 'track' must be between 1 and 60 bytes long, failed: %s" % phrase)

        # Verify location format
        for location in self.config.locations:
            if not location or len(location.split(",")) != 4:
                raise ValueError("Locations must have TWO longitude and latitude pairs separated by comma, and the pairs separated by comma. (Bounding box: 'x1,y1,x2,y2'.)")

        # TODO: Verify user format for "follow". How??

        # Build twitter request dict
        self._twitter_filter = {}
        if self.config.track:
            self._twitter_filter["track"] = ",".join(self.config.track)
        if self.config.follow:
            self._twitter_filter["follow"] = ",".join(self.config.follow)
        if self.config.locations:
            self._twitter_filter["locations"] = ",".join(self.config.locations)


        # Initialize Twitter API with credentials
        self._twitter_api = TwitterAPI.TwitterAPI(
            self.config.consumer_key,
            self.config.consumer_secret,
            self.config.access_token,
            self.config.access_token_secret)

        self._connected = False
        self._connecting = True
        self._last_connect_attempt_ms = 0
        self._connect_attempts = 0
        self._connect_delay = 0

        # Try a request once, which includes connecting. Some validation should cast an exception.
        # Upon connection problems will try a reconnect in the run loop, later.
        self._connect(raise_instead_of_abort=True)

    def on_close(self):
        pass

    def on_shutdown(self):
        pass

    def stop(self):
        # Note: I hate overriding this method, but since the _twitter_iterator.next() call blocks
        #       we need to make it stop blocking. What we do here will cause a StopIteration exception
        #       that will pass control back to on_tick() and we can return from that method with state
        #       'stopping'.

        if self.stopping or not self.running:
            return
        # TODO: KILL THE TWITTER API CONNECTION IN A BETTER MANNER !!!
        if self._twitter_response:
            self.log.info("Shutting down Twitter stream.")
            self._may_iterate = False  # Prevent it from entering blocking call
            if self._inside_blocking:
                time.sleep(1) # Give it 1 second to finish whatever chunks it is assembling and get out!
            self._twitter_response.response.raw._fp.close()  # WTF... hackish (HTB)

        super(TwitterMonitor, self).stop()


    def _connect(self, raise_instead_of_abort=False):

        # First check if we are supposed to connect and we have waited long enough
        if not self._connecting:
            return  # Not connecting
        now_ms = time.time() * 1000
        if self._last_connect_attempt_ms and (now_ms - self._last_connect_attempt_ms) < self._connect_delay:
            return  # Not time yet

        self._last_connect_attempt_ms = time.time() * 1000
        self._connect_attempts += 1
        exception = None
        self._twitter_response = None
        self._connected = False
        try:
            self._twitter_response = self._twitter_api.request("statuses/filter", self._twitter_filter)
        except Exception as e:
            exception = e

        if exception or self._twitter_response.status_code != 200:
            self._handle_communication_error(exception, raise_instead_of_abort=True)
        else:
            self.log.info("Successfully connected to Twitter stream.")
            self._twitter_iterator = self._twitter_response.get_iterator()
            self._last_connect_attempt_ms = 0
            self._connect_attempts = 0
            self._connect_delay = 0
            self._connecting = False
            self._connected = True
            self._may_iterate = True  # It may now enter blocking call
            self._inside_blocking = False

    def _handle_communication_error(self, e=None, raise_instead_of_abort=False):

        do_abort = False
        code = self._twitter_response.status_code if self._twitter_response else 0

        # Service denial
        if isinstance(e, ConnectionError):
            # As suggested by Twitter:
            #   Back off linearly for TCP/IP level network errors. These problems
            #   are generally temporary and tend to clear quickly. Increase the
            #   delay in reconnects by 250ms each attempt, up to 16 seconds.
            self._connected = False
            if self._connect_attempts >= self.MAX_CONNECT_ATTEMPTS:
                self._connecting = False
                self.log.error("Connection error -- Max attempts (%d) exceeded. Aborting!" % self.MAX_CONNECT_ATTEMPTS)
                self.abort()
                return
            else:
                self._connecting = True
                if self._connect_delay < 16000:
                    self._connect_delay += 250
                self.log.debug("Connection error, exception: %s" % e.message)
                self.log.error("Connection error -- Trying to reconnect (%d/%d) in %.0f ms." % (self._connect_attempts, self.MAX_CONNECT_ATTEMPTS, self._connect_delay))
                return  # Run loop will try to reconnect
        elif code == 420:  # TODO
            # As suggested by Twitter;
            #   Back off exponentially for HTTP 420 errors. Start with a 1 minute
            #   wait and double each attempt. Note that every HTTP 420 received
            #   increases the time you must wait until rate limiting will no longer
            #   will be in effect for your account.
            self._connected = False
            if self._connect_attempts >= self.MAX_CONNECT_ATTEMPTS:
                self._connecting = False
                self.log.error("'420: Rate Limited' -- Max attempts (%d) exceeded. Aborting!" % self.MAX_CONNECT_ATTEMPTS)
                self.abort()
                return
            else:
                self._connecting = True
                if self._connect_delay < 320000:
                    self._connect_delay += 5000
                self.log.error("'420: Rate Limited' -- Trying to reconnect (%d/%d) in %.0f s." % (self._connect_attempts, self.MAX_CONNECT_ATTEMPTS, self._connect_delay/1000.0))
                return  # Run loop will try to reconnect
        elif code == 503:  # TODO
            # As suggested by Twitter;
            #   Back off exponentially for HTTP errors for which reconnecting would
            #   be appropriate. Start with a 5 second wait, doubling each attempt,
            #   up to 320 seconds.
            self._connected = False
            if self._connect_attempts >= self.MAX_CONNECT_ATTEMPTS:
                self._connecting = False
                self.log.error("'416: Service unavailable' -- Max attempts (%d) exceeded. Aborting!" % self.MAX_CONNECT_ATTEMPTS)
                self.abort()
                return
            else:
                self._connecting = True
                if self._connect_delay < 320000:
                    self._connect_delay += 5000
                self.log.error("'416: Service unavailable' -- Trying to reconnect (%d/%d) in %.0f s." % (self._connect_attempts, self.MAX_CONNECT_ATTEMPTS, self._connect_delay/1000.0))
                return  # Run loop will try to reconnect

        # Authorization and validation errors:
        elif code == 401:
            msg = "'401: Unauthorized' -- Authentication error. Aborting!"
            do_abort = True
        elif code == 403:
            msg = "'403: Forbidden' -- Trying to use unauthorized endpoint. Aborting!"
            do_abort = True
        elif code == 404:
            msg = "'404: Unknown' -- Requested resource does not exist. Aborting!"
        elif code == 406:
            msg = "'406: Not Acceptable' -- Illegal 'track' phrase length, invalid 'locations' bounding box, or invalid user id for 'follow'. Aborting!"
            do_abort = True
        elif code == 413:
            msg = "'413: Too Long' -- Number of 'track' phrases, 'locations' entries or 'follow' IDs exceeded allowance. Aborting!"
            do_abort = True
        elif code == 416:
            msg = "'416: Range Unacceptable' -- (Should not be possible to cause by this processor.) Aborting!"
            do_abort = True

        # Unknown errors:
        else:
            exstr = ", %s: %s" % (e.__class__.__name__, e.message if e else None) if e else ""
            msg = "Unknown problem reading from Twitter. Aborting! status_code=%d%s" % (code, exstr)
            do_abort = True

        if do_abort:
            if raise_instead_of_abort:
                self._connected = False
                raise Exception(msg)
            else:
                self.log.critical(msg)
                self.abort()

    def on_tick(self):

        if self._connecting:
            self._connect()  # Will check for delay, etc
            return  # Next tick will do work if we are now connected

        if self.suspended:
            return

        item = None
        self._inside_blocking = True
        try:
            if self._may_iterate:
                item = self._twitter_iterator.next()  # OBS OBS OBS: BLOCKING !!!!!!!!
        except StopIteration as e:
            if self.stopping:
                pass  # This is caused by us closing the underlying connection in order to get the iterator to stop. (Weirdness.. wish we could have sent a 'stop' -- HTB)
            else:
                self.log.error("StopIteration received without stopping. Aborting!")
                self.abort()
        except Exception as e:
            self._inside_blocking = False
            self._handle_communication_error(e)
            return  # If we need to reconnect, the next tick will handle that. If fatal (aborted), we are not getting back here.

        self._inside_blocking = False

        if item and self.has_output:
            self._handle(item)

    def _handle(self, twitter_item):
        raw, tweet, users, links = self._decode(twitter_item)
        if tweet:
            # Counters are for tweets:
            self.total += 1
            self.count += 1
            self.output_tweet.send(tweet)
            self.output_text.send(tweet["_source"]["text"])
        if raw:
            self.output_raw.send(raw)
        for user in users:
            self.output_user.send(user)
        for link in links:
            self.output_link.send(link)

    def _setfield(self, source, target, source_field, target_field=None, default=False, defaultValue=None, value_converter=None):
        if source_field in source and source[source_field] != None:
            value = source[source_field]
            target[target_field or source_field] = (value_converter(value) if value_converter else value)
        elif default:
            target[target_field or source_field] = defaultValue

    def _get_datetime(self, source, datestring_field, ms_field):
        dt = None
        if ms_field in source:
            try:
                ms = int(source[ms_field])
                dt = datetime.datetime.utcfromtimestamp(ms / 1000.0)
            except AttributeError:
                pass
        if not dt and datestring_field in source:
            try:
                dt = dateutil.parser.parse(source[datestring_field])
                if dt:
                    dt = dt - dt.utcoffset()
                    dt.replace(tzinfo=dateutil.tz.tzutc())
            except ValueError:
                pass
        return dt


    def _decode(self, twitter_item):
        "Return a tuple of (raw, tweet, users, links)."

        source = twitter_item
        now = datetime.datetime.utcnow()

        tweet = {"_id": source["id_str"], "_timestamp": now}
        users = []
        links = []

        ts = tweet["_source"] = {"id": source["id_str"] }  # id repeated intentionally

        # This user...
        source_user = source["user"]  # Always present
        user = { "id": source_user["id_str"] }
        # Add author to 'user' list
        users.append({"from": user["id"], "type": self.RELATION_AUTHOR, "to": user["id"]})

        ts["user"] = user
        self._setfield(source_user, user, "screen_name")
        self._setfield(source_user, user, "name")
        self._setfield(source_user, user, "lang")
        self._setfield(source_user, user, "description")
        self._setfield(source_user, user, "location")
        self._setfield(source_user, user, "profile_image_url")
        self._setfield(source_user, user, "protected")
        self._setfield(source_user, user, "geo_enabled")
        created_at = self._get_datetime(source_user, "created_at", "timestamp_ms") or now
        if created_at:
            user["created_at"] = created_at

        # Misc. top level stuff for the tweet:

        source_retweet = source.get("retweeted_status")
        if source_retweet:
            # Find out who has been retweeted:
            retweeted_user_id = source_retweet["user"]["id_str"]
            users.append({"from": user["id"], "type": self.RELATION_RETWEET, "to": retweeted_user_id})
            if self.config.ignore_retweets:
                return (None, None, users, links)

        self._setfield(source, ts, "text")
        self._setfield(source, ts, "truncated")
        self._setfield(source, ts, "lang")

        in_reply_to = {}
        self._setfield(source, in_reply_to, "in_reply_to_user_id_str", "user_id")
        self._setfield(source, in_reply_to, "in_reply_to_screen_name", "screen_name")
        self._setfield(source, in_reply_to, "in_reply_to_status_id", "status_id")
        if in_reply_to:
            ts["in_reply_to"] = in_reply_to

        ts["created_at"] = self._get_datetime(source, "created_at", "timestamp_ms") or now

        source_source = source.get("source")
        if source_source:
            try:
                ts["source"] = XML.fromstring(source_source.encode("utf8")).text
            except Exception as e:
                pass  # Otherwise just ignore this

        self._setfield(source, ts, "geo")

        # Do only a partial extract of 'place':
        source_place = source.get("place")
        if source_place:
            place = {}
            self._setfield(source_place, place, "country")
            self._setfield(source_place, place, "country_code")
            if place:
                ts["place"] = place

        # Handle entities extracted by Twitter:

        source_entities = source.get("entities")
        if source_entities:
            entities = {}
            # Get hashtags
            self._setfield(source_entities, entities, "hashtags")  # Use these directly
            # Get URLs
            # Do only a partial extract of 'urls'
            source_urls = source_entities.get("urls")
            if source_urls:
                urls = []
                for u in source_urls:
                    url = u["expanded_url"]
                    item = { "url": url, "indices": u["indices"] }
                    urls.append(item)
                    # Add to "links" list:
                    links.append({"url": url, "what": "twitter", "who": user["id"]})
                if urls:
                    entities["urls"] = urls
            # Get user mentions
            # Use 'id_str' as 'id' of type 'str'
            source_user_mentions = source_entities.get("user_mentions")
            if source_user_mentions:
                user_mentions = []
                for m in source_user_mentions:
                    m["id"] = m["id_str"]
                    del m["id_str"]
                    user_mentions.append(m)
                    # Add to 'users' list:
                    users.append({"from": user["id"], "type": self.RELATION_MENTION, "to": m["id"]})
                if user_mentions:
                    entities["user_mentions"] = user_mentions
            if entities:
                ts["entities"] = entities

        return (source, tweet, users, links)
