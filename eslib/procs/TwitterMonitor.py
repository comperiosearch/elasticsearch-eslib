__author__ = 'Hans Terje Bakke'

from ..Monitor import Monitor
import TwitterAPI
import datetime
from xml.etree import ElementTree as XML
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
        tweet      (esdoc.tweet)       : Tweet
        raw        (*)                 : The tweet reported in raw format, exactly as returned by the TwitterAPI.
        text       (str)               : Only the text from the tweet.
        link       (urlrequest)        : Link from the tweet, for potential follow-up.
        user       (graph-edge)        : Info about author, mentioned or retweeted users from the tweet.

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

    RELATION_AUTHOR  = "author"
    RELATION_RETWEET = "quote"
    RELATION_MENTION = "mention"

    def __init__(self, **kwargs):
        super(TwitterMonitor, self).__init__(**kwargs)

        self.output_tweet  = self.create_socket("tweet" , "esdoc.tweet"  , "Tweet.")
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

        # TODO: Perhaps move this to on_startup(), later
        self._twitter_api = TwitterAPI.TwitterAPI(
            self.config.consumer_key,
            self.config.consumer_secret,
            self.config.access_token,
            self.config.access_token_secret)
        self._twitter_response = self._twitter_api.request("statuses/filter", self._twitter_filter)
        self._twitter_iterator = self._twitter_response.get_iterator()

    def on_close(self):
        pass  # TODO

    def on_startup(self):
        pass  # TODO

    def on_shutdown(self):
        pass  # TODO

    def on_abort(self):
        pass  # TODO

    def on_suspend(self):
        pass  # TODO

    def on_resume(self):
        pass  # TODO

    def on_tick(self):

        item = None
        try:
            item = self._twitter_iterator.next()  # OBS OBS OBS: BLOCKING !!!!!!!!
        except Exception as e:
            self.log.error("Problem reading from Twitter: %s" + e.message)
            self.abort()

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
        if False: #source_source:
            try:
                ts["source"] = XML.fromstring(source_source).text
            except Exception as e:
                print "*** PARSER ERROR: ", source_source
                print e

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
