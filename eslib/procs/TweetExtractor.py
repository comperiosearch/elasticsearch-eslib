__author__ = 'Hans Terje Bakke'

from ..Processor import Processor


class TweetExtractor(Processor):
    """
    Extract properties from a tweet to different sockets: 'user' and 'link'.

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
        text   (str)                   : Only the text from the tweet.
        link   (urlrequest)            : Link from the tweet, for potential follow-up.
        user   (graph-edge)            : Info about author, mentioned or retweeted users from the tweet.

    Config:
        drop_retweets       = True     : Do not report tweets from retweets if set. User relation "quote" will still be reported.
    """

    RELATION_AUTHOR       = "author"
    RELATION_RETWEET      = "quote"
    RELATION_MENTION      = "mention"


    def __init__(self, **kwargs):
        super(TweetExtractor, self).__init__(**kwargs)

        self.create_connector(self._incoming, "tweet", "esdoc.tweet", "Tweet.");

        self.output_tweet  = self.create_socket("tweet" , "esdoc.tweet"  , "Tweet.", is_default=True)
        self.output_text   = self.create_socket("text"  , "str"          , "Only the text from the tweet.")
        self.output_link   = self.create_socket("link"  , "urlrequest"   , "Link from the tweet, for potential follow-up.")
        self.output_user   = self.create_socket("user"  , "graph-edge"   , "Info about author, mentioned or retweeted users from the tweet.")

        self.config.set_default(
            drop_retweets      = True
        )

    def _incoming(self, doc):

        if not doc or not type(doc) is dict or not self.has_output:
            return

        tweet, users, links = self._extract(doc)
        if tweet:
            self.output_tweet.send(tweet)
            self.output_text.send(tweet["_source"]["text"])
        for user in users:
            self.output_user.send(user)
        for link in links:
            self.output_link.send(link)

    def _extract(self, tweet):
        "Return a tuple of (tweet, users, links)."

        users = []
        links = []

        source = tweet["_source"]  # Always present

        # Add author to 'users' list
        user_id = source["user"]["id"]  # Always present
        users.append({"from": user_id, "type": self.RELATION_AUTHOR, "to": user_id})

        # Retweets
        retweet_user_id = source.get("retweet_user_id")
        if retweet_user_id:
            # Find out who has been retweeted:
            # Add retweet to 'users' list
            users.append({"from": user_id, "type": self.RELATION_RETWEET, "to": retweet_user_id})
            if self.config.drop_retweets:
                return (None, users, links)

        # URLs and mentions from entities
        entities = source.get("entities")
        if entities:
            # Get URLs
            urls = entities.get("urls")
            if urls:
                for url in urls:
                    # Add to "links" list:
                    links.append({
                        "url" : url["url"],
                        "what": "twitter",  # TODO: Maybe use self.name instead?
                        "who" : user_id
                    })
            # Get user mentions
            user_mentions = entities.get("user_mentions")
            if user_mentions:
                for m in user_mentions:
                    # Add relation to 'users' list:
                    users.append({"from": user_id, "type": self.RELATION_MENTION, "to": m["id"]})

        return (tweet, users, links)
