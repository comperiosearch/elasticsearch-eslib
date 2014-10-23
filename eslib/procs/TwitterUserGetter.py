__author__ = 'mats'

from ..Generator import Generator
from ..twitter import Twitter

import time


class TwitterUserGetter(Generator):
    """
    Receives uids on its connector and sends twitter user objects
    to its socket.

    # TODO: Document argument 'twitter' and how to configure this.

    Connectors:
        ids        (str)         : Incoming IDs to get data for.
    Sockets:
        user       (graph-user)  : Twitter users.
    """

    def __init__(self, twitter=None, **kwargs):
        super(TwitterUserGetter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "ids", "str")
        self.create_socket("user", "graph-user", "Twitter users.")
        self._queue = []
        self.last_call = time.time()
        self.twitter = twitter
        self.config.set_default(
            batchsize=100,
            batchtime=7
        )

    def on_open(self):
        """ Instantiate twitter class. """
        if self.twitter is None:
            self.twitter = Twitter(
                consumer_key=self.config.consumer_key,
                consumer_secret=self.config.consumer_secret,
                access_token=self.config.access_token,
                access_token_secret=self.config.access_token_secret
            )

    def _incoming(self, doc):
        """
        Put str(doc) into the queue.

        :param doc: the id of a twitter user

        """
        self._queue.append(str(doc))

    def on_tick(self):
        """
        Commit items in queue if queue exceeds batchsize or it's been long
        since last commit.

        """
        if ((len(self._queue) >= self.config.batchsize) or
            (time.time() - self.last_call > self.config.batchtime and self._queue)):
            self.get()

    def on_shutdown(self):
        """ Get rid of rest of queue before shutting down. """
        while self._queue:
            self.get()

    def get(self):
        """
        Gets users from twitter and outputs to a socket.

        """
        resp = self.twitter.get_users(uids=self._queue[:self.config.batchsize])
        self._queue = self._queue[self.config.batchsize:]
        for raw_user in resp:
            #TODO: Some kind of check here?
            user = self.twitter.raw_to_dict(raw_user)
            self.sockets["users"].send(user)
