__author__ = 'babadofar'

from ..Generator import Generator
from .twitter import Twitter

import time


class TwitterListMembersGetter(Generator):
    """
    Receives list user screen_name and list slug  its connector and sends twitter user objects
    to its socket.

    # TODO: Document argument 'twitter' and how to configure this.

    Connectors:
        twitterlist        
            list_name: name of list
            list_owner: screen_name of owner
    Sockets:
        user       (graph-user)  : Twitter users.

    Config:
        batchsize  = 100      : How many users to gather up before making a call to Twitter.
        batchtime  = 7.0      : How many seconds to wait before we send a batch if it is not full.
    """

    def __init__(self, twitter=None, **kwargs):
        super(TwitterListMembersGetter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "twitterlist");
        self._output = self.create_socket("user", "graph-user", "Twitter users")
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

        Gets users from twitter and outputs to a socket.
        :param twitterlist:
            list_owner
            list_name
        """
        try:
            list_name_ = str(doc['list_name'])
            list_owner_ = str(doc['list_owner'])
        except ValueError:
            self.doclog.exception("Could not find user or list in doc: %s " % doc)
        self.log.debug("Getting members of list %s users from Twitter" % num)
        resp = self.twitter.get_list_members(list_owner= list_owner, list_name = list_name)
        for raw_user in resp:
            try:
                user = self.twitter.raw_to_dict(raw_user)
            except TypeError as type_error:
                self.log.exception(type_error)
            else:
                self._output.send(user)
