__author__ = 'mats'
from ..Generator import Generator
from ..twitter import Twitter

class TwitterFollowerGetter(Generator):
    """
    This generator takes as input the ids of twitter users, and then goes
    ahead and retrieves the followers or friends of this user,
    and outputs the ids.

    # TODO: Document argument 'twitter' and how to configure this.

    Connectors:
        ids        (str)         : Incoming IDs to get data for.
    Sockets:
        ids        (str)         : IDs of related nodes.
    """
    def __init__(self, twitter=None, **kwargs):
        super(TwitterFollowerGetter, self).__init__(**kwargs)
        self.twitter = twitter
        self.create_connector(self._incoming, "ids", "str")
        self.create_socket("ids", "str", "IDs of related nodes.")
        self.create_socket("ids", "str", "ids of related nodes")
        self.create_socket("edges", "graph-edge")
        self.config.set_default(
            outgoing=True
        )

    def on_open(self):
        if self.twitter is None:
            self.twitter = Twitter(
                consumer_key=self.config.consumer_key,
                consumer_secret=self.config.consumer_secret,
                access_token=self.config.access_token,
                access_token_secret=self.config.access_token_secret
            )

    def _incoming(self, document):
        users = self.twitter.get_follows(uid=document,
                                         outgoing=self.config.outgoing)
        for id_ in users:
            self.sockets["ids"].send(id_)
            if self.config.outgoing:
                edge = {"from": document,
                        "type": "follows",
                        "to": id_
                        }
            else:
                edge = {"from": id_,
                        "type": "follows",
                        "to": document
                        }
            self.sockets["edges"].send(edge)
