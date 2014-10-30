__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
from eslib.text import remove_parts
from .. import esdoc

class TweetEntityRemover(Processor):
    """
    Remove URLs and/or mentioned users from the tweet text.

    Protocols:

        esdoc.tweet:

            # TODO

    Connectors:
        input      (esdoc.tweet)       : Tweet
    Sockets:
        output     (esdoc.tweet)       : Tweet (possibly extended with a cleaned field)

    Config:
        source_field        = "text"   : Part of twitter dev credentials.
        target_field        = None     : Defaults to 'source_field', replacing the input field.
        remove_urls         = True
        remove_mentions     = False
    """


    def __init__(self, **kwargs):
        super(TweetEntityRemover, self).__init__(**kwargs)

        self.create_connector(self._incoming, "input", "esdoc.tweet", "Incoming tweet.")
        self.output = self.create_socket("output" , "esdoc.tweet"  , "Outgoing, cleaned, tweet.")

        self.config.set_default(
            source_field    = "text",
            target_field    = None,
            remove_urls     = True,
            remove_mentions = False
        )

    def _clean(self, doc):

        source = doc.get("_source")
        if not source:
            return doc

        text = esdoc.getfield(source, self.config.source_field)

        coords = []
        entities = source.get("entities")
        if self.config.remove_urls:
            x = esdoc.getfield(entities, "urls", [])
            coords += [l["indices"] for l in x]
        if self.config.remove_mentions:
            x = esdoc.getfield(entities, "user_mentions", [])
            coords += [l["indices"] for l in x]
        cleaned = None
        if not text:
            cleaned = text
        else:
            # The removal from coords most often leaves two spaces, so remove them, too, and strip border spaces.
            cleaned = remove_parts(text, coords).replace("  ", " ").strip()

        return esdoc.shallowputfield(doc, "_source." + (self.config.target_field or self.config.source_field), cleaned)

    def _incoming(self, doc):
        if not self.output.has_output:
            return # No point then...
        cleaned_doc = self._clean(doc)
        self.output.send(cleaned_doc)
